"""
Config Generator for CDM Creation Application - Version 2.0

Completes chatbot-filled config templates by:
1. Reading partial config from chatbot
2. Prompting for thresholds
3. Using AI to determine required FHIR/IG resources (exact filenames)
4. Using AI to determine required NCPDP standards
5. Validating all files exist
6. Generating complete config JSON

Features:
- Skip FHIR or NCPDP analysis to save time
- Preserves skipped sections from most recent timestamped config
- Timestamped outputs: config_plan_20251121_232439.json
- timeout=1800, encoding='utf-8', list/dict NCPDP handling

Usage:
    python config/config_generator.py config/config_plan.json
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.llm_client import LLMClient


class ConfigGenerator:
    """Complete CDM configuration files from chatbot-filled templates."""
    
    def __init__(self):
        self.llm_client = LLMClient(timeout=1800)
        self.config_dir = Path(__file__).parent
        self.project_root = self.config_dir.parent
        self.input_dir = self.project_root / "input"
        self.standards_fhir_ig = self.input_dir / "strd_fhir_ig"
        self.standards_ncpdp = self.input_dir / "strd_ncpdp"
        
    def load_partial_config(self, filepath: str) -> Dict:
        """Load chatbot-filled partial config."""
        try:
            with open(filepath, 'r') as f:
                config = json.load(f)
            
            # Validate required fields
            required = ['cdm', 'input_files', 'thresholds']
            missing = [f for f in required if f not in config]
            if missing:
                raise ValueError(f"Missing required sections: {missing}")
            
            cdm_required = ['domain', 'type', 'description']
            cdm_missing = [f for f in cdm_required if not config['cdm'].get(f)]
            if cdm_missing:
                raise ValueError(f"Missing CDM fields: {cdm_missing}")
            
            # Validate type (case-insensitive)
            cdm_type = config['cdm']['type'].lower()
            if cdm_type not in ['core', 'functional']:
                raise ValueError(f"Invalid type: {config['cdm']['type']} (must be 'core' or 'functional')")
            
            # Validate functional has core_dependency
            if cdm_type == 'functional' and not config['cdm'].get('core_dependency'):
                raise ValueError("Functional CDMs must specify core_dependency")
            
            return config
            
        except FileNotFoundError:
            raise ValueError(f"Partial config file not found: {filepath}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in partial config: {e}")
    
    def load_existing_config(self, partial_config_file: str) -> Optional[Dict]:
        """Load most recent timestamped config if it exists."""
        # Get base name from partial config file (e.g., config_plan.json -> config_plan)
        base_path = Path(partial_config_file)
        base_name = base_path.stem  # e.g., "config_plan"
        
        # Find all timestamped configs matching pattern: config_plan_YYYYMMDD_HHMMSS.json
        pattern = f"{base_name}_*.json"
        timestamped_configs = sorted(self.config_dir.glob(pattern), reverse=True)
        
        if timestamped_configs:
            try:
                with open(timestamped_configs[0], 'r') as f:
                    print(f"   ℹ️  Loading from: {timestamped_configs[0].name}")
                    return json.load(f)
            except Exception as e:
                print(f"⚠️  Could not load existing config: {e}")
        
        return None
    
    def prompt_thresholds(self, partial_config: Dict) -> Dict[str, float]:
        """Prompt user for threshold values with defaults from template."""
        
        # Get defaults from template or use hardcoded defaults
        template_thresholds = partial_config.get('thresholds', {})
        entity_default = template_thresholds.get('entity_threshold', 60)
        attribute_default = template_thresholds.get('attribute_threshold', 40)
        
        print("\n=== Threshold Configuration ===\n")
        print(f"Current defaults: Entity={entity_default}%, Attribute={attribute_default}%")
        print("Press Enter to keep defaults, or enter new values.\n")
        
        entity_input = input(f"Enter Entity Alignment Threshold [{entity_default}]: ").strip()
        entity_threshold = int(entity_input) / 100 if entity_input else entity_default / 100
        
        attribute_input = input(f"Enter Attribute Fit Threshold [{attribute_default}]: ").strip()
        attribute_threshold = int(attribute_input) / 100 if attribute_input else attribute_default / 100
        
        return {
            "entity_threshold": entity_threshold,
            "attribute_threshold": attribute_threshold
        }
    
    def prompt_skip_analysis(self, analysis_type: str) -> bool:
        """Prompt user whether to skip an analysis."""
        response = input(f"\nRun {analysis_type} analysis? (Y/n): ").strip().lower()
        return response not in ['n', 'no']
    
    def determine_fhir_resources(self, partial_config: Dict) -> Dict:
        """Use AI to determine required FHIR/IG resources with exact filenames."""
        
        prompt = self._build_fhir_determination_prompt(partial_config)
        
        print("\n🤖 Analyzing FHIR/IG requirements with AI...\n")
        
        try:
            # Use chat() method and parse JSON
            messages = [{"role": "user", "content": prompt}]
            response_text, _ = self.llm_client.chat(messages)
            
            # Parse JSON response
            result = json.loads(response_text)
            return result
            
        except json.JSONDecodeError as e:
            print(f"Error parsing AI response as JSON: {e}")
            print(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            print(f"Error during FHIR AI analysis: {e}")
            raise
    
    def determine_ncpdp_standards(self, partial_config: Dict) -> Dict:
        """Use AI to determine required NCPDP standards."""
        
        prompt = self._build_ncpdp_determination_prompt(partial_config)
        
        print("\n🤖 Analyzing NCPDP requirements with AI...\n")
        
        try:
            # Use chat() method and parse JSON
            messages = [{"role": "user", "content": prompt}]
            response_text, _ = self.llm_client.chat(messages)
            
            # Parse JSON response
            result = json.loads(response_text)
            
            # Parse and auto-split if needed
            return self._parse_ncpdp_response(result)
            
        except json.JSONDecodeError as e:
            print(f"Error parsing AI response as JSON: {e}")
            print(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            print(f"Error during NCPDP AI analysis: {e}")
            raise
    
    def _parse_ncpdp_response(self, ai_response: Dict) -> Dict:
        """Parse AI NCPDP response and auto-split by code prefix if needed."""
        
        # If AI already split them properly
        if 'ncpdp_general_standards' in ai_response and 'ncpdp_script_standards' in ai_response:
            return ai_response
        
        # If AI returned flat list, auto-split by code prefix
        general = []
        script = []
        
        all_standards = ai_response.get('ncpdp_standards', [])
        if all_standards:
            print("   Auto-splitting NCPDP standards by code prefix...")
            for std in all_standards:
                code = std['code'].upper()
                if code.startswith('S') or code.startswith('Q'):
                    script.append(std)
                    print(f"      → {code} → SCRIPT")
                else:
                    general.append(std)
                    print(f"      → {code} → General")
        
        return {
            'ncpdp_general_standards': general,
            'ncpdp_script_standards': script,
            'domain_assessment': ai_response.get('domain_assessment', {})
        }
    
    def _build_fhir_determination_prompt(self, partial_config: Dict) -> str:
        """Build prompt for AI to determine FHIR/IG resources with exact filenames."""
        
        cdm = partial_config['cdm']
        
        return f"""You are a FHIR R4 and US healthcare IG expert analyzing CDM requirements.

# Task
Based on your knowledge of FHIR as well as industry understanding of Pharmacy Benefit Management, Specialty Drug, and Care Management, select the FHIR resource that will support the creation of Entity and Attributes for the CDM. Your task includes determining the EXACT FHIR resource files that are of highest value to build this CDM. You will be returning specific resource filenames.

# CDM Metadata
- **Domain**: {cdm['domain']}
- **Type**: {cdm['type']}
{f"- **Core Dependency**: {cdm.get('core_dependency', 'N/A')}" if cdm['type'].lower() == 'functional' else ""}
- **Description**: {cdm['description']}

# CRITICAL Instructions

1. **CDM Context for FHIR Resource Selection**
   a. The CDM domain is: {cdm['domain']}
   b. The CDM description is: {cdm['description']}

2. **Resources INCLUDED in the CDM Only**
   a. The selected FHIR resource files are used in a downstream step to define the CDM entities and attributes. Another step defines the relationships between the CDMs
   b. Return ONLY resources that represent entities and attributes IN this CDM
   c. Do NOT include resources that are only supporting/referenced by this CDM
   d. For each FHIR resource selected, include an explanation as to why it was selected for this CDM

3. **IG Priority for Resource Selection**
   a. When multiple IGs define the same resource, you need to choose the appropriate IG(s) based on CDM domain context to select the resource
   b. When a resource exists in more than one IG, choose the 1 or 2 BEST FIT duplicate FHIR resources that your knowledge of FHIR, PBM, Specialty Drug, and Care Management will support the CDM
      i. Only include alternates if they add clear domain-specific value
      ii. If duplicates exist, assign priority (1=use first, 2=alternate)

4. **FHIR resource file types to Include**
   a. For each resource, identify ALL relevant file types that your knowledge of FHIR, PBM, Specialty Drug, and Care Management will be of high value to support the CDM:
      i. **StructureDefinition-*.json** - Entity/attribute model (REQUIRED)
      ii. **CodeSystem-*.json** - Coded value definitions
      iii. **ValueSet-*.json** - Allowed value constraints
      iv. **CapabilityStatement-*.json** - IG context (optional)

5. **Exact Filenames**
   a. You have complete FHIR R4 and IG knowledge
   b. Return EXACT filenames as they appear in standard IGs
   c. Examples:
      i. `StructureDefinition-us-core-coverage.json`
      ii. `ValueSet-coverage-class.json`
      iii. `StructureDefinition-hrex-organization.json`
   d. Use standard IG file naming conventions

# Output Format

Respond with ONLY valid JSON:

{{
  "fhir_igs": [
    {{
      "filename": "StructureDefinition-us-core-coverage.json",
      "resource_name": "Coverage",
      "file_type": "StructureDefinition",
      "ig_source": "US Core",
      "priority": 1,
      "reasoning": "Primary coverage resource for member-level insurance. US Core is baseline standard."
    }},
    {{
      "filename": "ValueSet-coverage-class.json",
      "resource_name": "CoverageClass",
      "file_type": "ValueSet",
      "ig_source": "US Core",
      "priority": 1,
      "reasoning": "Defines coverage classification codes (group/plan)."
    }},
    {{
      "filename": "StructureDefinition-pdex-coverage.json",
      "resource_name": "Coverage",
      "file_type": "StructureDefinition",
      "ig_source": "PDex",
      "priority": 2,
      "reasoning": "Alternate: PDex extends US Core Coverage with payer-specific fields."
    }}
  ],
  "domain_assessment": {{
    "primary_igs": ["US Core", "Plan-Net"],
    "expected_entity_count": 5,
    "confidence": "high | medium | low",
    "notes": "Brief assessment of IG fit for this domain."
  }}
}}

# Critical Requirements
- Return ONLY valid JSON, no markdown or code blocks
- Use exact FHIR R4/IG standard filenames
- Assign priority (1=primary, 2=alternate) for duplicate resources
- Focus on resources for CDM entities and attributes, not supporting references
- Include all file types (StructureDefinition, CodeSystem, ValueSet) per resource
- Provide clear, specific reasoning for each file

Respond with JSON only:"""
    
    def _build_ncpdp_determination_prompt(self, partial_config: Dict) -> str:
        """Build prompt for AI to determine NCPDP standards."""
        
        cdm = partial_config['cdm']
        
        return f"""You are an NCPDP standards expert analyzing CDM requirements for Pharmacy Benefit Management.

# Task
Based on your knowledge of NCPDP standards as well as industry understanding of Pharmacy Benefit Management, Specialty Drug, and Care Management, select the NCPDP standard codes that will support the creation of Entity and Attributes for the CDM. Your task includes determining which NCPDP transaction and SCRIPT standards are of highest value to build this CDM. You will be returning specific standard codes with clear reasoning.

# CDM Metadata
- **Domain**: {cdm['domain']}
- **Type**: {cdm['type']}
{f"- **Core Dependency**: {cdm.get('core_dependency', 'N/A')}" if cdm['type'].lower() == 'functional' else ""}
- **Description**: {cdm['description']}

# CRITICAL Instructions

1. **CDM Context for NCPDP Standard Selection**
   a. The CDM domain is: {cdm['domain']}
   b. The CDM description is: {cdm['description']}

2. **CRITICAL: MAXIMUM SELECTIVITY - ENTITY AFFINITY ONLY**
   a. Select ONLY standards where the standard's PRIMARY PURPOSE is to DEFINE the structure, attributes, and rules of this CDM domain
   b. EXCLUDE standards that merely reference, route to, process, or transact using this CDM
   c. EXCLUDE standards whose purpose is integration, coordination, reporting, or billing
   d. Ask: "Does this standard exist to MODEL this domain?"
      - YES → Consider including
      - NO (exists to USE/PROCESS this domain) → Exclude
   e. Selectivity test:
      - If removing this standard means the CDM cannot be defined → INCLUDE
      - If removing this standard only affects transactions/processing → EXCLUDE
   f. Err on selecting TOO FEW rather than too many

3. **Use your knowledge of NCPDP Standards as well as industry knowledge of PBMs, Specialty Drug, and Care Management organizations to identify the NCPDP Standards that will be of high value generating the CDM entities and attributes**
   a. The selected NCPDP standards are used in a downstream step to define the CDM entities and attributes. Another step defines the relationships between the CDMs
   b. Return ONLY standards that represent entities and attributes IN this CDM
   c. Do NOT include standards that are only supporting/referenced by this CDM
   d. For each NCPDP standard selected, include an explanation as to why it was selected for this CDM

4. **To identify the selected standards return the Standard Code (A,B,C) for the selected standards**

# Output Format

Respond with ONLY valid JSON:

{{
  "ncpdp_general_standards": [
    {{
      "code": "I",
      "name": "Insurance Segment",
      "reasoning": "Contains plan routing identifiers (BIN/PCN/Group) and coverage structure fields essential for plan identification in pharmacy transactions."
    }},
    {{
      "code": "T",
      "name": "Transmission Header Standard",
      "reasoning": "Includes plan sponsor identification and routing information required for transaction processing and network configuration."
    }}
  ],
  "ncpdp_script_standards": [
    {{
      "code": "S",
      "name": "SCRIPT Messaging Standard",
      "reasoning": "Defines electronic prescription routing and plan-level formulary messaging workflows."
    }}
  ],
  "domain_assessment": {{
    "ncpdp_relevance": "high | medium | low",
    "confidence": "high | medium | low",
    "notes": "Brief assessment of NCPDP standards fit for this CDM domain. Include comparison to FHIR if relevant (e.g., 'NCPDP provides transaction-level identifiers while FHIR models plan structure')."
  }}
}}

# Critical Requirements
- Return ONLY valid JSON, no markdown or code blocks
- Use single-letter standard codes (T, F, H, I, C, D, P, etc.)
- Focus on standards for CDM entities and attributes, not supporting references
- Provide clear, specific reasoning for each standard
- Be selective - only include standards of highest value to the CDM

Respond with JSON only:"""
    
    def validate_and_match_files(self, partial_config: Dict, fhir_resources: Dict, ncpdp_resources: Dict) -> Tuple[Dict, List[str]]:
        """Validate input files exist and match AI-determined resources to actual files."""
        
        warnings = []
        matched_files = {
            "fhir_igs": [],
            "guardrails": [],
            "glue": [],
            "ddl": [],
            "ncpdp_general_standards": [],
            "ncpdp_script_standards": []
        }
        
        print("\n📂 Validating Files...\n")
        
        # Validate and normalize chatbot-provided files
        for source in ['guardrails', 'glue', 'ddl']:
            files = partial_config['input_files'].get(source, [])
            
            found = 0
            for filepath_str in files:
                filepath = Path(filepath_str)
                
                if filepath.exists():
                    # Normalize to relative path
                    rel_path = self._normalize_path(filepath)
                    matched_files[source].append(rel_path)
                    found += 1
                else:
                    warnings.append(f"⚠️  {source.capitalize()}: File not found: {filepath}")
            
            print(f"   {source.capitalize()}: {found}/{len(files)} files found")
        
        # Match FHIR/IG resources
        print(f"\n   FHIR/IG Resources:")
        for resource in fhir_resources.get('fhir_igs', []):
            filename = resource['filename']
            matches = self._find_exact_file(self.standards_fhir_ig, filename)
            
            if matches:
                rel_path = self._normalize_path(matches)
                
                # Build complete metadata
                file_entry = {
                    "file": rel_path,
                    "filename": filename,
                    "resource_name": resource['resource_name'],
                    "file_type": resource['file_type'],
                    "ig_source": resource['ig_source'],
                    "priority": resource.get('priority', 1),
                    "reasoning": resource['reasoning']
                }
                
                matched_files['fhir_igs'].append(file_entry)
                print(f"      ✅ {resource['resource_name']} ({resource['file_type']}): {filename}")
            else:
                warnings.append(f"⚠️  FHIR/IG: File not found: {filename}")
                print(f"      ⚠️  {resource['resource_name']}: {filename} NOT FOUND")
        
        # Validate NCPDP general standards
        print(f"\n   NCPDP General Standards:")
        ncpdp_general_file = self.standards_ncpdp / "ncpdp_general_standards.json"
        
        if ncpdp_general_file.exists():
            general_codes = self._validate_ncpdp_codes(
                ncpdp_general_file,
                ncpdp_resources.get('ncpdp_general_standards', []),
                warnings
            )
            matched_files['ncpdp_general_standards'] = general_codes
        else:
            warnings.append(f"⚠️  NCPDP: General standards file not found: {ncpdp_general_file}")
        
        # Validate NCPDP script standards
        print(f"\n   NCPDP SCRIPT Standards:")
        ncpdp_script_file = self.standards_ncpdp / "ncpdp_script_standards.json"
        
        if ncpdp_script_file.exists():
            script_codes = self._validate_ncpdp_codes(
                ncpdp_script_file,
                ncpdp_resources.get('ncpdp_script_standards', []),
                warnings
            )
            matched_files['ncpdp_script_standards'] = script_codes
        else:
            warnings.append(f"⚠️  NCPDP: SCRIPT standards file not found: {ncpdp_script_file}")
        
        return matched_files, warnings
    
    def _find_exact_file(self, base_dir: Path, filename: str) -> Optional[Path]:
        """Recursively search for exact filename in directory tree."""
        if not base_dir.exists():
            return None
        
        # Use rglob for recursive search
        matches = list(base_dir.rglob(filename))
        
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            # Multiple matches - return first, but this shouldn't happen with exact filenames
            print(f"      ⚠️  Warning: Multiple matches for {filename}, using first")
            return matches[0]
        
        return None
    
    def _normalize_path(self, filepath: Path) -> str:
        """Convert absolute path to relative (if within project)."""
        filepath = Path(filepath)
        
        if filepath.is_absolute():
            try:
                return str(filepath.relative_to(self.project_root))
            except ValueError:
                # Path is outside project, keep as absolute
                return str(filepath)
        
        return str(filepath)
    
    def _validate_ncpdp_codes(self, ncpdp_file: Path, ai_standards: List[Dict], warnings: List[str]) -> List[Dict]:
        """Validate NCPDP standard codes exist in file and preserve metadata."""
        validated_standards = []
        
        try:
            with open(ncpdp_file, 'r', encoding='utf-8') as f:
                ncpdp_data = json.load(f)
            
            # New structure: {"_standards": {...}, "A": [...fields...], "F": [...fields...], ...}
            # Check if code exists as a key in the data
            standards_map = ncpdp_data.get('_standards', {})
            
            for standard in ai_standards:
                code = standard['code']
                
                # Check if this standard code exists in the data
                if code in ncpdp_data and isinstance(ncpdp_data[code], list):
                    # Get standard name from _standards metadata or use AI-provided name
                    standard_name = standards_map.get(code, standard.get('name', code))
                    
                    validated_standards.append({
                        "code": code,
                        "name": standard.get('name', standard_name),
                        "reasoning": standard['reasoning']
                    })
                    print(f"      ✅ {code}: {standard.get('name', standard_name)}")
                else:
                    warnings.append(f"⚠️  NCPDP: Standard code '{code}' not found in {ncpdp_file.name}")
                    print(f"      ⚠️  {code}: NOT FOUND")
        
        except Exception as e:
            warnings.append(f"⚠️  NCPDP: Could not parse {ncpdp_file.name}: {e}")
        
        return validated_standards
    
    def generate_final_config(
        self, 
        partial_config: Dict, 
        fhir_resources: Dict,
        ncpdp_resources: Dict,
        matched_files: Dict,
        thresholds: Dict
    ) -> Dict:
        """Generate complete configuration file."""
        
        domain_name = partial_config['cdm']['domain']
        
        config = {
            "cdm": {
                "domain": domain_name,
                "type": partial_config['cdm']['type'],
                "description": partial_config['cdm']['description'],
                "version": "1.0"
            },
            "input_files": matched_files,
            "thresholds": thresholds,
            "output": {
                "directory": f"output/{self._safe_dirname(domain_name)}",
                "filename": f"{domain_name.replace(' ', '_')}_CDM.xlsx"
            },
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "ai_analysis": {
                    "fhir_assessment": fhir_resources.get('domain_assessment', {}),
                    "ncpdp_assessment": ncpdp_resources.get('domain_assessment', {})
                },
                "generator_version": "2.0"
            }
        }
        
        # Add core dependency if functional
        if partial_config['cdm'].get('core_dependency'):
            config['cdm']['core_dependency'] = partial_config['cdm']['core_dependency']
        
        return config
    
    def save_config(self, config: Dict, partial_config_file: str) -> Path:
        """Save configuration to timestamped file."""
        # Get base name from partial config file
        base_path = Path(partial_config_file)
        base_name = base_path.stem  # e.g., "config_plan"
        
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.json"
        filepath = self.config_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        
        return filepath
    
    def _safe_dirname(self, name: str) -> str:
        """Convert domain name to safe directory/filename."""
        return name.lower().replace(' ', '_').replace('&', 'and')
    
    def run(self, partial_config_file: str):
        """Execute full config completion workflow."""
        try:
            print("\n=== CDM Configuration Generator v2.0 ===\n")
            
            # Step 1: Load partial config
            print(f"📄 Loading partial config: {partial_config_file}")
            partial_config = self.load_partial_config(partial_config_file)
            print(f"   Domain: {partial_config['cdm']['domain']}")
            print(f"   Type: {partial_config['cdm']['type']}")
            
            # Check for existing config
            existing_config = self.load_existing_config(partial_config_file)
            if existing_config:
                print(f"   ℹ️  Found existing config - can reuse skipped sections")
            
            # Step 2: Prompt for thresholds
            thresholds = self.prompt_thresholds(partial_config)
            
            # Step 3: AI analysis - FHIR (with skip option)
            run_fhir = self.prompt_skip_analysis("FHIR")
            
            if run_fhir:
                fhir_resources = self.determine_fhir_resources(partial_config)
                
                # Display FHIR recommendations
                print("\n✅ FHIR Analysis Complete")
                print(f"\n   Recommended FHIR/IG Files: {len(fhir_resources.get('fhir_igs', []))}")
                
                # Group by resource for display
                resources_by_name = {}
                for f in fhir_resources.get('fhir_igs', []):
                    name = f['resource_name']
                    if name not in resources_by_name:
                        resources_by_name[name] = []
                    resources_by_name[name].append(f)
                
                for resource_name, files in resources_by_name.items():
                    print(f"\n      {resource_name}:")
                    for f in files:
                        priority_str = f"(Priority {f.get('priority', 1)})" if f.get('priority', 1) > 1 else ""
                        print(f"         • {f['file_type']} - {f['ig_source']} {priority_str}")
                        print(f"           {f['reasoning'][:80]}...")
                
                fhir_assessment = fhir_resources.get('domain_assessment', {})
                print(f"\n   Primary IGs: {', '.join(fhir_assessment.get('primary_igs', []))}")
                print(f"   Expected Entities: {fhir_assessment.get('expected_entity_count', 'unknown')}")
                print(f"   Confidence: {fhir_assessment.get('confidence', 'unknown')}")
            else:
                print("\n⏭️  Skipping FHIR analysis")
                if existing_config and 'fhir_igs' in existing_config.get('input_files', {}):
                    fhir_resources = {
                        'fhir_igs': existing_config['input_files']['fhir_igs'],
                        'domain_assessment': existing_config.get('metadata', {}).get('ai_analysis', {}).get('fhir_assessment', {})
                    }
                    print(f"   ℹ️  Using {len(fhir_resources['fhir_igs'])} FHIR files from existing config")
                else:
                    fhir_resources = {'fhir_igs': [], 'domain_assessment': {}}
                    print("   ⚠️  No existing FHIR data found - will be empty")
            
            # Step 4: AI analysis - NCPDP (with skip option)
            run_ncpdp = self.prompt_skip_analysis("NCPDP")
            
            if run_ncpdp:
                ncpdp_resources = self.determine_ncpdp_standards(partial_config)
                
                # Display NCPDP recommendations
                print("\n✅ NCPDP Analysis Complete")
                print(f"\n   Recommended NCPDP General Standards: {len(ncpdp_resources.get('ncpdp_general_standards', []))}")
                for std in ncpdp_resources.get('ncpdp_general_standards', []):
                    print(f"      • {std['code']}: {std['reasoning'][:60]}...")
                
                print(f"\n   Recommended NCPDP SCRIPT Standards: {len(ncpdp_resources.get('ncpdp_script_standards', []))}")
                for std in ncpdp_resources.get('ncpdp_script_standards', []):
                    print(f"      • {std['code']}: {std['reasoning'][:60]}...")
                
                ncpdp_assessment = ncpdp_resources.get('domain_assessment', {})
                print(f"\n   NCPDP Relevance: {ncpdp_assessment.get('ncpdp_relevance', 'unknown')}")
                print(f"   Confidence: {ncpdp_assessment.get('confidence', 'unknown')}")
            else:
                print("\n⏭️  Skipping NCPDP analysis")
                if existing_config and 'ncpdp_general_standards' in existing_config.get('input_files', {}):
                    ncpdp_resources = {
                        'ncpdp_general_standards': existing_config['input_files']['ncpdp_general_standards'],
                        'ncpdp_script_standards': existing_config['input_files']['ncpdp_script_standards'],
                        'domain_assessment': existing_config.get('metadata', {}).get('ai_analysis', {}).get('ncpdp_assessment', {})
                    }
                    gen_count = len(ncpdp_resources['ncpdp_general_standards'])
                    script_count = len(ncpdp_resources['ncpdp_script_standards'])
                    print(f"   ℹ️  Using {gen_count} general + {script_count} SCRIPT standards from existing config")
                else:
                    ncpdp_resources = {
                        'ncpdp_general_standards': [],
                        'ncpdp_script_standards': [],
                        'domain_assessment': {}
                    }
                    print("   ⚠️  No existing NCPDP data found - will be empty")
            
            # Step 5: Validate and match files
            matched_files, warnings = self.validate_and_match_files(
                partial_config, 
                fhir_resources, 
                ncpdp_resources
            )
            
            # Step 6: Generate final config
            print("\n📝 Generating final configuration...")
            final_config = self.generate_final_config(
                partial_config,
                fhir_resources,
                ncpdp_resources,
                matched_files,
                thresholds
            )
            
            # Step 7: Save config
            filepath = self.save_config(final_config, partial_config_file)
            
            # Final report
            print(f"\n{'='*60}")
            print(f"✅ Configuration Complete: {filepath.name}")
            print(f"{'='*60}")
            
            if warnings:
                print(f"\n⚠️  Warnings ({len(warnings)}):")
                for warning in warnings:
                    print(f"   {warning}")
                print(f"\n   Review warnings and manually fix config if needed.")
            
            print(f"\n📊 Summary:")
            print(f"   FHIR/IG files: {len(final_config['input_files']['fhir_igs'])}")
            print(f"   Guardrails files: {len(final_config['input_files']['guardrails'])}")
            print(f"   Glue files: {len(final_config['input_files']['glue'])}")
            print(f"   DDL files: {len(final_config['input_files']['ddl'])}")
            print(f"   NCPDP General standards: {len(final_config['input_files']['ncpdp_general_standards'])}")
            print(f"   NCPDP SCRIPT standards: {len(final_config['input_files']['ncpdp_script_standards'])}")
            
            print(f"\n🚀 Next Steps:")
            print(f"   1. Review config: {filepath}")
            print(f"   2. Fix any warnings (optional)")
            print(f"   3. Run: python cdm_orchestrator.py {filepath.name}")
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    """Entry point for config generator."""
    if len(sys.argv) < 2:
        print("Usage: python config/config_generator.py <partial_config_file>")
        print("\nExample:")
        print("  python config/config_generator.py config/config_plan.json")
        sys.exit(1)
    
    partial_config_file = sys.argv[1]
    
    generator = ConfigGenerator()
    generator.run(partial_config_file)


if __name__ == "__main__":
    main()