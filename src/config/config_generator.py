"""
Config Generator for CDM Creation Application - Version 2.2

Completes chatbot-filled config templates by:
1. Reading partial config from chatbot
2. Prompting for thresholds
3. Using AI to determine required FHIR/IG resources (exact filenames)
   - Pass 1: Primary resources for CDM domain
   - Pass 2: Referenced resources (e.g., Organization for InsurancePlan)
4. Using AI to determine required NCPDP standards
5. Validating all files exist
6. Generating complete config JSON

Features:
- Skip FHIR or NCPDP analysis to save time
- Preserves skipped sections from most recent timestamped config
- Timestamped outputs: config_plan_20251121_232439.json
- timeout=1800, encoding='utf-8', list/dict NCPDP handling
- NEW in 2.1: Two-pass FHIR analysis to capture referenced resources
- NEW in 2.2: AI-based FHIR filename correction for missing files

Usage:
    python src/config/config_generator.py input/business/cdm_plan/config/config_plan.json
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add project root to path for imports (src/config -> src -> project_root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.llm_client import LLMClient


class ConfigGenerator:
    """Complete CDM configuration files from chatbot-filled templates."""
    
    def __init__(self):
        self.llm_client = LLMClient(timeout=1800)
        self.config_dir = Path(__file__).parent  # src/config
        self.src_dir = self.config_dir.parent     # src
        self.project_root = self.src_dir.parent   # project root
        self.input_dir = self.project_root / "input"
        self.standards_fhir_ig = self.input_dir / "strd_fhir_ig"
        self.standards_ncpdp = self.input_dir / "strd_ncpdp"
        self._fhir_file_list_cache = None  # Cache for FHIR file list
    
    # =========================================================================
    # FHIR Filename Correction Methods (NEW in v2.2)
    # =========================================================================
    
    def _load_fhir_file_list(self) -> List[str]:
        """Load the list of available FHIR filenames from fhir_file_list.txt."""
        if self._fhir_file_list_cache is not None:
            return self._fhir_file_list_cache
        
        file_list_path = self.standards_fhir_ig / "fhir_file_list.txt"
        
        if not file_list_path.exists():
            print(f"   ⚠️  fhir_file_list.txt not found at {file_list_path}")
            print(f"   ℹ️  Generate with: Get-ChildItem -Path \"input\\strd_fhir_ig\" -Recurse -Filter \"*.json\" | Select-Object -ExpandProperty Name | Sort-Object -Unique | Out-File -FilePath \"input\\strd_fhir_ig\\fhir_file_list.txt\"")
            self._fhir_file_list_cache = []
            return []
        
        try:
            with open(file_list_path, 'r', encoding='utf-8') as f:
                filenames = [line.strip() for line in f.readlines() if line.strip()]
            self._fhir_file_list_cache = filenames
            return filenames
        except Exception as e:
            print(f"   ⚠️  Error reading fhir_file_list.txt: {e}")
            self._fhir_file_list_cache = []
            return []
    
    def _correct_fhir_filename(self, missing_filename: str, resource_name: str, file_type: str) -> Optional[str]:
        """DEPRECATED - use _correct_missing_fhir_files_batch instead."""
        pass
    
    def _correct_missing_fhir_files_batch(self, missing_files: List[Dict]) -> Dict[str, str]:
        """Batch correct all missing FHIR filenames in a single AI call.
        
        Args:
            missing_files: List of dicts with 'filename', 'resource_name', 'file_type'
            
        Returns:
            Dict mapping original filename -> corrected filename (only for successful matches)
        """
        if not missing_files:
            return {}
            
        available_files = self._load_fhir_file_list()
        if not available_files:
            return {}
        
        # Format missing files for prompt
        missing_list = "\n".join([
            f"  - {f['filename']} (resource: {f['resource_name']}, type: {f['file_type']})"
            for f in missing_files
        ])
        
        prompt = f"""Match these missing FHIR filenames to correct actual filenames.

MISSING FILES:
{missing_list}

AVAILABLE FILES:
{chr(10).join(available_files)}

COMMON FILENAME MISMATCHES - CHECK ALL:

1. CASE: File type prefix is lowercase
   CodeSystem-xxx → codesystem-xxx
   ValueSet-xxx → valueset-xxx
   StructureDefinition-xxx → structuredefinition-xxx

2. HYPHENS REMOVED from compound words (AFTER the prefix):
   insurance-plan-type → insuranceplan-type
   coverage-type → coveragetype
   benefit-category → benefitcategory

3. "ex-" PREFIX for example/extensible code systems:
   benefit-category → ex-benefitcategory
   diagnosis-type → ex-diagnosistype
   payee-type → ex-payeetype

4. COMBINED PATTERNS (most common):
   CodeSystem-insurance-plan-type.json → codesystem-insuranceplan-type.json
   ValueSet-benefit-category.json → valueset-ex-benefitcategory.json

Return a JSON object mapping each missing filename to its corrected filename.
Use NO_MATCH if no reasonable match exists.

Example response:
{{
  "CodeSystem-insurance-plan-type.json": "codesystem-insuranceplan-type.json",
  "ValueSet-benefit-qualifier.json": "NO_MATCH"
}}

Return ONLY valid JSON:"""

        try:
            messages = [{"role": "user", "content": prompt}]
            response_text, _ = self.llm_client.chat(messages)
            
            # Parse response
            response_text = response_text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
            
            corrections = json.loads(response_text)
            
            # Validate corrections exist in available files
            available_set = set(available_files)
            validated = {}
            for original, corrected in corrections.items():
                if corrected != "NO_MATCH" and corrected in available_set:
                    validated[original] = corrected
            
            return validated
            
        except Exception as e:
            print(f"      ⚠️  Batch filename correction error: {e}")
            return {}
    
    def _correct_missing_fhir_files(self, fhir_resources: Dict) -> Tuple[Dict, List[str]]:
        """Attempt to correct missing FHIR filenames using single batch AI call."""
        
        # First pass: identify all missing files
        missing_files = []
        for resource in fhir_resources.get('fhir_igs', []):
            filename = resource['filename']
            matches = self._find_exact_file(self.standards_fhir_ig, filename)
            if not matches:
                missing_files.append({
                    'filename': filename,
                    'resource_name': resource['resource_name'],
                    'file_type': resource['file_type']
                })
        
        if not missing_files:
            return fhir_resources, []
        
        # Single batch call for all missing files
        print(f"      🔍 Attempting to correct {len(missing_files)} missing filename(s)...")
        corrections_map = self._correct_missing_fhir_files_batch(missing_files)
        
        # Apply corrections
        corrections = []
        for resource in fhir_resources.get('fhir_igs', []):
            original = resource['filename']
            if original in corrections_map:
                corrected = corrections_map[original]
                corrections.append(f"{original} → {corrected}")
                resource['filename'] = corrected
        
        return fhir_resources, corrections
        
    def load_partial_config(self, filepath: str) -> Dict:
        """Load chatbot-filled partial config."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
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
        # Get base name and directory from partial config file
        base_path = Path(partial_config_file)
        base_name = base_path.stem  # e.g., "config_plan"
        config_dir = base_path.parent  # Search in same directory as input config
        
        # Find all timestamped configs matching pattern: config_plan_YYYYMMDD_HHMMSS.json
        pattern = f"{base_name}_*.json"
        timestamped_configs = sorted(config_dir.glob(pattern), reverse=True)
        
        if timestamped_configs:
            try:
                with open(timestamped_configs[0], 'r', encoding='utf-8') as f:
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
        """Use AI to determine required FHIR/IG resources with exact filenames.
        
        Two-pass approach:
        - Pass 1: Select primary resources for the CDM domain
        - Pass 2: Identify and add critical referenced resources (e.g., Organization)
        """
        
        # Pass 1: Primary resource selection
        prompt = self._build_fhir_determination_prompt(partial_config)
        
        print("\n🤖 Pass 1: Analyzing FHIR/IG requirements with AI...\n")
        
        try:
            # Use chat() method and parse JSON
            messages = [{"role": "user", "content": prompt}]
            response_text, _ = self.llm_client.chat(messages)
            
            # Parse JSON response
            pass1_result = json.loads(response_text)
            
            # Display Pass 1 results
            pass1_resources = pass1_result.get('fhir_igs', [])
            print(f"   Pass 1 selected {len(pass1_resources)} resources")
            
        except json.JSONDecodeError as e:
            print(f"Error parsing AI response as JSON: {e}")
            print(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            print(f"Error during FHIR AI analysis: {e}")
            raise
        
        # Pass 2: Reference resolution
        pass2_result = self.determine_fhir_references(partial_config, pass1_resources)
        
        # Merge results
        additional_resources = pass2_result.get('additional_fhir_igs', [])
        if additional_resources:
            # Check for duplicates before adding
            existing_filenames = {r['filename'] for r in pass1_resources}
            for resource in additional_resources:
                if resource['filename'] not in existing_filenames:
                    pass1_resources.append(resource)
                    print(f"   + Added from references: {resource['resource_name']}")
        
        # Update assessment with Pass 2 info
        domain_assessment = pass1_result.get('domain_assessment', {})
        domain_assessment['pass2_resources_added'] = len(additional_resources)
        domain_assessment['pass2_assessment'] = pass2_result.get('pass2_assessment', {})
        
        return {
            'fhir_igs': pass1_resources,
            'domain_assessment': domain_assessment
        }
    
    def determine_fhir_references(self, partial_config: Dict, pass1_resources: List[Dict]) -> Dict:
        """Pass 2: Identify and add critical referenced FHIR resources."""
        
        # Skip if no StructureDefinitions were selected in Pass 1
        structure_defs = [r for r in pass1_resources if r.get('file_type') == 'StructureDefinition']
        if not structure_defs:
            print("   ℹ️  No StructureDefinitions in Pass 1, skipping reference analysis")
            return {'additional_fhir_igs': [], 'pass2_assessment': {}}
        
        prompt = self._build_fhir_references_prompt(partial_config, pass1_resources)
        
        print("\n🤖 Pass 2: Analyzing FHIR references...\n")
        
        try:
            messages = [{"role": "user", "content": prompt}]
            response_text, _ = self.llm_client.chat(messages)
            
            # Parse JSON response
            result = json.loads(response_text)
            
            # Display results
            added = result.get('additional_fhir_igs', [])
            assessment = result.get('pass2_assessment', {})
            
            if added:
                print(f"   Found {len(added)} additional resources from references:")
                for r in added:
                    print(f"      + {r['resource_name']} ({r['file_type']})")
            else:
                print("   No additional referenced resources needed")
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"Error parsing Pass 2 AI response as JSON: {e}")
            print(f"Response text: {response_text[:500]}...")
            return {'additional_fhir_igs': [], 'pass2_assessment': {'error': str(e)}}
        except Exception as e:
            print(f"Error during FHIR Pass 2 analysis: {e}")
            return {'additional_fhir_igs': [], 'pass2_assessment': {'error': str(e)}}
    
    def determine_ncpdp_standards(self, partial_config: Dict) -> Dict:
        """Use AI to determine required NCPDP standards."""
        
        # Load actual standards from NCPDP files
        general_standards = self._load_ncpdp_standards_list("ncpdp_general_standards.json")
        script_standards = self._load_ncpdp_standards_list("ncpdp_script_standards.json")
        
        prompt = self._build_ncpdp_determination_prompt(partial_config, general_standards, script_standards)
        
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
    
    def _load_ncpdp_standards_list(self, filename: str) -> Dict[str, str]:
        """Load _standards mapping from NCPDP file."""
        ncpdp_file = self.standards_ncpdp / filename
        if ncpdp_file.exists():
            with open(ncpdp_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('_standards', {})
        return {}
    
    def _build_ncpdp_determination_prompt(self, partial_config: Dict, general_standards: Dict[str, str], script_standards: Dict[str, str]) -> str:
        """Build prompt for AI to determine NCPDP standards."""
        
        cdm = partial_config['cdm']
        
        # Format standards lists for the prompt
        general_list = "\n".join([f'   - "{code}": "{name}"' for code, name in general_standards.items()])
        script_list = "\n".join([f'   - "{code}": "{name}"' for code, name in script_standards.items()]) if script_standards else "   (No SCRIPT standards available)"
        
        return f"""You are an NCPDP standards expert analyzing and selecting the appropriate NCPDP Standard Format Key to support building out a Canonical Data Model (CDM) for the specific domain.

# Task
Based on your knowledge of NCPDP Standards as well as industry understanding of Pharmacy Benefit Management, Specialty Drug, and Care Management, select the Standard Formats (A-Z) that contain the NCPDP data elements that will be of HIGHEST VALUE for the creation of Entities and Attributes in the specified CDM.

# CDM Metadata
- **Domain**: {cdm['domain']}
- **Type**: {cdm['type']}
- **Description**: {cdm['description']}

# VALID NCPDP STANDARDS - SELECT ONLY FROM THESE LISTS

## General Standards (ncpdp_general_standards):
{general_list}

## SCRIPT Standards (ncpdp_script_standards):
{script_list}

# CRITICAL Selection Rules

1. **SELECT ONLY THOSE STANDARD FORMATS THAT ARE OF HIGHEST VALUE**
   a. This may be no resources to several resources
   b. Empty arrays are valid and can be correct for certain domains
   c. Only include standards that clearly pass ALL criteria below

2. **STANDARD FORMAT SELECTION TEST**
   a. Ask: "Does this Standard Format's data elements provide high value in defining the specified CDM?"
   b. SELECT only if the Standard Format's data elements primary purpose is to DEFINE data structures for this CDM domain
   c. REJECT if the Standard Format's data elements merely USES or REFERENCES fields from this domain in transactions
   d. BALANCE IN SELECTING the Standard Formats is CRITICAL. Include the Standard Format ONLY when it directly describes the core entities in this CDM. AVOID Standard Formats where the data elements in that Standard Format primarily represent details, logic, or processing rules that belong in OTHER DOMAINS.
   e. Balancing also includes determining when there are VERY FEW Standard Format's data elements that DIRECTLY support the creation of this CDM. ALWAYS CONSIDER THE FOLLOWING: Are the few identified data elements of enough value to the CDM in representing the domain to add all the Standard Format's data elements?  The answer may be YES, or the answer may be NO. Only if the answer is YES, add those Standard Formats with VERY FEW HIGH VALUE data elements.

3. **ENTITY-DEFINING vs FIELD-USING**
   a. SELECT: Standards that DEFINE entities, attributes, and data structures
   b. EXTREME CAUTION IN SELECTING: Transaction/message standards that happen to INCLUDE fields from this domain. Select ONLY if the data elements have VERY HIGH VALUE TO THE CREATION OF THE CDM

4. **STANDARD NAME vs CONTENT**
   a. Do not select/reject a Standard Format based on a name matching CDM keywords
   b. Evaluate what data elements in the Standard Format actually define
   c. A standard's name may not fully reflect its content scope

5. **VERIFICATION**
    a. For every NCPDP Standards format, review the data elements and provide a brief reasoning why it was or was not selected in the notes section.

# Output Format

Respond with ONLY valid JSON:

{{
  "ncpdp_general_standards": [
    {{
      "code": "X",
      "name": "Standard Name",
      "reasoning": "Primary purpose: [what it defines]. Relevant to {cdm['domain']} CDM because: [specific reason]."
    }}
  ],
  "ncpdp_script_standards": [
    {{
      "code": "SX",
      "name": "SCRIPT Standard Name", 
      "reasoning": "Primary purpose: [what it defines]. Relevant to {cdm['domain']} CDM because: [specific reason]."
    }}
  ],
  "domain_assessment": {{
    "ncpdp_relevance": "high | medium | low | none",
    "confidence": "high | medium | low",
    "notes": "Assessment of NCPDP fit for {cdm['domain']} domain."
  }}
}}

# Critical Requirements
- Return ONLY valid JSON, no markdown or code blocks
- Use ONLY codes from the provided lists above
- Empty arrays are acceptable
- Apply PRIMARY PURPOSE TEST to every candidate
- Provide reasoning that explains primary purpose alignment

Respond with JSON only:"""

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
        
        return f"""You are a FHIR IG expert analyzing the appropriate FHIR IGs to support building out a CDM for the select domain.

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
   a. The selected FHIR resource files are used in a downstream step to define the CDM entities and attributes. Another step defines the relationships between the CDMs.
   b. Return ONLY resources that represent entities and attributes IN this CDM.
   c. Do NOT include additional **StructureDefinition** resources whose primary entity is modeled in a DIFFERENT CDM that would be expected in a PBM, Specialty Drug or Care Management organization, even if they are referenced. However, you SHOULD include ValueSets and CodeSystems that are needed for the selected StructureDefinitions in this CDM.
   d. For each FHIR resource selected, include an explanation as to why it was selected for this CDM.

3. **IG Priority for Resource Selection**
   a. When multiple IGs define the same resource, you need to choose the appropriate IG(s) based on CDM domain context to select the resource.
   b. When a resource exists in more than one IG, choose the 1 or 2 BEST FIT duplicate FHIR resources that your knowledge of FHIR, PBM, Specialty Drug, and Care Management will support the CDM:
      i. Only include alternates if they add clear domain-specific value.
      ii. If duplicates exist, assign priority (1=use first, 2=alternate).

4. **FHIR resource file types to Include**
   a. For each resource, identify ALL relevant file types that your knowledge of FHIR, PBM, Specialty Drug, and Care Management will be of high value to support the CDM:
      i. **StructureDefinition-*.json** - Entity/attribute model (REQUIRED)
      ii. **CodeSystem-*.json** - Coded value definitions
      iii. **ValueSet-*.json** - Allowed value constraints
      iv. **CapabilityStatement-*.json** - IG context (optional)

5. **CRITICAL: Include Directly Supporting Terminology for Selected StructureDefinitions**
   a. When you select a StructureDefinition, identify the coded elements that are most important for this CDM's domain (for example: **type**, category, status, level, class, funding model, line of business, etc.). For those elements, you MUST include the ValueSets and CodeSystems that define their allowed values.
   b. Treat any element literally named "type" on the selected StructureDefinition (e.g., InsurancePlan.type, Organization.type, Contract.type) as a high-value classification field. If the IG defines a ValueSet or CodeSystem for that "type" element, you MUST include those terminology files in the output, as long as they exist in the relevant FHIR/IG packages.
   c. The StructureDefinition defines the entity structure; the ValueSets/CodeSystems define the valid values for coded elements. Without the needed supporting terminology, the downstream rationalization will have incomplete type mappings.
   d. BALANCE IN SELECTING supporting files is CRITICAL. Include supporting terminology ONLY when it directly describes the core identity, classification, or administrative characteristics of the entity in this CDM. Avoid terminology that is primarily about details, logic, or processing rules that belong in other domains.   

6. **Exact Filenames**
   a. You have complete FHIR R4 and IG knowledge.
   b. Return EXACT filenames as they appear in standard IGs.
   c. Avoid using canonical FHIR names, ONLY use standard IG file naming conventions.

# Output Format

Respond with ONLY valid JSON:

{{
  "fhir_igs": [
    {{
      "filename": "StructureDefinition-[name].json",
      "resource_name": "[name]",
      "file_type": "StructureDefinition",
      "ig_source": "[IG]",
      "priority": 1,
      "reasoning": "Primary [name] resource for [domain]. [IG] is baseline standard."
    }},
    {{
      "filename": "ValueSet-[name]-class.json",
      "resource_name": "[name]Class",
      "file_type": "ValueSet",
      "ig_source": "[IG]",
      "priority": 1,
      "reasoning": "Defines [name] classification codes."
    }},
    {{
      "filename": "StructureDefinition-[profile]-[name].json",
      "resource_name": "[name]",
      "file_type": "StructureDefinition",
      "ig_source": "[IG]",
      "priority": 2,
      "reasoning": "Alternate: [IG] extends base [name] with domain-specific fields."
    }}
  ],
  "domain_assessment": {{
    "primary_igs": ["[IG 1]", "[IG 2]"],
    "expected_entity_count": 5,
    "confidence": "high | medium | low",
    "notes": "Brief assessment of IG fit for this domain."
  }}
}}

# Critical Requirements
- Return ONLY valid JSON, no markdown or code blocks.
- Use exact FHIR R4/IG standard filenames.
- Assign priority (1=primary, 2=alternate) for duplicate resources.
- Focus on resources for CDM entities and attributes, not additional entities whose CDMs are out of scope.
- **ALWAYS include ValueSets and CodeSystems that support high-value coded elements (type, category, status, class, level, etc.) in the selected StructureDefinitions for this CDM.**
- Provide clear, specific reasoning for each file.

Respond with JSON only:"""
    
    def _build_fhir_references_prompt(self, partial_config: Dict, selected_resources: List[Dict]) -> str:
        """Build prompt for Pass 2: identify critical referenced resources."""
        
        cdm = partial_config['cdm']
        
        # Format selected resources for the prompt
        resources_list = "\n".join([
            f"   - {r['resource_name']} ({r['file_type']}) from {r['ig_source']}"
            for r in selected_resources
            if r.get('file_type') == 'StructureDefinition'
        ])
        
        return f"""You are a FHIR expert performing Pass 2 analysis: identifying CRITICAL referenced resources.

# Context

In Pass 1, we selected these FHIR StructureDefinitions for the {cdm['domain']} CDM:

{resources_list}

# Task

For each selected StructureDefinition above:
1. Identify elements with Reference() types that point to OTHER resources
2. Determine if those referenced resources contain data CRITICAL to this CDM
3. Return additional StructureDefinitions (and their ValueSets/CodeSystems) that should be included

# CDM Metadata
- **Domain**: {cdm['domain']}
- **Type**: {cdm['type']}
- **Description**: {cdm['description']}

# CRITICAL Rules for Reference Analysis

1. **FIRST: Extract Exclusions from CDM Description**
   Before analyzing any references, identify ALL exclusions stated in the CDM description.
   Look for phrases like:
   - "Explicitly excludes:"
   - "Exclude all..."
   - "Does not include..."
   - "...belongs to [X] CDM"
   - "Relies on other CDMs for..."
   
   These exclusions are ABSOLUTE - no referenced resource that falls into an excluded category may be added.

2. **Include Referenced Resource IF:**
   a. It contains identity/classification data for entities WITHIN THIS CDM's stated scope
   b. Business attributes would be UNMAPPED without it
   c. The reference is to a resource that DEFINES entities, not just links to them
   d. It does NOT fall into any category listed in the CDM description's exclusions
   
3. **Do NOT Include Referenced Resource IF:**
   a. It falls into ANY exclusion category from the CDM description
   b. It represents a domain the CDM description says it "relies on" or "depends on" another CDM for
   c. The reference is transactional/operational, not definitional
   d. Including it would expand scope beyond THIS CDM's stated purpose
   e. For Functional CDMs: it belongs to the Core CDM this functional CDM depends on

4. **Applying Exclusions - Examples:**
   
   IF CDM description says "Explicitly excludes: plan identity and hierarchy"
   THEN: Organization (payer/carrier identity), InsurancePlan structure → EXCLUDE
   
   IF CDM description says "Explicitly excludes: member eligibility"
   THEN: Patient, RelatedPerson, Coverage.beneficiary → EXCLUDE
   
   IF CDM description says "Explicitly excludes: drug/formulary definitions"
   THEN: MedicationKnowledge, FormularyItem → EXCLUDE
   
   IF CDM description says "Explicitly excludes: claims or encounter structures"
   THEN: Claim, Encounter, ExplanationOfBenefit → EXCLUDE

5. **Core vs Functional CDM Handling:**
   - Core CDMs define foundational entities (Plan, Drug, Eligibility)
   - Functional CDMs operate ON core entities (Benefit operates on Plan)
   - If this is a Functional CDM with core_dependency, resources belonging to that Core CDM are OUT OF SCOPE

6. **Reference Analysis Process:**
   For each reference found in primary resources:
   a. Identify what domain/category the referenced resource belongs to
   b. Check if that category appears in CDM description exclusions
   c. If excluded → include: false with reasoning citing the exclusion
   d. If not excluded → evaluate if it contains critical definitional data for THIS CDM

# Output Format

Respond with ONLY valid JSON:

{{
  "exclusions_identified": [
    "plan identity and hierarchy",
    "member eligibility", 
    "drug/formulary definitions"
  ],
  "referenced_resources_analysis": [
    {{
      "source_resource": "Coverage",
      "reference_element": "beneficiary",
      "referenced_resource": "Patient",
      "include": false,
      "exclusion_match": "member eligibility",
      "reasoning": "CDM description explicitly excludes member eligibility - Patient belongs to Eligibility CDM"
    }},
    {{
      "source_resource": "InsurancePlan",
      "reference_element": "ownedBy",
      "referenced_resource": "Organization",
      "include": false,
      "exclusion_match": "plan identity and hierarchy",
      "reasoning": "CDM description explicitly excludes plan identity - Organization (payer/carrier) belongs to Plan CDM"
    }},
    {{
      "source_resource": "SomeResource",
      "reference_element": "someElement",
      "referenced_resource": "SomeReference",
      "include": true,
      "exclusion_match": null,
      "reasoning": "Contains [X] data within this CDM's scope, not excluded by description"
    }}
  ],
  "additional_fhir_igs": [
    {{
      "filename": "structuredefinition-example.json",
      "resource_name": "Example",
      "file_type": "StructureDefinition",
      "ig_source": "FHIR R4 Base",
      "priority": 1,
      "reasoning": "Referenced by [X]. Contains [Y] data within CDM scope. Not excluded by description."
    }}
  ],
  "pass2_assessment": {{
    "references_analyzed": 5,
    "resources_added": 1,
    "exclusions_applied": 3,
    "confidence": "high | medium | low",
    "notes": "Summary of reference analysis. X references excluded per CDM description."
  }}
}}

# Critical Requirements
- FIRST extract and list all exclusions from CDM description before analyzing
- Return ONLY valid JSON, no markdown or code blocks
- Use exact FHIR R4/IG standard filenames (lowercase)
- Every exclusion decision must cite which exclusion phrase it matches
- Empty additional_fhir_igs array is EXPECTED if all references fall into exclusions
- The CDM description is the AUTHORITATIVE source for scope decisions

Respond with JSON only:"""
    
    def validate_and_match_files(self, partial_config: Dict, fhir_resources: Dict, ncpdp_resources: Dict, existing_config: Optional[Dict] = None) -> Tuple[Dict, List[str]]:
        """Validate input files exist and match AI-determined resources to actual files.
        
        If existing_config is provided, guardrails/glue/ddl are taken from it instead of partial_config.
        """
        
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
        
        # Validate and normalize guardrails/glue/ddl files
        # If existing_config exists, use it; otherwise use partial_config (base template)
        for source in ['guardrails', 'glue', 'ddl']:
            if existing_config and source in existing_config.get('input_files', {}):
                files = existing_config['input_files'].get(source, [])
                source_label = "existing config"
            else:
                files = partial_config['input_files'].get(source, [])
                source_label = "base template"
            
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
            
            print(f"   {source.capitalize()}: {found}/{len(files)} files found (from {source_label})")
        
        # Check for missing FHIR files and attempt AI-based correction
        missing_fhir = []
        for resource in fhir_resources.get('fhir_igs', []):
            if not self._find_exact_file(self.standards_fhir_ig, resource['filename']):
                missing_fhir.append(resource['filename'])
        
        if missing_fhir:
            print(f"\n   🔍 Found {len(missing_fhir)} missing FHIR file(s). Attempting AI correction...")
            fhir_resources, corrections = self._correct_missing_fhir_files(fhir_resources)
            if corrections:
                print(f"   ✅ Corrected {len(corrections)} filename(s):")
                for c in corrections:
                    print(f"      • {c}")
            else:
                print(f"   ⚠️  No corrections found")
        
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
                "generator_version": "2.2"
            }
        }
        
        # Add core dependency if functional
        if partial_config['cdm'].get('core_dependency'):
            config['cdm']['core_dependency'] = partial_config['cdm']['core_dependency']
        
        return config
    
    def save_config(self, config: Dict, partial_config_file: str) -> Path:
        """Save configuration to timestamped file."""
        # Get base name and directory from partial config file
        base_path = Path(partial_config_file)
        base_name = base_path.stem  # e.g., "config_plan"
        output_dir = base_path.parent  # Save to same directory as input config
        
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def _safe_dirname(self, name: str) -> str:
        """Convert domain name to safe directory/filename."""
        return name.lower().replace(' ', '_').replace('&', 'and')
    
    def run(self, partial_config_file: str):
        """Execute full config completion workflow."""
        try:
            print("\n=== CDM Configuration Generator v2.2 ===\n")
            
            # Step 1: Load config - prefer existing timestamped config over partial
            existing_config = self.load_existing_config(partial_config_file)
            
            if existing_config:
                print(f"   ℹ️  Using existing timestamped config as base")
                partial_config = existing_config
            else:
                print(f"📄 Loading partial config: {partial_config_file}")
                partial_config = self.load_partial_config(partial_config_file)
            
            print(f"   Domain: {partial_config['cdm']['domain']}")
            print(f"   Type: {partial_config['cdm']['type']}")
            
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
                
                # Show Pass 2 results if any
                if fhir_assessment.get('pass2_resources_added', 0) > 0:
                    print(f"   Pass 2 Resources Added: {fhir_assessment.get('pass2_resources_added')}")
            else:
                print("\n⏭️  Skipping FHIR analysis")
                if 'fhir_igs' in partial_config.get('input_files', {}):
                    fhir_resources = {
                        'fhir_igs': partial_config['input_files']['fhir_igs'],
                        'domain_assessment': partial_config.get('metadata', {}).get('ai_analysis', {}).get('fhir_assessment', {})
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
                if 'ncpdp_general_standards' in partial_config.get('input_files', {}):
                    ncpdp_resources = {
                        'ncpdp_general_standards': partial_config['input_files']['ncpdp_general_standards'],
                        'ncpdp_script_standards': partial_config['input_files'].get('ncpdp_script_standards', []),
                        'domain_assessment': partial_config.get('metadata', {}).get('ai_analysis', {}).get('ncpdp_assessment', {})
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
                ncpdp_resources,
                existing_config
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
        print("Usage: python src/config/config_generator.py <partial_config_file>")
        print("\nExample:")
        print("  python src/config/config_generator.py input/business/cdm_plan/config/config_plan.json")
        sys.exit(1)
    
    partial_config_file = sys.argv[1]
    
    generator = ConfigGenerator()
    generator.run(partial_config_file)


if __name__ == "__main__":
    main()