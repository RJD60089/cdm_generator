"""
FHIR/IG configuration generator for CDM.

Handles:
- Two-pass FHIR resource analysis (primary + references)
- AI-based filename correction for missing files
- Resource validation against available files

Work Item 4: Added canonical_url capture for VS/CS entries (used in post-process terminology enrichment)
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import config_utils
from .config_gen_core import ConfigGeneratorBase


class FHIRConfigGenerator(ConfigGeneratorBase):
    """FHIR resource analysis and selection for CDM configuration."""
    
    def __init__(self, cdm_name: str, llm_client=None):
        """Initialize FHIR config generator.
        
        Args:
            cdm_name: CDM name (e.g., 'plan', 'formulary')
            llm_client: LLM client for AI analysis
        """
        super().__init__(cdm_name, llm_client)
        self.fhir_dir = config_utils.get_standards_fhir_dir()
        self._file_list_cache = None
    
    def run_analysis(self, config: Dict, dry_run: bool = False) -> Dict:
        """Run FHIR analysis (Pass 1 only, correction happens in validate step).
        
        Args:
            config: Partial config dict with CDM metadata
            dry_run: If True, save prompts but don't call LLM
            
        Returns:
            Dict with fhir_igs list and domain_assessment
        """
        print("\n🤖 FHIR Analysis")
        
        # Pass 1: Primary resource selection
        result = self._run_pass1(config, dry_run)
        
        return result
    
    def _run_pass1(self, config: Dict, dry_run: bool = False) -> Dict:
        """Pass 1: Select primary FHIR resources for CDM.
        
        Args:
            config: Config with CDM metadata
            dry_run: If True, return prompt without calling LLM
            
        Returns:
            Dict with fhir_igs and domain_assessment
        """
        print("\n   === Pass 1: Primary Resource Selection ===")
        
        prompt = self._build_pass1_prompt(config)
        
        if dry_run:
            return {
                'fhir_igs': [],
                'domain_assessment': {},
                '_prompt': prompt
            }
        
        try:
            response_text = self.call_llm(prompt)
            result = self.parse_ai_json_response(response_text)
            
            count = len(result.get('fhir_igs', []))
            print(f"   ✓ Pass 1 selected {count} resources")
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"   ❌ Error parsing Pass 1 response: {e}")
            raise
        except Exception as e:
            print(f"   ❌ Error in Pass 1: {e}")
            raise
    
    def validate_and_correct_files(self, fhir_resources: Dict) -> Tuple[Dict, List[str]]:
        """Validate FHIR files exist and attempt to correct missing filenames.
        
        Args:
            fhir_resources: Dict with fhir_igs list
            
        Returns:
            Tuple of (corrected resources, list of corrections made)
        """
        # Validate all files and categorize
        found_files = []
        missing_files = []
        
        for resource in fhir_resources.get('fhir_igs', []):
            filename = resource['filename']
            
            # Handle NOMATCH: prefix from Pass 1
            if filename.startswith('NOMATCH:'):
                # Strip prefix for correction attempt
                actual_filename = filename[8:]  # len('NOMATCH:') = 8
                resource['filename'] = actual_filename
                missing_files.append({
                    'filename': actual_filename,
                    'resource_name': resource['resource_name'],
                    'file_type': resource['file_type']
                })
            elif config_utils.find_file_recursive(self.fhir_dir, filename):
                found_files.append(resource)
            else:
                missing_files.append({
                    'filename': filename,
                    'resource_name': resource['resource_name'],
                    'file_type': resource['file_type']
                })
        
        # Print summary to terminal
        print(f"\n   📁 File Validation:")
        print(f"      ✓ Found: {len(found_files)} files")
        if missing_files:
            print(f"      ✗ Missing/NOMATCH: {len(missing_files)} files")
            for mf in missing_files[:10]:  # Show first 10
                print(f"         - {mf['filename']}")
            if len(missing_files) > 10:
                print(f"         ... and {len(missing_files) - 10} more")
        
        if not missing_files:
            print(f"      ✓ All files found - skipping correction step")
            return fhir_resources, []
        
        # Load file list with verbose output for correction context
        print(f"\n   🔍 Attempting to correct {len(missing_files)} missing filename(s)...")
        available_files = self._load_file_list(verbose=True)
        
        if not available_files:
            print(f"   ⚠️  No file list available for correction")
            # Remove missing files from result
            fhir_resources['fhir_igs'] = found_files
            return fhir_resources, []
        
        corrections_map = self._correct_missing_files_batch(missing_files)
        
        # Apply corrections and track results
        corrections = []
        still_missing = []
        
        for resource in fhir_resources.get('fhir_igs', []):
            original = resource['filename']
            if original in corrections_map:
                corrected = corrections_map[original]
                corrections.append(f"{original} → {corrected}")
                resource['filename'] = corrected
                print(f"      ✓ {original} → {corrected}")
            elif not config_utils.find_file_recursive(self.fhir_dir, original):
                still_missing.append(original)
        
        # Report still missing
        if still_missing:
            print(f"\n   ⚠️  Still missing (will be skipped): {len(still_missing)} files")
            for f in still_missing[:5]:
                print(f"         - {f}")
            if len(still_missing) > 5:
                print(f"         ... and {len(still_missing) - 5} more")
            
            # Remove still-missing files from the result
            fhir_resources['fhir_igs'] = [
                r for r in fhir_resources.get('fhir_igs', [])
                if r['filename'] not in still_missing
            ]
            print(f"   ✓ Removed {len(still_missing)} unavailable files from config")
        
        return fhir_resources, corrections
    
    def _load_file_list(self, verbose: bool = False) -> List[str]:
        """Load list of available FHIR filenames from cache file.
        
        Args:
            verbose: If True, print file statistics
            
        Returns:
            List of available filenames
        """
        if self._file_list_cache is not None:
            return self._file_list_cache
        
        file_list_path = self.fhir_dir / "fhir_file_list.txt"
        
        if not file_list_path.exists():
            print(f"   ⚠️  fhir_file_list.txt not found at {file_list_path}")
            print(f"      Generate with: find <fhir_dir> -name '*.json' -type f -printf '%f\\n' > fhir_file_list.txt")
            self._file_list_cache = []
            return []
        
        try:
            with open(file_list_path, 'r', encoding='utf-8') as f:
                filenames = [line.strip() for line in f.readlines() if line.strip()]
            self._file_list_cache = filenames
            
            if verbose:
                # Count by type
                structs = sum(1 for f in filenames if f.lower().startswith('structuredefinition-'))
                vsets = sum(1 for f in filenames if f.lower().startswith('valueset-'))
                csys = sum(1 for f in filenames if f.lower().startswith('codesystem-'))
                print(f"   📂 Available FHIR files: {len(filenames)} total")
                print(f"      StructureDefinitions: {structs}, ValueSets: {vsets}, CodeSystems: {csys}")
            
            return filenames
        except Exception as e:
            print(f"   ⚠️  Error reading fhir_file_list.txt: {e}")
            self._file_list_cache = []
            return []
    
    def _correct_missing_files_batch(self, missing_files: List[Dict]) -> Dict[str, str]:
        """Batch correct missing FHIR filenames using AI (Pass 2).
        
        Args:
            missing_files: List of dicts with filename, resource_name, file_type
            
        Returns:
            Dict mapping original filename -> corrected filename
        """
        if not missing_files or self.llm_client is None:
            return {}
        
        available_files = self._load_file_list()
        if not available_files:
            return {}
        
        # Filter available files by type to reduce context
        missing_by_type = {}
        for f in missing_files:
            ftype = f['file_type']
            if ftype not in missing_by_type:
                missing_by_type[ftype] = []
            missing_by_type[ftype].append(f)
        
        # Build filtered available files list
        filtered_available = []
        for ftype in missing_by_type.keys():
            prefix = ftype.lower() + '-'
            type_files = [f for f in available_files if f.lower().startswith(prefix)]
            filtered_available.extend(type_files)
        
        # Format missing files for prompt
        missing_list = "\n".join([
            f"  - {f['filename']} (resource: {f['resource_name']}, type: {f['file_type']})"
            for f in missing_files
        ])
        
        prompt = f"""You are a FHIR expert. Match these missing filenames to the correct actual filenames.

# MISSING FILES (from Pass 1):
{missing_list}

# AVAILABLE FILES TO MATCH AGAINST:
(List is ALPHABETICALLY SORTED - scan to the appropriate prefix section: CodeSystem-*, StructureDefinition-*, ValueSet-*)

{chr(10).join(sorted(filtered_available))}

# MATCHING INSTRUCTIONS

1. **SEMANTIC MATCHING** - The missing filename represents a FHIR concept. Find the file that represents the SAME concept, even if named differently.
   
   Examples:
   - "usdf-CoveragePlan" might match "usdf-PayerInsurancePlan" or "insurance-plan-coverage"
   - "usdf-FormularyItemExtension" might match "usdf-AdditionalCoverageInformation-extension"

2. **IG PREFIX MATCHING** - Try matching within the same IG first:
   - usdf- (US Drug Formulary)
   - carin-bb- or C4BB- (CARIN Blue Button)
   - davinci- or hrex- (Da Vinci)
   - us-core- (US Core)
   - qicore- (QI Core)
   
   If no match in same IG, try base FHIR or other IGs.

3. **CASE AND HYPHEN VARIATIONS**:
   - CoveragePlan → coverage-plan or coverageplan
   - FormularyItem → formulary-item or formularyitem
   - Prefix case: StructureDefinition- vs structuredefinition-

4. **NO MATCH** - If the concept truly doesn't exist in available files, return "NO_MATCH"

# OUTPUT FORMAT

Return a JSON object mapping each missing filename to its corrected filename OR "NO_MATCH":

{{
  "StructureDefinition-usdf-CoveragePlan.json": "StructureDefinition-usdf-PayerInsurancePlan.json",
  "ValueSet-usdf-SomethingNotReal.json": "NO_MATCH"
}}

**CRITICAL**: Every corrected filename MUST be from the AVAILABLE FILES list above.

Return ONLY valid JSON:"""

        try:
            response_text = self.call_llm(prompt)
            corrections = self.parse_ai_json_response(response_text)
            
            # Validate corrections exist in available files
            available_set = set(available_files)
            validated = {}
            matched = 0
            no_match = 0
            
            for original, corrected in corrections.items():
                if corrected == "NO_MATCH":
                    no_match += 1
                elif corrected in available_set:
                    validated[original] = corrected
                    matched += 1
                else:
                    # AI returned a file not in list - ignore
                    no_match += 1
            
            print(f"      ✓ Matched: {matched}, No match: {no_match}")
            
            return validated
            
        except Exception as e:
            print(f"   ⚠️  Filename correction error: {e}")
            return {}
    
    def _build_pass1_prompt(self, config: Dict) -> str:
        """Build Pass 1 prompt for primary resource selection.
        
        Work Item 4: Added canonical_url to output format for VS/CS entries.
        """
        cdm = config['cdm']
        
        # Load available file list
        available_files = self._load_file_list()
        
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
   a. The selected FHIR resource files are used in a downstream step to define the CDM entities and attributes.
   b. Return ONLY resources that represent entities and attributes IN this CDM.
   c. Do NOT include StructureDefinition resources whose primary entity is modeled in a DIFFERENT CDM.
   d. Include ValueSets and CodeSystems needed for selected StructureDefinitions.

3. **IG Priority for Resource Selection**
   a. When multiple IGs define the same resource, choose based on CDM domain context.
   b. Assign priority (1=primary, 2=alternate) for duplicates.

4. **FHIR resource file types to Include**
   a. **StructureDefinition-*.json** - Entity/attribute model (REQUIRED)
   b. **CodeSystem-*.json** - Coded value definitions
   c. **ValueSet-*.json** - Allowed value constraints
   d. **CapabilityStatement-*.json** - IG context (optional)

5. **Include Supporting Terminology**
   a. For selected StructureDefinitions, include ValueSets and CodeSystems for key coded elements.
   b. Focus on type, category, status, level, class, funding model elements.

6. **Exact Filenames**
   a. Return EXACT filenames as they appear in the AVAILABLE FHIR FILES list below.
   b. ONLY select filenames from the list - do NOT invent or guess filenames.
   c. If an identified resource has NO match in the list, include it with filename prefix "NOMATCH:" (e.g., "NOMATCH:StructureDefinition-expected-name.json") - these will be resolved in a correction step.

7. **Respect CDM Exclusions**
   a. Parse "Excludes" or "Explicitly excludes" statements in the CDM description carefully.
   b. Do NOT include resources whose primary purpose matches an excluded domain.
   c. Examples of exclusion enforcement:
      - If CDM excludes "member eligibility" → do NOT include Coverage, Patient, or enrollment resources
      - If CDM excludes "formulary definitions" → do NOT include Formulary, FormularyItem, FormularyDrug
      - If CDM excludes "prior authorization/UM rules" → do NOT include PA, StepTherapy, QuantityLimit extensions
      - If CDM excludes "plan hierarchy/identity" → do NOT include plan-level InsurancePlan profiles
   d. When uncertain whether a resource belongs to an excluded domain, EXCLUDE it.

8. **Canonical URL for ValueSets and CodeSystems**
   a. For ValueSet and CodeSystem files, include the canonical_url field.
   b. This is the FHIR canonical URL (e.g., "http://hl7.org/fhir/us/davinci-drug-formulary/ValueSet/DrugTierVS")
   c. The canonical_url is used downstream to link terminology to attribute bindings.
   d. If you don't know the exact canonical URL, use the pattern: http://hl7.org/fhir/<ig-path>/<ResourceType>/<resource-name>

# AVAILABLE FHIR FILES
(Alphabetically sorted - scan to CodeSystem-*, StructureDefinition-*, or ValueSet-* section)

{chr(10).join(available_files)}

# Output Format

Respond with ONLY valid JSON:

{{
  "fhir_igs": [
    {{
      "filename": "StructureDefinition-usdf-FormularyItem.json",
      "resource_name": "usdf-FormularyItem",
      "file_type": "StructureDefinition",
      "ig_source": "US Drug Formulary",
      "priority": 1,
      "reasoning": "Primary formulary item resource."
    }},
    {{
      "filename": "ValueSet-usdf-DrugTierVS.json",
      "resource_name": "DrugTierVS",
      "file_type": "ValueSet",
      "ig_source": "US Drug Formulary",
      "priority": 1,
      "canonical_url": "http://hl7.org/fhir/us/davinci-drug-formulary/ValueSet/DrugTierVS",
      "reasoning": "Drug tier value set for formulary tiering."
    }},
    {{
      "filename": "CodeSystem-usdf-DrugTierCS.json",
      "resource_name": "DrugTierCS",
      "file_type": "CodeSystem",
      "ig_source": "US Drug Formulary",
      "priority": 1,
      "canonical_url": "http://hl7.org/fhir/us/davinci-drug-formulary/CodeSystem/usdf-DrugTierCS",
      "reasoning": "Drug tier code system defining tier codes."
    }},
    {{
      "filename": "NOMATCH:StructureDefinition-usdf-CoveragePlan.json",
      "resource_name": "usdf-CoveragePlan",
      "file_type": "StructureDefinition",
      "ig_source": "US Drug Formulary",
      "priority": 1,
      "reasoning": "Coverage plan structure - not found in list, needs correction."
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
- PREFER filenames from the AVAILABLE FHIR FILES list above.
- Use "NOMATCH:" prefix for identified resources not found in list.
- STRICTLY EXCLUDE resources matching excluded domains in CDM description.
- Assign priority (1=primary, 2=alternate) for duplicate resources.
- Focus on resources for CDM entities and attributes.
- Include supporting ValueSets and CodeSystems WITH canonical_url field.
- canonical_url is REQUIRED for ValueSet and CodeSystem entries.

Respond with JSON only:"""