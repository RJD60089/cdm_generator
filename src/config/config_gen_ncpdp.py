"""
NCPDP configuration generator for CDM.

Handles:
- NCPDP standard format selection (General and SCRIPT)
- AI-based analysis for CDM relevance
- Standard code validation
"""
import json
from pathlib import Path
from typing import Dict, List, Optional

from . import config_utils
from .config_gen_core import ConfigGeneratorBase


class NCPDPConfigGenerator(ConfigGeneratorBase):
    """NCPDP standards analysis and selection for CDM configuration."""
    
    def __init__(self, cdm_name: str, llm_client=None):
        """Initialize NCPDP config generator.
        
        Args:
            cdm_name: CDM name (e.g., 'plan', 'formulary')
            llm_client: LLM client for AI analysis
        """
        super().__init__(cdm_name, llm_client)
        self.ncpdp_dir = config_utils.get_standards_ncpdp_dir()
    
    def run_analysis(self, config: Dict, dry_run: bool = False) -> Dict:
        """Run NCPDP standards analysis.
        
        Args:
            config: Partial config dict with CDM metadata
            dry_run: If True, save prompts but don't call LLM
            
        Returns:
            Dict with ncpdp_general_standards, ncpdp_script_standards, domain_assessment
        """
        print("\n🤖 NCPDP Analysis")
        
        # Load available standards
        general_standards = self._load_standards_list("ncpdp_general_standards.json")
        script_standards = self._load_standards_list("ncpdp_script_standards.json")
        
        if not general_standards and not script_standards:
            print("   ⚠️  No NCPDP standards files found")
            return {
                'ncpdp_general_standards': [],
                'ncpdp_script_standards': [],
                'domain_assessment': {'ncpdp_relevance': 'unknown'}
            }
        
        prompt = self._build_analysis_prompt(config, general_standards, script_standards)
        
        if dry_run:
            return {
                'ncpdp_general_standards': [],
                'ncpdp_script_standards': [],
                'domain_assessment': {},
                '_prompt': prompt
            }
        
        try:
            response_text = self.call_llm(prompt)
            result = self.parse_ai_json_response(response_text)
            
            # Handle flat response format (auto-split by code prefix)
            result = self._normalize_response(result)
            
            # Display results
            gen_count = len(result.get('ncpdp_general_standards', []))
            script_count = len(result.get('ncpdp_script_standards', []))
            print(f"   ✓ Selected {gen_count} general + {script_count} SCRIPT standards")
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"   ❌ Error parsing NCPDP response: {e}")
            raise
        except Exception as e:
            print(f"   ❌ Error in NCPDP analysis: {e}")
            raise
    
    def validate_codes(self, ncpdp_resources: Dict) -> Dict:
        """Validate NCPDP standard codes exist in standards files.
        
        Args:
            ncpdp_resources: Dict with ncpdp_general_standards and ncpdp_script_standards
            
        Returns:
            Dict with validated standards and any warnings
        """
        warnings = []
        validated = {
            'ncpdp_general_standards': [],
            'ncpdp_script_standards': [],
            'domain_assessment': ncpdp_resources.get('domain_assessment', {})
        }
        
        # Validate general standards
        general_file = self.ncpdp_dir / "ncpdp_general_standards.json"
        if general_file.exists():
            validated['ncpdp_general_standards'], gen_warnings = self._validate_codes_against_file(
                general_file,
                ncpdp_resources.get('ncpdp_general_standards', [])
            )
            warnings.extend(gen_warnings)
        
        # Validate SCRIPT standards
        script_file = self.ncpdp_dir / "ncpdp_script_standards.json"
        if script_file.exists():
            validated['ncpdp_script_standards'], script_warnings = self._validate_codes_against_file(
                script_file,
                ncpdp_resources.get('ncpdp_script_standards', [])
            )
            warnings.extend(script_warnings)
        
        validated['_warnings'] = warnings
        return validated
    
    def _load_standards_list(self, filename: str) -> Dict[str, str]:
        """Load _standards mapping from NCPDP file.
        
        Args:
            filename: NCPDP standards filename
            
        Returns:
            Dict mapping code -> name
        """
        ncpdp_file = self.ncpdp_dir / filename
        if ncpdp_file.exists():
            try:
                data = config_utils.load_json_file(ncpdp_file)
                return data.get('_standards', {})
            except Exception:
                pass
        return {}
    
    def _validate_codes_against_file(self, ncpdp_file: Path, ai_standards: List[Dict]) -> tuple:
        """Validate standard codes exist in file.
        
        Args:
            ncpdp_file: Path to NCPDP standards file
            ai_standards: List of standards from AI
            
        Returns:
            Tuple of (validated standards list, warnings list)
        """
        validated = []
        warnings = []
        
        try:
            data = config_utils.load_json_file(ncpdp_file)
            standards_map = data.get('_standards', {})
            
            for standard in ai_standards:
                code = standard['code']
                
                # Check if code exists as a key (excluding _standards metadata)
                if code in data and isinstance(data[code], list):
                    standard_name = standards_map.get(code, standard.get('name', code))
                    validated.append({
                        "code": code,
                        "name": standard.get('name', standard_name),
                        "reasoning": standard['reasoning']
                    })
                    print(f"      ✅ {code}: {standard.get('name', standard_name)}")
                else:
                    warnings.append(f"NCPDP code '{code}' not found in {ncpdp_file.name}")
                    print(f"      ⚠️  {code}: NOT FOUND")
                    
        except Exception as e:
            warnings.append(f"Could not parse {ncpdp_file.name}: {e}")
        
        return validated, warnings
    
    def _normalize_response(self, ai_response: Dict) -> Dict:
        """Normalize AI response, auto-splitting if needed.
        
        Handles case where AI returns flat 'ncpdp_standards' list instead
        of split general/script lists.
        
        Args:
            ai_response: Raw AI response dict
            
        Returns:
            Normalized response with split lists
        """
        # Already properly split
        if 'ncpdp_general_standards' in ai_response and 'ncpdp_script_standards' in ai_response:
            return ai_response
        
        # Auto-split flat list by code prefix
        general = []
        script = []
        
        all_standards = ai_response.get('ncpdp_standards', [])
        if all_standards:
            print("   Auto-splitting NCPDP standards by code prefix...")
            for std in all_standards:
                code = std['code'].upper()
                # SCRIPT standards typically start with S or Q
                if code.startswith('S') or code.startswith('Q'):
                    script.append(std)
                else:
                    general.append(std)
        
        return {
            'ncpdp_general_standards': general,
            'ncpdp_script_standards': script,
            'domain_assessment': ai_response.get('domain_assessment', {})
        }
    
    def _build_analysis_prompt(self, config: Dict, general_standards: Dict, script_standards: Dict) -> str:
        """Build prompt for NCPDP standards analysis."""
        cdm = config['cdm']
        
        # Format standards lists
        general_list = "\n".join([
            f'   - "{code}": "{name}"' for code, name in general_standards.items()
        ]) or "   (No general standards available)"
        
        script_list = "\n".join([
            f'   - "{code}": "{name}"' for code, name in script_standards.items()
        ]) or "   (No SCRIPT standards available)"
        
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

3. **ENTITY-DEFINING vs FIELD-USING**
   a. SELECT: Standards that DEFINE entities, attributes, and data structures
   b. EXTREME CAUTION: Transaction/message standards that happen to INCLUDE fields from this domain

4. **STANDARD NAME vs CONTENT**
   a. Do not select/reject based on name matching CDM keywords
   b. Evaluate what data elements the Standard Format actually defines

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

Respond with JSON only:"""
