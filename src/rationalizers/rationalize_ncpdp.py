"""
NCPDP Rationalization Module
Transforms NCPDP standards file into unified rationalized format
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# LLM client type hint - actual import happens at runtime
try:
    from src.core.llm_client import LLMClient
except ImportError:
    LLMClient = None


class NCPDPRationalizer:
    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        self.cdm_description = self.config.get('cdm', {}).get('description', '')
        
        # Handle standards as objects with 'code', 'name', 'reasoning' fields
        input_files = self.config.get('input_files', {})
        general_raw = input_files.get('ncpdp_general_standards', [])
        script_raw = input_files.get('ncpdp_script_standards', [])
        
        # Build standards info preserving all metadata
        self.ncpdp_general_standards = []
        self.ncpdp_script_standards = []
        self.standards_info = {}  # code -> {name, reasoning}
        
        for s in general_raw:
            if isinstance(s, dict):
                code = s.get('code', '')
                self.ncpdp_general_standards.append(code)
                self.standards_info[code] = {
                    'name': s.get('name', ''),
                    'reasoning': s.get('reasoning', '')
                }
            else:
                self.ncpdp_general_standards.append(s)
                self.standards_info[s] = {'name': '', 'reasoning': ''}
        
        for s in script_raw:
            if isinstance(s, dict):
                code = s.get('code', '')
                self.ncpdp_script_standards.append(code)
                self.standards_info[code] = {
                    'name': s.get('name', ''),
                    'reasoning': s.get('reasoning', '')
                }
            else:
                self.ncpdp_script_standards.append(s)
                self.standards_info[s] = {'name': '', 'reasoning': ''}
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  General standards: {self.ncpdp_general_standards}")
        print(f"  SCRIPT standards: {self.ncpdp_script_standards}")
    
    def map_ncpdp_type_to_sql(self, field_format: str, field_length: str) -> str:
        """Map NCPDP field format to SQL type"""
        if not field_format:
            return "VARCHAR(255)"
        
        # Handle multiple formats (e.g., "x(15) --------------- x35")
        field_format = field_format.split()[0] if ' ' in field_format else field_format
        field_length = field_length.split()[0] if field_length and ' ' in field_length else field_length
        
        # x(n) = VARCHAR(n)
        if match := re.match(r'x\((\d+)\)', field_format, re.IGNORECASE):
            return f"VARCHAR({match.group(1)})"
        
        # 9(n) = INTEGER if length reasonable, otherwise VARCHAR
        if match := re.match(r'9\((\d+)\)', field_format):
            length = int(match.group(1))
            return "INTEGER" if length <= 10 else f"VARCHAR({length})"
        
        # s9(n)v99 or 9(n)v999 = DECIMAL
        if match := re.match(r's?9\((\d+)\)v(\d+)', field_format):
            precision = int(match.group(1)) + len(match.group(2))
            scale = len(match.group(2))
            return f"DECIMAL({precision},{scale})"
        
        # an = VARCHAR(255) default
        if field_format == "an":
            return "VARCHAR(255)"
        
        return f"VARCHAR({field_length if field_length else '255'})"
    
    def transform_field_to_attribute(self, field: Dict[str, Any], standard_code: str, source_file: str) -> Dict[str, Any]:
        """Transform NCPDP field to common rationalized attribute format"""
        field_id = field.get('id', '')
        field_code = field.get('i', '')
        field_name = field.get('n', '')
        field_def = field.get('d', '')
        field_format = field.get('t', '')
        field_length = field.get('l', '')
        comments = field.get('o', '')
        
        # Normalize attribute name
        attr_name = field_name.replace(' ', '_').replace('/', '_').replace('-', '_').lower()
        attr_name = re.sub(r'_+', '_', attr_name).strip('_')
        
        sql_type = self.map_ncpdp_type_to_sql(field_format, field_length)
        
        # Parse length if present
        length_val = None
        if field_length:
            try:
                length_val = int(field_length)
            except (ValueError, TypeError):
                pass
        
        # Build attribute in common format
        attr = {
            "attribute_name": attr_name,
            "description": field_def,
            "data_type": sql_type,
            "source_attribute": field_name,
            "source_files": [source_file],
            "required": False,  # NCPDP doesn't specify required
            "nullable": True,
            "cardinality": {"min": 0, "max": "1"},
            "length": length_val,
            "precision": None,
            "scale": None,
            "default_value": None,
            "is_array": False,
            "is_nested": False,
            "is_pii": False,
            "is_phi": False,
            "data_classification": None,
            "business_context": comments if comments else None,
            "business_rules": None,
            "validation_rules": None,
            "is_calculated": False,
            "calculation_dependency": None,
            "source_metadata": {
                "source_id": field_id,
                "source_ref": f"{field_code} | {standard_code}",
                "source_data_type": field_format if field_format else None,
                "source_length": field_length if field_length else None
            }
        }
        
        return attr
    
    def build_prune_prompt(self, entity_name: str, source_detail: str, business_purpose: str, 
                           raw_fields: List[Dict[str, Any]]) -> str:
        """Build prompt for AI to identify fields to keep - uses raw NCPDP data"""
        
        prompt = f"""You are a data architect analyzing NCPDP standard fields for relevance to the CDM domain specified below.

## CDM CONTEXT

**Domain:** {self.cdm_domain}
**Classification:** {self.cdm_classification}
**Description:** {self.cdm_description}

## ENTITY TO ANALYZE

**Entity:** {entity_name}
**Source:** {source_detail}
**Business Purpose:** {business_purpose}

## FIELD FORMAT KEY

The fields below use abbreviated keys:
- "id": Unique field identifier
- "i": Field code
- "n": Name of field
- "d": Definition of field
- "t": Field format
- "l": Field length
- "o": Comments / examples
- "s": Standard format

## FIELDS ({len(raw_fields)} total)

```json
{json.dumps(raw_fields, indent=2)}
```

## YOUR TASK ##
Review each field and determine if it should be retained for downstream processing to create the CDM for the Plan domain. The purpose of this task is to REDUCE FIELDS THAT ARE CLEARLY NOT RELATED TO THIS DOMAIN to avoid unnecessary processing downstream.

## THE APPROACH ##
It is BETTER to leave a field for downstream processing that will not be used. It is WORSE to remove a field that could possibly be used.

## DIRECTIONTIONS ## 
KEEP a field if:
- ALWAYS consider the Definition of Field, in addition to the Field Code, Name or Format 
- It could reasonably be used in this domain
- It relates to identification, dates, or interpretive codes/classifications (used to qualify or interpret another field that is relevant to the current CDM's domain)
- You are unsure whether it belongs
- It might be useful for relationships or context

## IMPORTANT GUIDANCE ##
When in doubt, KEEP the field - Err on the side of inclusion. It is better to keep 10 unnecessary fields than to remove 1 necessary one.

## INTERPRETATION OF DOMAIN DESCRIPTION ## 
The Description is provided to give general context about the CDM domain, but it was originally 
written for selecting standards, not for pruning attributes. For this pruning task, treat the 
Description as a high-level hint; not a strict set of removal rules.

You MUST NOT remove an attribute solely because the Description mentions that certain topics are 
handled by other domains. These exclusions should NOT be interpreted as instructions to drop any 
attribute that uses similar words.

Use the Description ONLY to identify attributes that are clearly and unambiguously outside 
the structural intent of this domain. The Description should guide you gently, but not 
override the core KEEP rules (identifiers, dates, statuses, qualifiers, relationship/linkage fields).

## OUTPUT FORMAT

Return ONLY valid JSON with the list of field IDs to keep:

```json
{{
  "entity_name": "{entity_name}",
  "fields_reviewed": {len(raw_fields)},
  "keep": ["T_001", "T_002", ...]
}}
```

CRITICAL: 
- Return ONLY valid JSON (no markdown, no code blocks, no commentary)
- Use exact field IDs from the "id" values provided
- When in doubt, KEEP the field
"""
        return prompt
    
    def prune_fields_with_ai(self, entity_name: str, source_detail: str, business_purpose: str,
                             raw_fields: List[Dict[str, Any]], standard_code: str) -> Tuple[List[Dict[str, Any]], int, int]:
        """Use AI to prune raw fields, return filtered raw fields"""
        
        prompt = self.build_prune_prompt(entity_name, source_detail, business_purpose, raw_fields)
        
        # Dry run - save prompt
        if self.dry_run:
            if self.prompts_dir:
                prompt_file = self.prompts_dir / f"prune_{entity_name}_{datetime.now().strftime('%H%M%S')}.txt"
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(prompt)
                print(f"    Prompt saved: {prompt_file.name}")
            return raw_fields, len(raw_fields), 0  # Return all in dry run
        
        # No LLM - return all
        if not self.llm:
            print(f"    Warning: No LLM client, skipping prune for {entity_name}")
            return raw_fields, len(raw_fields), 0
        
        print(f"    Pruning {entity_name} ({len(raw_fields)} fields)...")
        
        messages = [
            {
                "role": "system",
                "content": "You are a data architect. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ]
        
        try:
            response, token_usage = self.llm.chat(messages)
            
            # Parse response
            response_clean = response.strip()
            if response_clean.startswith("```"):
                lines = response_clean.split("\n")
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
            
            result = json.loads(response_clean)
            
            # Get list of field IDs to keep
            keep_ids = set(result.get('keep', []))
            
            # Debug: Check what AI returned
            print(f"    AI returned {len(keep_ids)} IDs to keep")
            
            # Get all field IDs from raw data
            raw_ids = {f.get('id') for f in raw_fields if f.get('id')}
            
            # Check for mismatches
            missing_in_raw = keep_ids - raw_ids
            if missing_in_raw:
                print(f"    WARNING: {len(missing_in_raw)} IDs from AI not found in raw fields: {list(missing_in_raw)[:5]}...")
            
            # Filter raw fields by ID
            original_count = len(raw_fields)
            kept_fields = [f for f in raw_fields if f.get('id') in keep_ids]
            kept_count = len(kept_fields)
            removed_count = original_count - kept_count
            
            # Debug: Check if filtering worked
            if kept_count != len(keep_ids):
                matched_ids = len(keep_ids) - len(missing_in_raw)
                print(f"    WARNING: AI returned {len(keep_ids)} IDs, but only {kept_count} matched raw fields")
            
            print(f"    ✓ Kept {kept_count}/{original_count} fields")
            
            return kept_fields, original_count, removed_count
            
        except json.JSONDecodeError as e:
            print(f"    ERROR: Failed to parse AI response: {e}")
            print(f"    Response preview: {response[:200] if response else 'empty'}...")
            return raw_fields, len(raw_fields), 0
        except Exception as e:
            print(f"    ERROR: AI pruning failed: {e}")
            return raw_fields, len(raw_fields), 0
    
    def rationalize_standard(self, standard_code: str, raw_fields: List[Dict[str, Any]], 
                             standards_map: Dict[str, str]) -> Dict[str, Any]:
        """Rationalize a single standard: prune first, then transform"""
        
        std_info = self.standards_info.get(standard_code, {})
        std_name = std_info.get('name') or standards_map.get(standard_code, f"Standard_{standard_code}")
        
        entity_name = std_name.replace(' ', '_').replace('/', '_')
        source_detail = f"NCPDP {std_name}"
        business_purpose = std_info.get('reasoning', '')
        
        # Step 1: Prune raw fields with AI
        kept_fields, original_count, removed_count = self.prune_fields_with_ai(
            entity_name, source_detail, business_purpose, raw_fields, standard_code
        )
        
        # Step 2: Transform kept fields to attributes
        attributes = []
        for field in kept_fields:
            attr = self.transform_field_to_attribute(field, standard_code, f"ncpdp_{standard_code.lower()}_standards.json")
            attributes.append(attr)
        
        # Build entity in common format
        entity = {
            "entity_name": entity_name,
            "description": f"NCPDP {std_name} data elements",
            "source_type": "NCPDP",
            "source_info": {
                "files": [f"ncpdp_{standard_code.lower()}_standards.json"],
                "api": None,
                "schema": None,
                "table": None,
                "url": None,
                "version": None
            },
            "business_context": business_purpose,
            "technical_context": None,
            "ai_metadata": {
                "selection_reasoning": business_purpose,
                "pruning_notes": f"Pruned {removed_count} of {original_count} fields ({len(attributes)} kept)"
            },
            "attributes": attributes,
            "source_metadata": {
                "standard_code": standard_code,
                "standard_name": std_name,
                "source_detail": source_detail,
                "original_count": original_count,
                "kept_count": len(attributes),
                "removed_count": removed_count
            }
        }
        
        return entity
    
    def run(self, ncpdp_general_path: str, ncpdp_script_path: str, output_dir: str) -> Optional[str]:
        """Run rationalization, return output file path"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Set up prompts directory for dry run
        if self.dry_run:
            self.prompts_dir = output_path / "prompts"
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            print(f"  Dry run mode - prompts will be saved to: {self.prompts_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = self.cdm_domain.replace(' ', '_')
        
        all_entities = []
        source_files = []
        all_standards = []
        ai_reasoning_parts = []
        
        # Process general standards
        if self.ncpdp_general_standards and Path(ncpdp_general_path).exists():
            print(f"  Processing NCPDP General: {self.ncpdp_general_standards}")
            
            with open(ncpdp_general_path, 'r', encoding='utf-8') as f:
                ncpdp_data = json.load(f)
            
            standards_map = ncpdp_data.get('_standards', {})
            source_files.append(Path(ncpdp_general_path).name)
            
            for standard_code in self.ncpdp_general_standards:
                if standard_code not in ncpdp_data:
                    print(f"    Warning: Standard {standard_code} not found in file")
                    continue
                
                raw_fields = ncpdp_data[standard_code]
                entity = self.rationalize_standard(standard_code, raw_fields, standards_map)
                all_entities.append(entity)
                all_standards.append(standard_code)
                
                # Collect AI reasoning
                info = self.standards_info.get(standard_code, {})
                if info.get('reasoning'):
                    ai_reasoning_parts.append(f"{standard_code}: {info['reasoning']}")
        
        # Process SCRIPT standards
        if self.ncpdp_script_standards and Path(ncpdp_script_path).exists():
            print(f"  Processing NCPDP SCRIPT: {self.ncpdp_script_standards}")
            
            with open(ncpdp_script_path, 'r', encoding='utf-8') as f:
                ncpdp_data = json.load(f)
            
            standards_map = ncpdp_data.get('_standards', {})
            source_files.append(Path(ncpdp_script_path).name)
            
            for standard_code in self.ncpdp_script_standards:
                if standard_code not in ncpdp_data:
                    print(f"    Warning: Standard {standard_code} not found in file")
                    continue
                
                raw_fields = ncpdp_data[standard_code]
                entity = self.rationalize_standard(standard_code, raw_fields, standards_map)
                all_entities.append(entity)
                all_standards.append(standard_code)
                
                # Collect AI reasoning
                info = self.standards_info.get(standard_code, {})
                if info.get('reasoning'):
                    ai_reasoning_parts.append(f"{standard_code}: {info['reasoning']}")
        
        if not all_entities:
            print("  No NCPDP entities generated")
            return None
        
        # Build consolidated output in common format
        consolidated = {
            "rationalization_metadata": {
                "source_type": "NCPDP",
                "cdm_domain": self.cdm_domain,
                "cdm_classification": self.cdm_classification,
                "rationalization_timestamp": datetime.now().isoformat(),
                "source_files": source_files,
                "selected_standards": all_standards,
                "entities_processed": len(all_entities)
            },
            "entities": all_entities,
            "reference_data": {
                "value_sets": [],
                "code_systems": []
            }
        }
        
        if ai_reasoning_parts:
            consolidated["rationalization_metadata"]["ai_reasoning"] = " | ".join(ai_reasoning_parts)
        
        output_file = output_path / f"rationalized_ncpdp_{domain_safe}_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated, f, indent=2)
        
        attr_count = sum(len(e.get('attributes', [])) for e in all_entities)
        print(f"  ✓ Saved: {output_file.name}")
        print(f"    Entities: {len(all_entities)}, Attributes: {attr_count}")
        
        return str(output_file)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: python rationalize_ncpdp.py <config_file> <ncpdp_general_file> <ncpdp_script_file> <output_dir>")
        sys.exit(1)
    
    rationalizer = NCPDPRationalizer(sys.argv[1])
    rationalizer.run(sys.argv[2], sys.argv[3], sys.argv[4])