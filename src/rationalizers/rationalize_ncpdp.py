"""
NCPDP Rationalization Module
Transforms NCPDP standards file into unified entity-based rationalized format
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any


class NCPDPRationalizer:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        
        # Handle standards as objects with 'code' field or simple strings
        input_files = self.config.get('input_files', {})
        general_raw = input_files.get('ncpdp_general_standards', [])
        script_raw = input_files.get('ncpdp_script_standards', [])
        
        self.ncpdp_general_standards = [
            s['code'] if isinstance(s, dict) else s for s in general_raw
        ]
        self.ncpdp_script_standards = [
            s['code'] if isinstance(s, dict) else s for s in script_raw
        ]
        
        # Preserve reasoning for metadata
        self.standards_reasoning = {}
        for s in general_raw + script_raw:
            if isinstance(s, dict):
                self.standards_reasoning[s['code']] = s.get('reasoning', '')
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  General standards to process: {self.ncpdp_general_standards}")
        print(f"  SCRIPT standards to process: {self.ncpdp_script_standards}")
        
    def map_ncpdp_type_to_sql(self, field_format: str, field_length: str) -> str:
        """Map NCPDP field format to SQL type"""
        if not field_format:
            return "VARCHAR(255)"
            
        # x(n) = VARCHAR(n)
        if match := re.match(r'x\((\d+)\)', field_format):
            return f"VARCHAR({match.group(1)})"
        
        # 9(n) = INTEGER if length reasonable, otherwise VARCHAR
        if match := re.match(r'9\((\d+)\)', field_format):
            length = int(match.group(1))
            if length <= 10:
                return "INTEGER"
            else:
                return f"VARCHAR({length})"
        
        # s9(n)v99 = DECIMAL
        if match := re.match(r's?9\((\d+)\)v(\d+)', field_format):
            precision = int(match.group(1)) + int(match.group(2))
            scale = int(match.group(2))
            return f"DECIMAL({precision},{scale})"
        
        # an = VARCHAR(255) default
        if field_format == "an":
            return "VARCHAR(255)"
        
        # Default fallback
        return f"VARCHAR({field_length if field_length else '255'})"
    
    def auto_detect_classification(self, field_name: str, field_def: str) -> tuple:
        """Auto-detect PII/PHI classification"""
        text = (field_name + " " + field_def).lower()
        
        # PHI indicators
        phi_keywords = ['diagnosis', 'medical', 'health', 'treatment', 'prescription', 'drug', 'patient']
        # PII indicators
        pii_keywords = ['name', 'address', 'phone', 'email', 'ssn', 'member', 'cardholder', 'person']
        
        is_phi = any(kw in text for kw in phi_keywords)
        is_pii = any(kw in text for kw in pii_keywords) and not is_phi
        
        if is_phi:
            return (True, False, "PHI")
        elif is_pii:
            return (False, True, "PII")
        else:
            return (False, False, "Internal")
    
    def transform_field_to_attribute(self, field: Dict[str, Any], standard: str) -> Dict[str, Any]:
        """Transform NCPDP field to rationalized attribute"""
        field_code = field.get('i', '')
        field_name = field.get('n', '')
        field_def = field.get('d', '')
        field_format = field.get('t', '')
        field_length = field.get('l', '')
        comments = field.get('o', '')
        
        # Map to SQL type
        sql_type = self.map_ncpdp_type_to_sql(field_format, field_length)
        
        # Auto-detect PII/PHI
        is_phi, is_pii, classification = self.auto_detect_classification(field_name, field_def)
        
        # Build attribute
        attr = {
            "attribute_name": field_name.replace(' ', '_').replace('/', '_').lower(),
            "data_type": sql_type,
            "description": field_def,
            "nullable": True,  # NCPDP doesn't specify, default to nullable
            "required": False,
            "business_context": comments if comments else None,
            "data_classification": classification,
            "is_pii": is_pii,
            "is_phi": is_phi,
            "calculated_field": False,
            "field_code": field_code,
            "source_type": field_format,
            "source_length": field_length,
            "standards": [standard]
        }
        
        # Remove None values
        return {k: v for k, v in attr.items() if v is not None}
    
    def rationalize_ncpdp_file(self, ncpdp_file_path: str, selected_standards: List[str], 
                                source_type: str) -> Dict[str, Any]:
        """Rationalize NCPDP file to entity-based format"""
        with open(ncpdp_file_path, 'r', encoding='utf-8') as f:
            ncpdp_data = json.load(f)
        
        # Get standard names mapping
        standards_map = ncpdp_data.get('_standards', {})
        
        entities = []
        
        for standard_code in selected_standards:
            if standard_code not in ncpdp_data:
                print(f"Warning: Standard {standard_code} not found in NCPDP file")
                continue
            
            fields = ncpdp_data[standard_code]
            
            # Create entity for this standard
            entity = {
                "entity_name": standards_map.get(standard_code, f"Standard_{standard_code}"),
                "description": f"NCPDP {standards_map.get(standard_code, standard_code)} fields",
                "ncpdp_standard": standard_code,
                "ncpdp_standard_name": standards_map.get(standard_code, f"Standard {standard_code}"),
                "ai_selection_reasoning": self.standards_reasoning.get(standard_code, ''),
                "attributes": []
            }
            
            # Transform fields to attributes
            for field in fields:
                attr = self.transform_field_to_attribute(field, standard_code)
                entity["attributes"].append(attr)
            
            entities.append(entity)
        
        # Build rationalized output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        rationalized = {
            "rationalization_metadata": {
                "source_type": source_type,
                "cdm_domain": self.cdm_domain,
                "cdm_classification": self.cdm_classification,
                "rationalization_timestamp": datetime.now().isoformat(),
                "rationalization_version": "1.0",
                "source_files": [Path(ncpdp_file_path).name],
                "rationalization_approach": f"Selected standards {selected_standards} for {self.cdm_domain} domain",
                "selected_standards": selected_standards,
                "total_entities": len(entities),
                "total_attributes": sum(len(e["attributes"]) for e in entities)
            },
            "entities": entities
        }
        
        return rationalized, timestamp
    
    def run(self, ncpdp_general_path: str, ncpdp_script_path: str, output_dir: str):
        """Run rationalization for both NCPDP files"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Rationalize general standards
        if self.ncpdp_general_standards:
            print(f"Rationalizing NCPDP General Standards: {self.ncpdp_general_standards}")
            rationalized_general, timestamp = self.rationalize_ncpdp_file(
                ncpdp_general_path, 
                self.ncpdp_general_standards,
                "ncpdp_general"
            )
            
            output_file = output_path / f"rationalized_ncpdp_general_{self.cdm_domain.replace(' ', '_')}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(rationalized_general, f, indent=2)
            print(f"✓ Saved: {output_file}")
            print(f"  Entities: {rationalized_general['rationalization_metadata']['total_entities']}")
            print(f"  Attributes: {rationalized_general['rationalization_metadata']['total_attributes']}")
        
        # Rationalize SCRIPT standards
        if self.ncpdp_script_standards:
            print(f"\nRationalizing NCPDP SCRIPT Standards: {self.ncpdp_script_standards}")
            rationalized_script, timestamp = self.rationalize_ncpdp_file(
                ncpdp_script_path,
                self.ncpdp_script_standards,
                "ncpdp_script"
            )
            
            output_file = output_path / f"rationalized_ncpdp_script_{self.cdm_domain.replace(' ', '_')}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(rationalized_script, f, indent=2)
            print(f"✓ Saved: {output_file}")
            print(f"  Entities: {rationalized_script['rationalization_metadata']['total_entities']}")
            print(f"  Attributes: {rationalized_script['rationalization_metadata']['total_attributes']}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: python rationalize_ncpdp.py <config_file> <ncpdp_general_file> <ncpdp_script_file> <output_dir>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    ncpdp_general_file = sys.argv[2]
    ncpdp_script_file = sys.argv[3]
    output_dir = sys.argv[4]
    
    rationalizer = NCPDPRationalizer(config_file)
    rationalizer.run(ncpdp_general_file, ncpdp_script_file, output_dir)