"""
Glue Rationalization Module
Rationalizes AWS Glue table definitions into unified entities and attributes.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import converter
from src.converters import convert_glue_to_json


class GlueRationalizer:
    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        self.cdm_description = self.config.get('cdm', {}).get('description', '')
        
        # Get glue files from config
        input_files = self.config.get('input_files', {})
        self.glue_files = input_files.get('glue', [])
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  Glue files: {len(self.glue_files)}")
    
    def build_prompt(self) -> str:
        """Build Glue rationalization prompt with CDM description"""
        
        # Convert files to JSON
        glue_json = []
        for glue_file in self.glue_files:
            content = convert_glue_to_json(glue_file)
            # Parse the JSON string returned by converter
            content_dict = json.loads(content)
            glue_json.append({
                'filename': Path(glue_file).name,
                'content': content_dict
            })
            # Print file info
            columns = content_dict.get('Columns', [])
            print(f"\n  File: {Path(glue_file).name}")
            print(f"    Columns: {len(columns)}")
        
        # Build prompt with CDM context
        prompt = f"""You are a data architect rationalizing AWS Glue table definitions for a PBM CDM.

## CDM CONTEXT

**Domain:** {self.cdm_domain}

**Description:** {self.cdm_description}

## YOUR TASK

Analyze the {len(self.glue_files)} AWS Glue schema files and rationalize them into a unified set of technical entities and attributes that aligns with the CDM description above.

## RATIONALIZATION GOALS

1. Identify all unique tables/entities across all Glue catalogs relevant to this CDM
2. Consolidate duplicate or overlapping columns/attributes
3. Resolve conflicts between different schema versions
4. **Preserve technical metadata (data types, array structures, nested fields)**
5. **Capture actual data types, sizes, and Glue-specific details**
6. Track source schemas and tables for each rationalized element
7. Consider the CDM description when determining relevance and priority

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "domain": "{self.cdm_domain}",
  "rationalized_entities": [
    {{
      "entity_name": "Plan",
      "source_schema": "source_navitus_bpm_plan_event",
      "source_table": "plan_event",
      "source_files": ["plan_bpm_tables.json"],
      "description": "...",
      "technical_context": "...",
      "attributes": [
        {{
          "attribute_name": "detail_planid",
          "source_column": "detail_planid",
          "source_files": ["plan_bpm_tables.json"],
          "data_type": "int",
          "max_length": null,
          "precision": null,
          "scale": null,
          "required": true,
          "nullable": false,
          "default_value": null,
          "description": "...",
          "is_array": false,
          "is_nested": false
        }}
      ]
    }}
  ]
}}
```

## DATA TYPE MAPPING

Preserve exact Glue data types:
- string, int, bigint, decimal, float, double
- date, timestamp
- boolean
- array<type> - mark is_array: true
- struct - mark is_nested: true
- Include max_length for strings where applicable

## ARRAY AND NESTED HANDLING

For array fields like `detail_bicplans[0]_id`:
- Recognize as array element
- Set is_array: true
- Track base attribute name
- Note array structure in description

For nested/struct fields:
- Set is_nested: true
- Document structure in description

## CRITICAL

- Output ONLY valid JSON (no markdown, no code blocks)
- Preserve exact Glue data types and structures
- Track source schema and table names
- Handle arrays and nested structures appropriately
- Focus on elements relevant to: {self.cdm_description}

---

## AWS GLUE SCHEMA FILES

"""
        
        for i, glue_data in enumerate(glue_json, 1):
            prompt += f"### Glue Schema File {i}: {glue_data['filename']}\n\n```json\n{json.dumps(glue_data['content'], indent=2)}\n```\n\n"
        
        prompt += """
---

Generate the rationalized JSON now.
"""
        
        return prompt
    
    def save_prompt(self, prompt: str, output_dir: Path) -> dict:
        """Save prompt to file and return stats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = output_dir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"glue_rationalization_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        return {
            'file': str(output_file),
            'characters': len(prompt),
            'tokens_estimate': len(prompt) // 4
        }
    
    def run(self, output_dir: str) -> Optional[str]:
        """
        Run Glue rationalization.
        
        Args:
            output_dir: Directory to save output files
            
        Returns:
            Path to output file (None in dry run)
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if not self.glue_files:
            print("  No glue files configured, skipping")
            return None
        
        # Build prompt
        prompt = self.build_prompt()
        
        # Dry run - save prompt and exit
        if self.dry_run:
            self.prompts_dir = output_path / "prompts"
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            
            stats = self.save_prompt(prompt, output_path)
            print(f"  ✓ Prompt saved: {stats['file']}")
            print(f"    Characters: {stats['characters']:,}")
            print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            return None
        
        # Live mode - call LLM
        print(f"  Calling LLM...")
        
        messages = [
            {
                "role": "system",
                "content": "You are a data architect expert. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        response, token_usage = self.llm.chat(messages)
        
        # Parse response
        try:
            # Strip markdown if present
            response_clean = response.strip()
            if response_clean.startswith("```"):
                lines = response_clean.split("\n")
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
            
            rationalized_data = json.loads(response_clean)
            
            # Transform AI output to common format
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            domain_safe = self.cdm_domain.replace(' ', '_')
            
            # Transform entities to common format
            raw_entities = rationalized_data.get('rationalized_entities', [])
            entities = []
            
            for raw_entity in raw_entities:
                # Transform attributes to common format
                raw_attrs = raw_entity.get('attributes', [])
                attributes = []
                
                for raw_attr in raw_attrs:
                    attr = {
                        "attribute_name": raw_attr.get('attribute_name', ''),
                        "description": raw_attr.get('description', ''),
                        "data_type": raw_attr.get('data_type', 'string'),
                        "source_attribute": raw_attr.get('source_column', raw_attr.get('attribute_name', '')),
                        "source_files": raw_attr.get('source_files', []),
                        "required": raw_attr.get('required', False),
                        "nullable": raw_attr.get('nullable', True),
                        "cardinality": {"min": 1 if raw_attr.get('required', False) else 0, "max": "1"},
                        "length": raw_attr.get('max_length'),
                        "precision": raw_attr.get('precision'),
                        "scale": raw_attr.get('scale'),
                        "default_value": raw_attr.get('default_value'),
                        "is_array": raw_attr.get('is_array', False),
                        "is_nested": raw_attr.get('is_nested', False),
                        "is_pii": False,
                        "is_phi": False,
                        "data_classification": None,
                        "business_context": None,
                        "business_rules": None,
                        "validation_rules": None,
                        "is_calculated": False,
                        "calculation_dependency": None,
                        "source_metadata": {}
                    }
                    attributes.append(attr)
                
                # Transform entity to common format
                entity = {
                    "entity_name": raw_entity.get('entity_name', ''),
                    "description": raw_entity.get('description', ''),
                    "source_type": "Glue",
                    "source_info": {
                        "files": raw_entity.get('source_files', []),
                        "api": None,
                        "schema": raw_entity.get('source_schema'),
                        "table": raw_entity.get('source_table'),
                        "url": None,
                        "version": None
                    },
                    "business_context": None,
                    "technical_context": raw_entity.get('technical_context'),
                    "ai_metadata": {
                        "selection_reasoning": None,
                        "pruning_notes": None
                    },
                    "attributes": attributes,
                    "source_metadata": {}
                }
                entities.append(entity)
            
            total_attrs = sum(len(e.get('attributes', [])) for e in entities)
            
            output = {
                "rationalization_metadata": {
                    "source_type": "Glue",
                    "cdm_domain": self.cdm_domain,
                    "cdm_classification": self.cdm_classification,
                    "rationalization_timestamp": datetime.now().isoformat(),
                    "files_processed": len(self.glue_files),
                    "entities_processed": len(entities),
                    "attributes_processed": total_attrs
                },
                "entities": entities,
                "reference_data": {
                    "value_sets": [],
                    "code_systems": []
                }
            }
            
            output_file = output_path / f"rationalized_glue_{domain_safe}_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            print(f"  ✓ Saved: {output_file.name}")
            print(f"    Entities: {len(entities)}")
            print(f"    Attributes: {total_attrs}")
            
            return str(output_file)
            
        except json.JSONDecodeError as e:
            print(f"  ERROR: Failed to parse LLM response: {e}")
            print(f"  Response preview: {response[:500]}...")
            raise


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python rationalize_glue.py <config_file> <output_dir>")
        sys.exit(1)
    
    rationalizer = GlueRationalizer(sys.argv[1])
    rationalizer.run(sys.argv[2])