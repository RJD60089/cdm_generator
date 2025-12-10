"""
Guardrails Rationalization Module
Rationalizes Guardrails API specification files into unified entities and attributes.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import converter - assumes src.converters module exists
from src.converters import convert_guardrails_to_json


class GuardrailsRationalizer:
    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        self.cdm_description = self.config.get('cdm', {}).get('description', '')
        
        # Get guardrails files from config
        input_files = self.config.get('input_files', {})
        self.guardrails_files = input_files.get('guardrails', [])
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  Guardrails files: {len(self.guardrails_files)}")
    
    def build_prompt(self) -> str:
        """Build Guardrails rationalization prompt with CDM description"""
        
        # Convert files to JSON and print tab info
        gr_json = []
        print(f"\n  Processing {len(self.guardrails_files)} Guardrails file(s):")
        for gr_file in self.guardrails_files:
            content = convert_guardrails_to_json(gr_file)
            gr_json.append({
                'filename': Path(gr_file).name,
                'content': content
            })
            # Print file and tab info
            tabs = list(content.get('sheets', {}).keys())
            print(f"\n  File: {Path(gr_file).name}")
            for tab in tabs:
                print(f"    • {tab}")
        
        print(f"\n  Total tabs: {sum(len(g['content'].get('sheets', {})) for g in gr_json)}")
        
        # Build prompt with CDM context
        prompt = f"""You are a business analyst rationalizing multiple API specifications for a PBM CDM.

## CDM CONTEXT

**Domain:** {self.cdm_domain}

**Description:** {self.cdm_description}

## YOUR TASK

Analyze the {len(self.guardrails_files)} Guardrails specification files and rationalize them into a unified set of business entities and attributes with complete data governance that aligns with the CDM description above.

## RATIONALIZATION GOALS

1. Identify all unique business entities across all API specifications relevant to this CDM
2. Consolidate duplicate or overlapping attributes across different APIs
3. Resolve conflicts between API versions and specifications
4. **Preserve business rules, validation requirements, AND data governance metadata**
5. **Capture calculated field information and API request/response context**
6. Track source files AND tabs (file::tab format) for each rationalized element
7. Consider the CDM description when determining relevance and priority

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "domain": "{self.cdm_domain}",
  "rationalized_entities": [
    {{
      "entity_name": "Plan",
      "source_api": "Hierarchy API v1.5",
      "source_files": ["GR_File.xlsx::Hierarchy", "GR_File.xlsx::Plan Setup"],
      "description": "...",
      "business_context": "...",
      "attributes": [
        {{
          "attribute_name": "plan_id",
          "source_files": ["GR_File.xlsx::Hierarchy"],
          "data_type": "string",
          "required": true,
          "allow_null": false,
          "description": "...",
          "business_context": "...",
          "is_calculated": false,
          "calculation_dependency": null,
          "data_classification": "Internal",
          "is_pii": false,
          "is_phi": false,
          "validation_rules": ["Required", "Must be unique"],
          "business_rules": []
        }}
      ]
    }}
  ]
}}
```

## CRITICAL

- Output ONLY valid JSON (no markdown, no code blocks)
- Use file::tab format for source tracking (e.g., "File.xlsx::Tab Name")
- Track calculated fields with dependencies
- Preserve data governance (PII, PHI, classification)
- Focus on elements relevant to: {self.cdm_description}

---

## GUARDRAILS SPECIFICATION FILES

"""
        
        for i, gr_data in enumerate(gr_json, 1):
            prompt += f"### Guardrails File {i}: {gr_data['filename']}\n\n```json\n{json.dumps(gr_data['content'], indent=2)}\n```\n\n"
        
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
        
        output_file = prompts_dir / f"guardrails_rationalization_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        return {
            'file': str(output_file),
            'characters': len(prompt),
            'tokens_estimate': len(prompt) // 4
        }
    
    def run(self, output_dir: str) -> Optional[str]:
        """
        Run Guardrails rationalization.
        
        Args:
            output_dir: Directory to save output files
            
        Returns:
            Path to output file (None in dry run)
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if not self.guardrails_files:
            print("  No guardrails files configured, skipping")
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
                "content": "You are a business analyst expert. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
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
                        "source_attribute": raw_attr.get('attribute_name', ''),
                        "source_files": raw_attr.get('source_files', []),
                        "required": raw_attr.get('required', False),
                        "nullable": raw_attr.get('allow_null', True),
                        "cardinality": {"min": 1 if raw_attr.get('required', False) else 0, "max": "1"},
                        "length": None,
                        "precision": None,
                        "scale": None,
                        "default_value": None,
                        "is_array": False,
                        "is_nested": False,
                        "is_pii": raw_attr.get('is_pii', False),
                        "is_phi": raw_attr.get('is_phi', False),
                        "data_classification": raw_attr.get('data_classification'),
                        "business_context": raw_attr.get('business_context'),
                        "business_rules": raw_attr.get('business_rules'),
                        "validation_rules": raw_attr.get('validation_rules'),
                        "is_calculated": raw_attr.get('is_calculated', False),
                        "calculation_dependency": raw_attr.get('calculation_dependency'),
                        "source_metadata": {}
                    }
                    attributes.append(attr)
                
                # Transform entity to common format
                entity = {
                    "entity_name": raw_entity.get('entity_name', ''),
                    "description": raw_entity.get('description', ''),
                    "source_type": "Guardrails",
                    "source_info": {
                        "files": raw_entity.get('source_files', []),
                        "api": raw_entity.get('source_api'),
                        "schema": None,
                        "table": None,
                        "url": None,
                        "version": None
                    },
                    "business_context": raw_entity.get('business_context'),
                    "technical_context": None,
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
                    "source_type": "Guardrails",
                    "cdm_domain": self.cdm_domain,
                    "cdm_classification": self.cdm_classification,
                    "rationalization_timestamp": datetime.now().isoformat(),
                    "files_processed": len(self.guardrails_files),
                    "entities_processed": len(entities),
                    "attributes_processed": total_attrs
                },
                "entities": entities,
                "reference_data": {
                    "value_sets": [],
                    "code_systems": []
                }
            }
            
            output_file = output_path / f"rationalized_guardrails_{domain_safe}_{timestamp}.json"
            
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
        print("Usage: python rationalize_guardrails.py <config_file> <output_dir>")
        sys.exit(1)
    
    rationalizer = GuardrailsRationalizer(sys.argv[1])
    rationalizer.run(sys.argv[2])


# =============================================================================
# ORCHESTRATOR WRAPPER
# =============================================================================

def run_guardrails_rationalization(config, outdir, llm=None, dry_run=False, config_path=None):
    """
    Wrapper function for orchestrator compatibility.
    
    Args:
        config: AppConfig instance (unused, for interface consistency)
        outdir: Output directory path
        llm: LLM client instance
        dry_run: If True, save prompts only
        config_path: Path to config JSON file (required)
    
    Returns:
        Path to output file, or None if dry run
    """
    if not config_path:
        raise ValueError("config_path is required for Guardrails rationalization")
    
    rationalizer = GuardrailsRationalizer(config_path, llm=llm, dry_run=dry_run)
    return rationalizer.run(str(outdir))