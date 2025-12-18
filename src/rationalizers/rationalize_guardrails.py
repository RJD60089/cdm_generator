"""
Guardrails Rationalization Module
Rationalizes Guardrails API specification files into unified entities and attributes.

Version 2.0:
- Iterative file-by-file processing with in-memory accumulation
- Incremental rationalization (AI merges new file against prior state)
- Improved prompt with scope filtering and lineage tracking
- UTF-8 encoding throughout
- Config uses filename-only, resolved via config_utils
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import converter - assumes src.converters module exists
from src.converters import convert_guardrails_to_json
from src.config import config_utils


class GuardrailsRationalizer:
    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        self.cdm_description = self.config.get('cdm', {}).get('description', '')
        
        # Get guardrails files from config (filename-only format)
        input_files = self.config.get('input_files', {})
        self.guardrails_files = input_files.get('guardrails', [])
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  Guardrails files: {len(self.guardrails_files)}")
    
    def build_prompt(self, gr_file: str, prior_state: Optional[Dict] = None) -> str:
        """
        Build Guardrails rationalization prompt for a single file.
        
        Args:
            gr_file: Path to guardrails file to process
            prior_state: Previously rationalized state (None for first file)
            
        Returns:
            Complete prompt string
        """
        # Convert file to JSON
        content = convert_guardrails_to_json(gr_file)
        filename = Path(gr_file).name
        tabs = list(content.get('sheets', {}).keys())
        
        print(f"\n  File: {filename}")
        for tab in tabs:
            print(f"    - {tab}")
        
        # Build prompt with CDM context
        prompt = f"""You are a business analyst engaged in developing a CDM for a PBM organization.

## CDM CONTEXT

**CDM Domain:** {self.cdm_domain}

**CDM Description:** {self.cdm_description}

## SCOPE FILTERING

Use the CDM Description's Includes/Excludes to determine relevance:
- INCLUDE: Entities/attributes that directly define what's listed in "Includes:"
- EXCLUDE: Entities/attributes that belong to domains listed in "Excludes:"
- When uncertain, check if the element's PRIMARY PURPOSE aligns with this CDM

## YOUR TASK

Analyze the provided Guardrail file which is a collection of API interface definitions and data structures. These files will have a varying number of data elements relevant to defining the CDM for given domain. Each and every element needs to be reviewed and a determination made if it is of business value to include in the CDM. It is possible that the entities and data elements represented in the files may be duplicated or have similar but not exact names. When an entity or data element is identified for inclusion into the CDM, it needs to be appropriately integrated into the rationalized output. The guardrail files represent existing uses of data in the organization and it is very important that the lineage from the guardrail file to the rationalized output is defined and retained, along with all metadata and governance in the output.

## RATIONALIZATION GOALS

1. Identify all unique business entities across all API specifications and data structures relevant to this CDM
2. Consolidate duplicate or overlapping attributes across different APIs
3. Resolve conflicts between API versions and specifications
4. **Important to preserve the lineage from the data entity or element from the guardrail file to the output**
5. **Preserve business rules, validation requirements, AND data governance metadata**
6. **Capture calculated field information and API request/response context**
7. Track source files AND tabs (file::tab format) for each rationalized element
8. Consider the CDM description when determining relevance and priority
"""

        # Add incremental rationalization instructions if prior state exists
        if prior_state:
            prompt += """
## INCREMENTAL RATIONALIZATION

A previously rationalized output state is included below. You must:
1. Treat it as the current rationalized output state
2. Analyze the NEW guardrails file against this state
3. For matching entities: merge attributes, append to source_files lists
4. For new entities: add to rationalized_entities
5. For duplicate attributes: consolidate, preserve all source lineage
6. For conflicts: prefer the more complete/specific definition, note in business_context
7. Return the COMPLETE updated rationalized output (not just changes)
"""

        prompt += f"""
## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "domain": "{self.cdm_domain}",
  "rationalized_entities": [
    {{
      "entity_name": "[SELECTED ENTITY]",
      "source_api": "[API NAME AND VERSION]",
      "source_files": ["[FILE.xlsx]::[TAB NAME 1]", "[FILE.xlsx]::[TAB NAME 2]"],
      "description": "...",
      "business_context": "...",
      "attributes": [
        {{
          "attribute_name": "[SELECTED ATTRIBUTE]",
          "source_files_element": ["[FILE.xlsx]::[TAB NAME]::[ORIGINAL ELEMENT NAME]"],
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

**Note:** This prompt is CDM-agnostic. Interpret `[SELECTED ENTITY]` and `[SELECTED ATTRIBUTE]` as appropriate for the current domain context provided above.

## CRITICAL - MUST DO ALL OF THE FOLLOWING FOR EVERY SELECTED DATA ENTITY OR DATA ELEMENT

- Output ONLY valid JSON (no markdown, no code blocks)
- `attribute_name` = your rationalized/cleaned name for the CDM
- `source_files_element` = EXACT original field name from source (e.g., "File.xlsx::Tab::Dollars" not "File.xlsx::Tab::copay_amount")
- Use file::tab::element format for complete source lineage tracking
- Track calculated fields with dependencies
- Preserve data governance (PII, PHI, classification)
- Focus on elements relevant to: {self.cdm_description}

---
"""

        # Add prior state if incremental
        if prior_state:
            prompt += f"""
## PREVIOUSLY RATIONALIZED OUTPUT STATE

```json
{json.dumps(prior_state, indent=2)}
```

---
"""

        # Add current guardrails file
        prompt += f"""
## GUARDRAILS FILE TO PROCESS

### {filename}

```json
{json.dumps(content, indent=2)}
```

---

Generate the rationalized JSON now."""
        
        return prompt
    
    def save_prompt(self, prompt: str, output_dir: Path, file_index: int) -> dict:
        """Save prompt to file and return stats."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = output_dir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        domain_safe = self.cdm_domain.replace(' ', '_')
        prompt_file = prompts_dir / f"guardrails_prompt_{domain_safe}_{file_index}_{timestamp}.txt"
        
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        # Estimate tokens (rough: 4 chars per token)
        char_count = len(prompt)
        token_estimate = char_count // 4
        
        return {
            "file": str(prompt_file),
            "characters": char_count,
            "tokens_estimate": token_estimate
        }
    
    def _call_llm(self, prompt: str) -> Dict:
        """Call LLM and parse JSON response."""
        if self.llm is None:
            raise ValueError("LLM client not configured")
        
        messages = [{"role": "user", "content": prompt}]
        response_text, _ = self.llm.chat(messages)
        
        # Clean response - remove markdown code blocks if present
        text = response_text.strip()
        if text.startswith("```"):
            lines = text.split("```")
            if len(lines) >= 2:
                text = lines[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
        
        return json.loads(text)
    
    def _transform_to_common_format(self, raw_output: Dict) -> List[Dict]:
        """Transform raw AI output to common entity format."""
        entities = []
        
        for raw_entity in raw_output.get('rationalized_entities', []):
            attributes = []
            
            for raw_attr in raw_entity.get('attributes', []):
                attr: Dict[str, Any] = {
                    "attribute_name": raw_attr.get('attribute_name', ''),
                    "description": raw_attr.get('description', ''),
                    "data_type": raw_attr.get('data_type', 'string'),
                    "source_attribute": raw_attr.get('source_files_element', []),  # Preserve lineage
                    "source_files": raw_attr.get('source_files_element', []),  # For compatibility
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
            
            entity: Dict[str, Any] = {
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
        
        return entities
    
    def run(self, output_dir: str) -> Optional[str]:
        """
        Run Guardrails rationalization with iterative file processing.
        
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
        
        # Initialize rationalized state
        rationalized_state: Optional[Dict] = None
        total_files = len(self.guardrails_files)
        
        print(f"\n  Processing {total_files} guardrails file(s) iteratively...")
        
        for idx, gr_filename in enumerate(self.guardrails_files, 1):
            # Resolve filename to full path
            gr_file = config_utils.resolve_guardrail_file(self.cdm_domain, gr_filename)
            
            print(f"\n  [{idx}/{total_files}] Processing: {gr_filename}")
            
            # Verify file exists
            if not gr_file.exists():
                print(f"    ⚠️  File not found: {gr_file}")
                continue
            
            # Build prompt (with prior state for files 2+)
            prompt = self.build_prompt(str(gr_file), prior_state=rationalized_state)
            
            # Dry run - save prompts and continue
            if self.dry_run:
                self.prompts_dir = output_path / "prompts"
                self.prompts_dir.mkdir(parents=True, exist_ok=True)
                
                stats = self.save_prompt(prompt, output_path, idx)
                print(f"    ✓ Prompt saved: {Path(stats['file']).name}")
                print(f"      Characters: {stats['characters']:,}")
                print(f"      Tokens (est): {stats['tokens_estimate']:,}")
                
                # For dry run, create mock state to test incremental prompts
                if rationalized_state is None:
                    rationalized_state = {"domain": self.cdm_domain, "rationalized_entities": []}
                continue
            
            # Live mode - call LLM
            print(f"    Calling LLM...")
            
            try:
                rationalized_state = self._call_llm(prompt)
                
                entity_count = len(rationalized_state.get('rationalized_entities', []))
                attr_count = sum(
                    len(e.get('attributes', [])) 
                    for e in rationalized_state.get('rationalized_entities', [])
                )
                print(f"    ✓ Rationalized: {entity_count} entities, {attr_count} attributes")
                
            except json.JSONDecodeError as e:
                print(f"    ERROR: Failed to parse LLM response for {gr_filename}: {e}")
                raise
        
        # Dry run complete
        if self.dry_run:
            print(f"\n  ✓ Dry run complete. {total_files} prompts saved.")
            return None
        
        # Transform final state to common format and save
        if rationalized_state is None:
            print("  ERROR: No rationalized state produced")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = self.cdm_domain.replace(' ', '_')
        
        entities = self._transform_to_common_format(rationalized_state)
        total_attrs = sum(len(e.get('attributes', [])) for e in entities)
        
        output: Dict[str, Any] = {
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
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n  ✓ Saved: {output_file.name}")
        print(f"    Entities: {len(entities)}")
        print(f"    Attributes: {total_attrs}")
        
        return str(output_file)


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

def run_guardrails_rationalization(config: Any, outdir: str, llm: Optional[Any] = None, 
                                   dry_run: bool = False, config_path: Optional[str] = None) -> Optional[str]:
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