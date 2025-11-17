# src/steps/step1c_glue.py
"""
Step 1c: Glue Schema Rationalization

Rationalizes AWS Glue table definitions into unified entities and attributes.
Injects CDM description into prompt for context-aware rationalization.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config import load_config
from src.core.llm_client import LLMClient
from src.converters import convert_glue_to_json
from src.config.config_parser import AppConfig

def build_prompt(config: AppConfig, glue_files: list) -> str:
    """Build Glue rationalization prompt with CDM description"""
    
    # Convert files to JSON
    glue_json = []
    for glue_file in glue_files:
        glue_json.append({
            'filename': Path(glue_file).name,
            'content': convert_glue_to_json(glue_file)
        })
    
    # Build prompt with CDM context
    prompt = f"""You are a data architect rationalizing AWS Glue table definitions for a PBM CDM.

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Analyze the {len(glue_files)} AWS Glue schema files and rationalize them into a unified set of technical entities and attributes that aligns with the CDM description above.

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
  "domain": "{config.cdm.domain}",
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
- Focus on elements relevant to: {config.cdm.description}

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


def save_prompt(prompt: str, outdir: Path) -> dict:
    """Save prompt to file and return stats"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prompts_dir = outdir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = prompts_dir / f"step1c_glue_{timestamp}.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    return {
        'file': str(output_file),
        'characters': len(prompt),
        'tokens_estimate': len(prompt) // 4
    }


def run_step1c(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool
) -> Optional[dict]:
    """
    Step 1c: Rationalize AWS Glue schema files
    
    Args:
        config: Configuration object
        outdir: Output directory
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Rationalized data dict (None in dry run)
    """
    
    # Build prompt
    prompt = build_prompt(config, config.inputs.glue)
    
    # Dry run - save prompt and exit
    if dry_run:
        stats = save_prompt(prompt, outdir)
        print(f"  ✓ Prompt saved: {stats['file']}")
        print(f"    Characters: {stats['characters']:,}")
        print(f"    Tokens (est): {stats['tokens_estimate']:,}")
        return None
    
    # Live mode - call LLM
    print(f"  Calling LLM ({llm.model})...")
    
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
    
    response, token_usage = llm.chat(messages)
    
    # Parse response
    try:
        # Strip markdown if present
        response_clean = response.strip()
        if response_clean.startswith("```"):
            lines = response_clean.split("\n")
            response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
        
        rationalized = json.loads(response_clean)
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.replace(' ', '_')
        output_file = outdir / f"rationalized_glue_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rationalized, f, indent=2)
        
        print(f"  ✓ Output: {output_file}")
        print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        return rationalized
        
    except json.JSONDecodeError as e:
        print(f"  ERROR: Failed to parse LLM response: {e}")
        print(f"  Response preview: {response[:500]}...")
        raise