# src/steps/step1a_fhir.py
"""
Step 1a: FHIR Rationalization

Rationalizes multiple FHIR profile files into unified entities and attributes.
Injects CDM description into prompt for context-aware rationalization.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config import load_config
from src.core.llm_client import LLMClient
from src.converters import convert_fhir_to_json
from src.config.config_parser import AppConfig

def build_prompt(config: AppConfig, fhir_files: list) -> str:
    """Build FHIR rationalization prompt with CDM description"""
    
    # Convert files to JSON
    fhir_json = []
    for fhir_file in fhir_files:
        fhir_json.append({
            'filename': Path(fhir_file).name,
            'content': convert_fhir_to_json(fhir_file)
        })
    
    # Build prompt with CDM context
    prompt = f"""You are a FHIR expert rationalizing multiple FHIR resource profiles for a PBM CDM.

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Analyze the {len(fhir_files)} FHIR profile files provided and rationalize them into a unified set of entities and attributes with FULL business context that aligns with the CDM description above.

## RATIONALIZATION GOALS

1. Identify all unique entities across all FHIR resources that are relevant to this CDM
2. Consolidate duplicate or overlapping attributes
3. Resolve conflicts between different FHIR resources
4. **Preserve ALL business context** (definitions, comments, requirements, constraints)
5. Consider the CDM description when determining relevance and priority
6. Track source files for each rationalized element

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "domain": "{config.cdm.domain}",
  "rationalized_entities": [
    {{
      "entity_name": "Plan",
      "source_resource": "InsurancePlan",
      "source_files": ["insuranceplan.profile.json"],
      "description": "...",
      "business_context": "...",
      "attributes": [
        {{
          "attribute_name": "identifier",
          "source_path": "InsurancePlan.identifier",
          "source_files": ["insuranceplan.profile.json"],
          "data_type": "Identifier",
          "cardinality": "0..*",
          "required": false,
          "short_description": "...",
          "definition": "...",
          "comment": "...",
          "requirements": "...",
          "constraints": [],
          "binding": null,
          "must_support": false
        }}
      ]
    }}
  ]
}}
```

## FIELD MAPPING

Map FHIR elements to rationalized attributes:
1. `element.short` → `short_description`
2. `element.definition` → `definition`
3. `element.comment` → `comment`
4. `element.requirements` → `requirements`
5. `element.constraint` → `constraints` (array)
6. `element.binding` → `binding` (object)
7. `element.mustSupport` → `must_support` (boolean)
8. `element.min` and `element.max` → `cardinality` (e.g., "0..1", "1..1", "0..*")
9. `element.min > 0` → `required` (true/false)

## HANDLING MISSING FIELDS

- If a field doesn't exist in FHIR, set to null (not omit)
- Always include short_description and definition at minimum
- For nested/complex types, create separate entities

## CRITICAL

- Output ONLY valid JSON (no markdown, no code blocks)
- Include ALL textual fields from FHIR
- Rationalize conflicts (don't duplicate), but preserve all context
- Focus on elements relevant to: {config.cdm.description}

---

## FHIR PROFILE FILES

"""
    
    for i, fhir_data in enumerate(fhir_json, 1):
        prompt += f"### FHIR File {i}: {fhir_data['filename']}\n\n```json\n{json.dumps(fhir_data['content'], indent=2)}\n```\n\n"
    
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
    
    output_file = prompts_dir / f"step1a_fhir_{timestamp}.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    return {
        'file': str(output_file),
        'characters': len(prompt),
        'tokens_estimate': len(prompt) // 4
    }


def run_step1a(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool
) -> Optional[dict]:
    """
    Step 1a: Rationalize FHIR files
    
    Args:
        config: Configuration object
        outdir: Output directory
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Rationalized data dict (None in dry run)
    """
    
    # Build prompt
    prompt = build_prompt(config, config.inputs.fhir)
    
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
            "content": "You are a FHIR expert. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
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
        output_file = outdir / f"rationalized_fhir_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rationalized, f, indent=2)
        
        print(f"  ✓ Output: {output_file}")
        print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        return rationalized
        
    except json.JSONDecodeError as e:
        print(f"  ERROR: Failed to parse LLM response: {e}")
        print(f"  Response preview: {response[:500]}...")
        raise