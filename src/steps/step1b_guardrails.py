# src/steps/step1b_guardrails.py
"""
Step 1b: Guardrails Rationalization

Rationalizes Guardrails API specification files into unified entities and attributes.
Injects CDM description into prompt for context-aware rationalization.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config import load_config
from src.core.llm_client import LLMClient
from src.converters import convert_guardrails_to_json
from src.config.config_parser import AppConfig

def build_prompt(config: AppConfig, gr_files: list) -> str:
    """Build Guardrails rationalization prompt with CDM description"""
    
    # Convert files to JSON
    gr_json = []
    for gr_file in gr_files:
        gr_json.append({
            'filename': Path(gr_file).name,
            'content': convert_guardrails_to_json(gr_file)
        })
    
    # Build prompt with CDM context
    prompt = f"""You are a business analyst rationalizing multiple API specifications for a PBM CDM.

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Analyze the {len(gr_files)} Guardrails specification files and rationalize them into a unified set of business entities and attributes with complete data governance that aligns with the CDM description above.

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
  "domain": "{config.cdm.domain}",
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
- Focus on elements relevant to: {config.cdm.description}

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


def save_prompt(prompt: str, outdir: Path) -> dict:
    """Save prompt to file and return stats"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prompts_dir = outdir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = prompts_dir / f"step1b_guardrails_{timestamp}.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    return {
        'file': str(output_file),
        'characters': len(prompt),
        'tokens_estimate': len(prompt) // 4
    }


def run_step1b(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool
) -> Optional[dict]:
    """
    Step 1b: Rationalize Guardrails files
    
    Args:
        config: Configuration object
        outdir: Output directory
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Rationalized data dict (None in dry run)
    """
    
    # Build prompt
    prompt = build_prompt(config, config.inputs.guardrails)
    
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
            "content": "You are a business analyst expert. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
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
        output_file = outdir / f"rationalized_guardrails_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rationalized, f, indent=2)
        
        print(f"  ✓ Output: {output_file}")
        print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        return rationalized
        
    except json.JSONDecodeError as e:
        print(f"  ERROR: Failed to parse LLM response: {e}")
        print(f"  Response preview: {response[:500]}...")
        raise