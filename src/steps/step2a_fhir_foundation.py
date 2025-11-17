# src/steps/step2a_fhir_foundation.py
"""
Step 2a: FHIR Foundation CDM Generation

Transforms rationalized FHIR entities into canonical CDM structure with:
- Logical data types (not FHIR types)
- Flattened complex structures
- Business context from CDM description
- Scaffolding for Steps 2b-2e (NCPDP, Guardrails, Glue mappings)

Input: Rationalized FHIR JSON from Step 1a
Output: Foundation CDM JSON ready for Step 2b enhancement
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_prompt(config: AppConfig, rationalized_fhir: dict) -> str:
    """Build Step 2a prompt to transform FHIR into canonical CDM"""
    
    prompt = f"""You are a data architect creating a canonical data model (CDM) for a PBM.

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Transform the rationalized FHIR entities into a **canonical CDM** with logical data types and PBM-specific business context.

This is **Step 2a: FHIR Foundation** - you are creating the standards-based foundation that will be enhanced in subsequent steps with NCPDP, Guardrails, and Glue mappings.

---

## INPUT: RATIONALIZED FHIR

{json.dumps(rationalized_fhir, indent=2)}

---

## TRANSFORMATION RULES

### 1. PRESERVE ENTITIES AND ATTRIBUTES
- Keep all entities from rationalized FHIR
- Keep all attributes (but transform types)
- Preserve business context and definitions

### 2. TRANSFORM FHIR TYPES TO LOGICAL TYPES

**Complex Type Expansions:**
- `Identifier` → 
  - `<field>_value`: VARCHAR(100)
  - `<field>_system`: VARCHAR(200)
  
- `Period` →
  - `<field>_start_date`: DATE
  - `<field>_end_date`: DATE
  
- `CodeableConcept` →
  - `<field>_code`: VARCHAR(50)
  - `<field>_display`: VARCHAR(200)
  - `<field>_system`: VARCHAR(200)
  
- `Reference(EntityName)` →
  - `<entity_name_singular>_id`: VARCHAR(50)
  
- `Money` →
  - `<field>_amount`: DECIMAL(18,4)
  - `<field>_currency`: VARCHAR(3)

**Simple Type Mappings:**
- `string` → VARCHAR (estimate appropriate size: 50-500)
- `integer` → INTEGER
- `decimal` → DECIMAL(18,4)
- `boolean` → BOOLEAN
- `date` → DATE
- `dateTime` → DATETIME
- `code` → VARCHAR(50)

### 3. NAMING CONVENTIONS

**Canonical Column Names:**
- Use snake_case: `plan_identifier`, `effective_date`
- Descriptive, not abbreviated: `identifier` not `id`
- Consistent patterns: all dates end in `_date`, all codes end in `_code`

**Source Column Names:**
- UPPERCASE SNAKE_CASE: `PLAN_IDENTIFIER`, `EFFECTIVE_DATE`
- Match canonical but uppercase

### 4. BUSINESS CONTEXT

For each entity and attribute, include:
- **glossary_term**: Clear business definition (2-3 sentences)
- **business_context**: How it's used in PBM passthrough operations
- **classification**: PII, PHI, Operational, or Reference

### 5. ENTITY CLASSIFICATION

Label each entity:
- **Core**: Primary business entities (Plan, Coverage, Member)
- **Reference**: Lookup tables, code sets
- **Transaction**: Event/fact data

---

## OUTPUT FORMAT

Return ONLY valid JSON in this exact structure:

```json
{{
  "cdm_metadata": {{
    "domain": "{config.cdm.domain}",
    "version": "1.0",
    "description": "{config.cdm.description}",
    "foundation_standard": "FHIR",
    "generation_timestamp": "{datetime.now().isoformat()}",
    "generation_steps_completed": ["2a"]
  }},
  
  "entities": [
    {{
      "entity_name": "InsurancePlan",
      "classification": "Core",
      "business_definition": "Insurance plan product offering...",
      "business_context": "In PBM passthrough model, represents the benefit package with transparent pricing...",
      "key_business_questions": [
        "What plans are available?",
        "What are the plan identifiers?",
        "What pricing structures apply?"
      ],
      "fhir_source_entity": "InsurancePlan",
      
      "attributes": [
        {{
          "canonical_column": "plan_identifier",
          "source_column": "PLAN_IDENTIFIER",
          "data_type": "VARCHAR",
          "size": 50,
          "nullable": false,
          "glossary_term": "Business identifier for the insurance plan. Primary business key used across systems.",
          "business_context": "Used in adjudication for plan lookup. Must be unique within client scope.",
          "classification": "Operational",
          
          "source_mappings": {{
            "fhir": {{
              "path": "InsurancePlan.identifier.value",
              "fhir_type": "Identifier",
              "source_files": ["insuranceplan.profile.json"]
            }},
            "ncpdp": null,
            "guardrails": null,
            "glue": null
          }}
        }}
      ]
    }}
  ],
  
  "business_capabilities": []
}}
```

---

## CRITICAL REQUIREMENTS

1. **Preserve all entities** from rationalized FHIR
2. **Transform ALL complex FHIR types** to logical types (flatten Identifier, Period, CodeableConcept, Reference)
3. **Estimate appropriate VARCHAR sizes** based on field purpose:
   - Codes: 10-50
   - Names: 100-200
   - Descriptions: 200-500
   - URLs/Systems: 200-500
4. **Add PBM business context** to every entity and attribute
5. **Create scaffolding** - set ncpdp/guardrails/glue to null (filled in Steps 2b-2d)
6. **Output ONLY valid JSON** - no markdown, no code blocks, no commentary

---

Generate the canonical CDM JSON now.
"""
    
    return prompt


def run_step2a(
    config: AppConfig,
    rationalized_fhir_file: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[dict]:
    """
    Step 2a: Transform rationalized FHIR into canonical CDM foundation
    
    Args:
        config: Configuration object with CDM description
        rationalized_fhir_file: Path to rationalized FHIR JSON from Step 1a
        outdir: Output directory for CDM JSON
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Foundation CDM dict (None in dry run)
    """
    
    print(f"  📖 Loading rationalized FHIR from: {rationalized_fhir_file}")
    
    # Load rationalized FHIR
    with open(rationalized_fhir_file, 'r', encoding='utf-8') as f:
        rationalized_fhir = json.load(f)
    
    entity_count = len(rationalized_fhir.get('rationalized_entities', []))
    print(f"  📊 Found {entity_count} rationalized entities")
    
    # Build prompt
    prompt = build_prompt(config, rationalized_fhir)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"step2a_fhir_foundation_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"  ✓ Prompt saved: {output_file}")
        print(f"    Characters: {len(prompt):,}")
        print(f"    Tokens (est): {len(prompt) // 4:,}")
        return None
    
    # Live mode - call LLM
    print(f"  🤖 Calling LLM to generate foundation CDM...")
    
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
    
    response, token_usage = llm.chat(messages)
    
    # Parse response
    try:
        # Strip markdown if present
        response_clean = response.strip()
        if response_clean.startswith("```"):
            lines = response_clean.split("\n")
            response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
            # Remove json tag if present
            if lines[0].strip().lower() == "```json":
                response_clean = "\n".join(lines[1:-1])
        
        foundation_cdm = json.loads(response_clean)
        
        # Validate structure
        if 'entities' not in foundation_cdm:
            raise ValueError("Response missing 'entities' key")
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.replace(' ', '_')
        output_file = outdir / f"foundation_cdm_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(foundation_cdm, f, indent=2)
        
        entity_count = len(foundation_cdm.get('entities', []))
        total_attrs = sum(len(e.get('attributes', [])) for e in foundation_cdm.get('entities', []))
        
        print(f"  ✓ Foundation CDM generated")
        print(f"  📁 Output: {output_file}")
        print(f"  📊 Entities: {entity_count}")
        print(f"  📊 Total attributes: {total_attrs}")
        
        return foundation_cdm
        
    except json.JSONDecodeError as e:
        print(f"  ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"  Response preview: {response[:500]}...")
        
        # Save failed response for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"step2a_error_response_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"  💾 Full response saved to: {error_file}")
        
        raise
    except ValueError as e:
        print(f"  ❌ ERROR: {e}")
        raise