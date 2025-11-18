# src/steps/step2c_guardrails_refinement.py
"""
Step 2c: Guardrails Refinement & Gap Analysis

Enhances CDM from Step 2b by:
- Mapping Guardrails (internal API) fields to existing CDM attributes
- Evaluating if Guardrails entities are business concepts or interface artifacts
- Adding new entities/attributes only when proper semantic fit exists
- Outputting unmapped fields to separate JSON for review

Input: Enhanced CDM from Step 2b + Rationalized Guardrails JSON
Output: Enhanced CDM with Guardrails mappings + unmapped fields JSON
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_prompt(config: AppConfig, enhanced_cdm: dict, guardrails: dict) -> str:
    """Build Step 2c prompt for Guardrails refinement"""
    
    prompt = f"""You are an expert healthcare data modeler specializing in PBM data integration.

## ⚠️ PRESERVE THE CDM STRUCTURE

**YOU MUST PRESERVE THE ENHANCED CDM:**
- **EVERY entity** from the enhanced CDM input must appear in output
- **EVERY attribute** from enhanced CDM must appear in output
- You may REORDER, but do not REMOVE
- Your task is to ADD Guardrails mappings, not rewrite the CDM

**If you approach response size limits:**
- PRIORITIZE keeping CDM entities/attributes intact
- You MAY summarize disposition entries
- DO NOT return error; return best complete CDM JSON within limits

---

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Enhance the CDM by mapping Guardrails (internal API specifications) fields and identifying gaps.

**This is Step 2c: Guardrails Refinement** - you are:
1. Evaluating Guardrails entities (business concept vs interface artifact)
2. Mapping Guardrails attributes to existing CDM
3. Adding new entities/attributes ONLY when proper semantic fit exists
4. Outputting unmapped fields for human review

---

## INPUTS

### ENHANCED CDM (from Steps 2a-2b)

{json.dumps(enhanced_cdm, indent=2)}

---

### GUARDRAILS (Internal API Specifications)

**Source:** Internal API definitions from business/data analysts

**Structure:**
- `rationalized_entities`: List of API entity groupings
- Each entity contains attributes with:
  - `attribute_name`, `data_type`, `description`
  - `business_context`, `api_context`
  - `source_files` (which APIs use this field)

{json.dumps(guardrails, indent=2)}

---

## CRITICAL: UNDERSTANDING GUARDRAILS "ENTITIES"

**Guardrails entities may be API/interface groupings, NOT true business entities.**

Analysts who create these specs often organize data by interface structure. This means:
- Some Guardrails "entities" are genuine business concepts (e.g., `carrier`, `plan`, `group`)
- Others are just API endpoint structures (e.g., `handler_copay`, `routing_pcn`)

**YOUR TASK: Evaluate each Guardrails entity**

For each Guardrails entity, determine:
1. **Business entity** - Persists independently, recognized by stakeholders as business object
2. **Interface artifact** - Groups fields for API structure, not a persisted business concept

**Evaluation criteria:**
- Does it represent a real-world business object?
- Would business users recognize and discuss this as a "thing"?
- Or is it just organizing API request/response structure?

**Actions based on evaluation:**

**If business entity:**
- Check if similar concept exists in CDM
- If exists → Map attributes to that entity
- If doesn't exist → Consider as extension_entity

**If interface artifact:**
- **Focus on attributes, not entity alignment**
- Distribute attributes to appropriate CDM entities based on semantics
- Document that Guardrails entity was interface grouping

---

## DECISION FRAMEWORK: PROPER SEMANTIC MATCHING

### Step 1: Can attribute map to existing CDM?

**Semantic matching criteria:**
- Same business meaning (not just similar names)
- Same data domain and usage
- Reasonable fit (not forced alignment)

**If YES** → MAP IT
- Example: Guardrails `carrier_code` → CDM `Organization.identifier_value`

**If NO** → Proceed to Step 2

### Step 2: Does attribute need a new location?

**Check if proper entity exists:**
- Is there a CDM entity that semantically fits this attribute?
- Would adding this attribute make sense to that entity?

**If proper entity exists** → Add as extension_attribute

**If no proper entity exists:**
- Could this be part of a new entity cluster?
- Check other unmapped Guardrails attributes for patterns
- If 3+ related attributes → Consider extension_entity
- If isolated → Mark as **unmapped**

### Step 3: Document disposition

**Every Guardrails attribute must be:**
1. **mapped** - Semantic match to existing CDM attribute
2. **transformed** - Derived from existing CDM attributes
3. **extension_attribute** - New attribute added to proper entity
4. **extension_entity** - Part of new entity (if entity needed)
5. **unmapped** - No proper fit found, requires human review

---

## UNMAPPED FIELDS

**Purpose:** Identify fields that need human decision

**When to mark unmapped:**
- No semantic match in existing CDM
- No proper entity exists for this attribute
- Unclear if new entity warranted
- Business context ambiguous

**Unmapped fields output to separate JSON file for review**

---

## OUTPUT STRUCTURE

Your response must be valid JSON with this structure:

```json
{{
  "cdm_metadata": {{
    "domain": "{config.cdm.domain}",
    "version": "1.0",
    "description": "...",
    "foundation_standard": "FHIR",
    "generation_timestamp": "ISO_DATETIME",
    "generation_steps_completed": ["2a", "2b", "2c"]
  }},
  
  "entities": [
    {{
      "entity_name": "InsurancePlan",
      "classification": "Core",
      "business_definition": "...",
      "attributes": [
        {{
          "canonical_column": "plan_identifier_value",
          "source_column": "PLAN_IDENTIFIER_VALUE",
          "data_type": "VARCHAR",
          "size": 50,
          "nullable": false,
          "glossary_term": "...",
          "business_context": "...",
          "classification": "Identifier",
          "source_mappings": {{
            "fhir": {{...}},
            "ncpdp": {{...}},
            "guardrails": {{
              "disposition": "mapped",
              "guardrails_entity": "group_plan_enrollment",
              "guardrails_attribute": "plan_id",
              "mapping_type": "direct",
              "added_in_step": "2c",
              "api_sources": ["Hierarchy_Gen1_API", "Benefit_Modernization_API"]
            }},
            "glue": null
          }}
        }}
      ]
    }}
  ],
  
  "guardrails_disposition_report": {{
    "summary": {{
      "total_guardrails_entities": 0,
      "business_entities_identified": 0,
      "interface_artifacts_identified": 0,
      "total_attributes_evaluated": 0,
      "mapped_to_existing_cdm": 0,
      "mapped_via_transformation": 0,
      "extension_attributes_added": 0,
      "extension_entities_added": 0,
      "unmapped_for_review": 0
    }},
    "field_accounting": {{
      "total_input_attributes": 0,
      "detailed_disposition_count": 0,
      "total_accounted_for": 0,
      "accounting_complete": true,
      "note": "Must equal 100%: total_input_attributes = detailed_disposition_count"
    }},
    "entity_evaluations": [
      {{
        "guardrails_entity": "carrier",
        "evaluation": "business_entity",
        "reasoning": "Represents insurance carrier organization, persisted business concept",
        "cdm_alignment": "Mapped to Organization entity",
        "attributes_count": 5,
        "attributes_mapped": 4,
        "attributes_unmapped": 1
      }},
      {{
        "guardrails_entity": "handler_copay",
        "evaluation": "interface_artifact",
        "reasoning": "Groups copay calculation logic for API response, not a persisted object",
        "cdm_alignment": "Distributed attributes to PlanBenefitCostShare",
        "attributes_count": 3,
        "attributes_mapped": 3,
        "attributes_unmapped": 0
      }}
    ],
    "details": [
      {{
        "guardrails_entity": "carrier",
        "guardrails_attribute": "carrier_code",
        "disposition": "mapped",
        "cdm_target": "Organization.identifier_value",
        "mapping_type": "semantic_match",
        "notes": "Primary carrier identifier"
      }},
      {{
        "guardrails_entity": "group_benefit_date_info",
        "guardrails_attribute": "coverage_start_date",
        "disposition": "extension_attribute",
        "cdm_target": "Coverage.coverage_period_start",
        "mapping_type": "new_attribute",
        "justification": "Group-level coverage dates not captured in FHIR Coverage"
      }}
    ]
  }},
  
  "unmapped_fields": [
    {{
      "guardrails_entity": "rate_handler",
      "guardrails_attribute": "handler_config_json",
      "data_type": "string",
      "description": "JSON configuration for pricing handler",
      "business_context": "...",
      "reason_unmapped": "Technical configuration field, unclear if CDM should persist vs runtime only",
      "recommendation": "Review with architects - may belong in operational config, not analytical CDM"
    }}
  ]
}}
```

---

## CRITICAL REQUIREMENTS

1. **PRESERVE EVERYTHING** - Every entity and attribute from enhanced CDM must appear in output
2. **EVALUATE ENTITIES** - Determine if Guardrails entity is business concept or interface artifact
3. **SEMANTIC MATCHING** - Only map when reasonable fit exists (not forced)
4. **DOCUMENT DECISIONS** - Clear reasoning for entity evaluations and unmapped fields
5. **ACCOUNT FOR ALL FIELDS** - You MUST account for 100% of Guardrails attributes:
   - Count total Guardrails attributes in input
   - Every attribute must have disposition (mapped/transformed/extension/unmapped)
   - Provide field_accounting showing: total = detailed dispositions
   - If accounting ≠ 100%, you FAILED
6. **UNMAPPED OUTPUT** - Fields with no proper fit go to unmapped_fields array
7. **OUTPUT ONLY VALID JSON** - No markdown, no code blocks, no commentary

---

Generate the enhanced CDM JSON with Guardrails mappings now.
"""
    
    return prompt


def run_step2c(
    config: AppConfig,
    enhanced_cdm_file: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[dict]:
    """
    Step 2c: Add Guardrails refinement to enhanced CDM
    
    Args:
        config: Configuration object with CDM description
        enhanced_cdm_file: Path to enhanced CDM JSON from Step 2b
        outdir: Output directory for enhanced CDM JSON
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Enhanced CDM dict (None in dry run)
    """
    
    print(f"  📖 Loading enhanced CDM from: {enhanced_cdm_file}")
    
    # Load enhanced CDM from Step 2b
    with open(enhanced_cdm_file, 'r', encoding='utf-8') as f:
        enhanced_cdm = json.load(f)
    
    entity_count = len(enhanced_cdm.get('entities', []))
    print(f"  📊 Found {entity_count} entities in enhanced CDM")
    
    # Load rationalized Guardrails
    print(f"  📖 Loading rationalized Guardrails...")
    prep_outdir = outdir.parent / "prep"
    
    guardrails_files = sorted(prep_outdir.glob("rationalized_guardrails_*.json"))
    if not guardrails_files:
        print(f"  ❌ ERROR: No rationalized Guardrails found. Run Step 1b first.")
        return None
    
    guardrails_file = guardrails_files[-1]
    print(f"  📁 Using: {guardrails_file.name}")
    
    with open(guardrails_file, 'r', encoding='utf-8') as f:
        guardrails = json.load(f)
    
    gr_entities = len(guardrails.get('rationalized_entities', []))
    gr_attrs = sum(len(e.get('attributes', [])) for e in guardrails.get('rationalized_entities', []))
    print(f"  📊 Guardrails: {gr_entities} entities, {gr_attrs} attributes")
    
    # Build prompt
    prompt = build_prompt(config, enhanced_cdm, guardrails)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"step2c_guardrails_refinement_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"  ✓ Prompt saved: {output_file}")
        print(f"    Characters: {len(prompt):,}")
        print(f"    Tokens (est): {len(prompt) // 4:,}")
        return None
    
    # Live mode - call LLM
    print(f"  🤖 Calling LLM to enhance CDM with Guardrails mappings...")
    
    messages = [
        {
            "role": "system",
            "content": "You are a healthcare data architect. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
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
            if lines[0].strip().lower() == "```json":
                response_clean = "\n".join(lines[1:-1])
            else:
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
        
        result = json.loads(response_clean)
        
        # Validate structure
        if 'entities' not in result:
            raise ValueError("Response missing 'entities' key")
        if 'guardrails_disposition_report' not in result:
            print("  ⚠️  WARNING: Response missing 'guardrails_disposition_report'")
        
        # Validate no data loss - CRITICAL
        input_entity_count = len(enhanced_cdm.get('entities', []))
        output_entity_count = len(result.get('entities', []))
        
        if output_entity_count < input_entity_count:
            raise ValueError(
                f"❌ DATA LOSS DETECTED: Output has {output_entity_count} entities "
                f"but input had {input_entity_count}. "
                f"LLM removed {input_entity_count - output_entity_count} entities. REJECTING OUTPUT."
            )
        
        input_attr_count = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        output_attr_count = sum(len(e.get('attributes', [])) for e in result.get('entities', []))
        
        if output_attr_count < input_attr_count:
            raise ValueError(
                f"❌ DATA LOSS DETECTED: Output has {output_attr_count} attributes "
                f"but input had {input_attr_count}. "
                f"LLM removed {input_attr_count - output_attr_count} attributes. REJECTING OUTPUT."
            )
        
        # Validate field accounting - CRITICAL
        total_gr_attrs = sum(len(e.get('attributes', [])) for e in guardrails.get('rationalized_entities', []))
        
        if total_gr_attrs > 0:
            field_accounting = result.get('guardrails_disposition_report', {}).get('field_accounting', {})
            if field_accounting:
                accounted = field_accounting.get('total_accounted_for', 0)
                if accounted != total_gr_attrs:
                    print(f"  ⚠️  WARNING: Field accounting incomplete")
                    print(f"     Total Guardrails attributes: {total_gr_attrs}")
                    print(f"     Attributes accounted for: {accounted}")
                    print(f"     Missing: {total_gr_attrs - accounted} attributes")
                else:
                    print(f"  ✓ Field accounting complete: {accounted}/{total_gr_attrs} attributes")
            else:
                print(f"  ⚠️  WARNING: No field_accounting section in disposition report")
        
        print(f"  ✓ Validation passed: No data loss detected")
        
        # Extract enhanced CDM and unmapped fields
        enhanced_cdm_output = {k: v for k, v in result.items() if k != 'unmapped_fields'}
        unmapped_fields = result.get('unmapped_fields', [])
        
        # Save enhanced CDM
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.replace(' ', '_')
        
        cdm_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
        with open(cdm_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_cdm_output, f, indent=2)
        
        entity_count = len(enhanced_cdm_output.get('entities', []))
        total_attrs = sum(len(e.get('attributes', [])) for e in enhanced_cdm_output.get('entities', []))
        
        print(f"  ✓ Enhanced CDM generated")
        print(f"  📁 Output: {cdm_file}")
        print(f"  📊 Entities: {entity_count}")
        print(f"  📊 Total attributes: {total_attrs}")
        
        # Save unmapped fields if any
        if unmapped_fields:
            unmapped_file = outdir / f"unmapped_guardrails_{domain_safe}_{timestamp}.json"
            with open(unmapped_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "domain": config.cdm.domain,
                    "step": "2c",
                    "timestamp": timestamp,
                    "total_unmapped": len(unmapped_fields),
                    "unmapped_fields": unmapped_fields
                }, f, indent=2)
            print(f"  📁 Unmapped fields: {unmapped_file}")
            print(f"  ⚠️  {len(unmapped_fields)} fields require review")
        else:
            print(f"  ✓ All fields mapped - no unmapped output")
        
        # Report disposition summary
        disp = enhanced_cdm_output.get('guardrails_disposition_report', {}).get('summary', {})
        
        if disp:
            print(f"\n  📋 Guardrails Disposition:")
            print(f"     Total entities: {disp.get('total_guardrails_entities', 0)}")
            print(f"     Business entities: {disp.get('business_entities_identified', 0)}")
            print(f"     Interface artifacts: {disp.get('interface_artifacts_identified', 0)}")
            print(f"     Attributes evaluated: {disp.get('total_attributes_evaluated', 0)}")
            print(f"     Mapped to existing: {disp.get('mapped_to_existing_cdm', 0)}")
            print(f"     New attributes: {disp.get('extension_attributes_added', 0)}")
            print(f"     New entities: {disp.get('extension_entities_added', 0)}")
            print(f"     Unmapped: {disp.get('unmapped_for_review', 0)}")
        
        return enhanced_cdm_output
        
    except json.JSONDecodeError as e:
        print(f"  ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"  Response preview: {response[:500]}...")
        
        # Save failed response for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"step2c_error_response_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"  💾 Full response saved to: {error_file}")
        
        raise
    except ValueError as e:
        print(f"  ❌ ERROR: {e}")
        raise