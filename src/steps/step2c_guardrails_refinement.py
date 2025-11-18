# src/steps/step2c_guardrails_refinement.py
"""
Step 2c: Guardrails Refinement & Gap Analysis

Enhances CDM from Step 2b by:
- Mapping Guardrails (internal API) fields to existing CDM attributes
- Evaluating if Guardrails entities are business concepts or interface artifacts
- Adding new entities/attributes only when proper semantic fit exists
- Outputting unmapped fields to separate JSON for review

Input: Enhanced CDM from Step 2b + Rationalized Guardrails JSON
Output: Enhanced CDM with Guardrails mappings + unmapped fields JSON + disposition report
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
    
    prompt = f"""You are an expert healthcare data modeler specializing in PBM, FHIR, and NCPDP standards.

## ⚠️ CRITICAL CONTEXT: GUARDRAILS REPRESENTS PRODUCTION REALITY

**Guardrails files contain KNOWN, USED fields from actual production APIs.** These are NOT theoretical standards - these are fields actively used in business operations.

**Your task:** Map ALL Guardrails fields to the Foundational CDM provided as input. If fields cannot map, this indicates a GAP in the CDM - missing attributes or potentially missing entities.

This task is one step out of many to produce an enterprise CDM core to the Enterprise Data Platform.

This process as well as this prompt can be used for any CDM creation, the specific context for the CDM to be processed is below:

## CDM CONTEXT

**Domain:** {config.cdm.domain} - IMPORTANT TO REMEMBER THIS - WILL BE REFERENCED SEVERAL TIMES BELOW

**Description:** {config.cdm.description}

Overview of the approach for this task:
1. Map ALL Guardrails attributes to the Foundational CDM (unmapped = CDM gap)
2. Evaluate Guardrails entities (business entity vs API interface artifact)
3. For business entities: attributes likely map to ONE CDM entity
4. For interface artifacts: attributes may distribute across MULTIPLE CDM entities
5. Add new entities/attributes when gaps identified (with justification)
6. **NEVER removing or omitting any existing entities or attributes from the Foundation CDM input file**

The Foundational CDM maintaining all entities and attributes is critical to the full process **YOU MUST PRESERVE THE Foundational CDM**
- **EVERY entity** from the Foundational CDM input below must appear in your output
- **EVERY attribute** from the Foundational CDM input must appear in your output
- **DO NOT SUMMARIZE** or condense the CDM structure itself

**YOUR TASK IS TO MAP ALL KNOWN FIELDS:**
- MAP all Guardrails fields to existing CDM attributes where proper semantic fit exists
- If no semantic fit exists, this is a CDM GAP requiring new attribute or entity
- ADD new Guardrails attributes with justification when gaps identified
- PRESERVE everything from the Foundational CDM

---

## DECISION FRAMEWORK

To ensure a complete processing of EVERY Guardrails attribute, start at the first attribute in the list and step through EACH AND EVERY attribute, SKIP NONE.

**HOW TO PROCESS:**
Start with the first Guardrails entity and evaluate it.
For each entity, determine if business entity or interface artifact.
Then process each attribute sequentially: field 1, field 2, field 3, etc.
For each attribute, work through Steps 1-4 below.
Continue until all attributes processed.

### Step 1: Does the Guardrails attribute map to an attribute in the Foundational CDM?

**Semantic matching criteria:**
- Same business meaning (not just similar names)
- Same data domain and usage
- Reasonable fit (not forced alignment)

**If YES** → add the mapping information in source_mappings.guardrails

**If NO** → Proceed to Step 2

### Step 2: Is it materially important to the CDM's domain?

**⚠️ THIS SHOULD ALMOST NEVER OCCUR** - Guardrails represents known, used production fields.

Ask yourself: Does this attribute materially contribute to the semantic value of the CDM for use in the CDM's domain?

**If NO** → Mark as "unmapped" for human review
- **WARNING:** This indicates a potential CDM SCOPE PROBLEM
- Field is used in production but doesn't fit CDM domain
- Example: Prescriber NPI in Plan & Benefit CDM (belongs in Prescriber CDM)
- Document why field is out of scope

**If YES** → Proceed to Step 3

### Step 3: Identify target entity for the new attribute

**If proper entity exists in CDM:**
- Add as extension_attribute on that entity

**If NO proper entity exists:**
- **CRITICAL CDM GAP:** Production field has no semantic home
- Consider if this is part of a new entity cluster
- Check other unmapped Guardrails attributes for patterns
- If 3+ related attributes → Create extension_entity with justification
- If isolated → Mark as **unmapped** with note that CDM may be missing entity

### Step 4: Classify disposition

Every Guardrails attribute must be one of:
1. **mapped** - Direct mapping to existing CDM attribute
2. **transformed** - Derived/combined from existing CDM attributes
3. **extension_attribute** - New attribute added to existing entity
4. **extension_entity** - Part of new entity (if entity needed)
5. **unmapped** - No proper fit found, requires human review

For clearly OUT-OF-SCOPE attributes (VERY RARE - interface artifacts containing fields outside CDM domain), you MAY:
- Group them into a summarized disposition rule
- Example: "Prescriber NPI, DEA from routing_config interface are out of scope for {config.cdm.domain} CDM - belong in Prescriber CDM"
- Example: "API pagination fields (page_size, offset, limit) are technical interface artifacts not relevant to {config.cdm.domain} CDM"
- Document the grouping in the disposition report summary
- **NOTE:** Most Guardrails fields SHOULD map - extensive grouping indicates CDM scope or completeness issues

---

## ENTITY EVALUATION: BUSINESS VS INTERFACE ARTIFACT

**CRITICAL:** Guardrails entities may be API/interface groupings, NOT true business entities.

For each Guardrails entity, evaluate:

**Business Entity:**
- Represents a real-world business object
- Persists independently in a database
- Recognized by business stakeholders as a "thing"
- Examples: Carrier, Plan, Group, Member
- **Mapping pattern:** Attributes likely map to ONE corresponding CDM entity

**Interface Artifact:**
- Groups fields for API request/response structure
- Not a persisted business concept
- API plumbing or technical grouping
- Examples: handler_copay, routing_pcn, pagination_params
- **Mapping pattern:** Attributes likely DISTRIBUTE across MULTIPLE CDM entities based on semantics
- **WARNING:** Interface artifacts may contain out-of-domain attributes that don't belong in this CDM

**Actions based on evaluation:**

**If business entity:**
- Identify corresponding CDM entity (should exist if CDM is complete)
- Map attributes to that entity
- If NO corresponding entity exists → CRITICAL CDM GAP, add extension_entity with justification

**If interface artifact:**
- **Distribute attributes** to appropriate CDM entities based on semantic meaning
- Example: "handler_copay" attributes distribute to PlanBenefitCostShare, Coverage, etc.
- **Check for out-of-domain attributes:** Interface artifacts may include fields outside CDM scope (e.g., prescriber fields in Plan & Benefit CDM)
- Group out-of-domain attributes with justification (RARE)

---

## INPUTS

### Foundational CDM (from Step 2B)

**This is the Foundational CDM that was created by Steps 2a and 2b and is entrusted to you to enhance with Guardrails mappings**

This file is structured by CDM Entity then Entity Attributes. Each attribute has source_mappings showing FHIR and NCPDP mappings already completed.

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

**Structure HINTS for Guardrails files:**
- Guardrails entities may represent API structures, not always business entities
- Attributes may be calculated/derived (check `is_calculated` field)
- API context shows which endpoints use each field

{json.dumps(guardrails, indent=2)}

---

## CRITICAL REQUIREMENTS

1. **PRESERVE EVERYTHING** - To allow following steps to be performed correctly, it is CRITICAL that every entity and attribute from Foundational CDM must appear in output
2. **MAP ALL GUARDRAILS FIELDS** - These are known, used production fields. Unmapped fields indicate CDM gaps.
3. **Semantic fit required** - Map FIRST to existing CDM, but mappings must be semantically correct, not forced
4. **Justify additions** - Every new attribute/entity needs origin.justification explaining the gap being filled
5. **ACCOUNT FOR ALL ATTRIBUTES** - You MUST account for 100% of Guardrails attributes:
   - Count total Guardrails attributes in the input
   - Every attribute must have EITHER detailed disposition OR be part of a grouped rule (rare)
   - Provide field_accounting reconciliation showing: total_input_attributes = detailed + grouped
   - If your accounting does NOT equal 100%, you have FAILED the requirement
6. **Disposition requirements:**
   - Every Guardrails attribute must have a disposition classification
   - Out-of-scope grouping should be RARE (indicates CDM scope issues)
7. **Origin requirements:**
   - Preserve origin for all existing CDM attributes
   - Add origin for new attributes/entities
   - For grouped dispositions, describe the rule
8. **Output ONLY valid JSON** - No markdown, no code blocks, no commentary
9. **Processing feedback** - Include a processing_feedback section in your JSON to help improve this process (see output format below)

---

Following is the REQUIRED OUTPUT FORMAT - CRITICAL DO NOT DEVIATE FROM THIS FORMAT

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

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
      "business_context": "...",
      "key_business_questions": ["..."],
      "fhir_source_entity": "InsurancePlan",
      
      "attributes": [
        {{
          "canonical_column": "plan_identifier_value",
          "source_column": "PLAN_IDENTIFIER_VALUE",
          "data_type": "VARCHAR",
          "size": 100,
          "nullable": false,
          "glossary_term": "...",
          "business_context": "...",
          "classification": "Operational",
          
          "origin": {{
            "standard": "fhir",
            "created_in_step": "2a",
            "source_path": "InsurancePlan.identifier.value",
            "source_file": "FHIR_insuranceproduct.profile.json"
          }},
          
          "source_mappings": {{
            "fhir": {{
              "path": "InsurancePlan.identifier.value",
              "fhir_type": "Identifier",
              "source_files": ["insuranceplan.profile.json"]
            }},
            "ncpdp": {{
              "disposition": "mapped",
              "standard": "D.0",
              "segment": "AM07",
              "field": "Plan_ID",
              "data_type": "AN",
              "max_length": 8,
              "added_in_step": "2b",
              "mapping_type": "direct"
            }},
            "guardrails": {{
              "disposition": "mapped",
              "guardrails_entity": "group_plan_enrollment",
              "guardrails_attribute": "plan_id",
              "mapping_type": "direct",
              "added_in_step": "2c",
              "api_source_files": ["GR_Navitus_DGBee_v1.5_Plan_and_Benefit.xlsx"]
            }},
            "glue": null
          }}
        }}
      ]
    }}
  ],
  
  "business_capabilities": [],
  
  "guardrails_disposition_report": {{
    "summary": {{
      "total_guardrails_entities_evaluated": 0,
      "business_entities_identified": 0,
      "interface_artifacts_identified": 0,
      "total_attributes_evaluated": 0,
      "in_scope_attributes_detailed": 0,
      "out_of_scope_attributes_grouped": 0,
      "mapped_to_existing_cdm": 0,
      "mapped_via_transformation": 0,
      "extension_attributes_added": 0,
      "extension_entities_added": 0,
      "unmapped_for_review": 0
    }},
    "field_accounting": {{
      "total_input_attributes": 0,
      "detailed_disposition_count": 0,
      "grouped_disposition_count": 0,
      "total_accounted_for": 0,
      "accounting_complete": true,
      "note": "Must equal 100%: total_input_attributes = detailed + grouped"
    }},
    "entity_evaluations": [
      {{
        "guardrails_entity": "carrier",
        "evaluation": "business_entity",
        "reasoning": "Insurance carrier organization, persisted concept recognized by business",
        "cdm_mapping": "Organization entity"
      }}
    ],
    "details": [
      {{
        "guardrails_attribute": "carrier.carrier_code",
        "disposition": "mapped",
        "cdm_target": "Organization.identifier_value",
        "mapping_type": "direct",
        "notes": "{config.cdm.domain} relevant - carrier identification"
      }}
    ]
  }},
  
  "unmapped_fields": [
    {{
      "guardrails_entity": "routing_configuration",
      "guardrails_attribute": "routing_algorithm_version",
      "data_type": "string",
      "description": "Version of routing algorithm used",
      "reason_unmapped": "Technical routing detail, unclear if needed for {config.cdm.domain} CDM or belongs in separate routing domain",
      "recommendation": "Review with business analyst - may belong in different CDM or not needed"
    }}
  ],
  
  "processing_feedback": {{
    "overall_difficulty": "low | medium | high",
    "prompt_clarity": {{
      "rating": "clear | somewhat_clear | confusing",
      "issues": ["description of any unclear instructions or empty array"]
    }},
    "input_quality": {{
      "foundational_cdm_from_2b": "excellent | good | issues",
      "guardrails_specifications": "excellent | good | issues",
      "issues_encountered": ["description of any data quality problems or empty array"]
    }},
    "entity_evaluation_challenges": {{
      "ambiguous_entities": 0,
      "difficult_decisions": 0,
      "examples": []
    }},
    "mapping_challenges": {{
      "ambiguous_attributes": 0,
      "difficult_decisions": 0,
      "examples": []
    }},
    "cdm_gap_analysis": {{
      "missing_entities_identified": 0,
      "missing_attributes_identified": 0,
      "out_of_scope_attributes": 0,
      "notes": []
    }},
    "recommendations": [],
    "quality_for_downstream": "This section is critical - quality issues here cascade to Steps 2d (Glue), 2e (Final), 3 (Relationships), 4 (DDL), and 5 (Excel). Note any concerns about output quality."
  }}
}}
```

---

Generate the enhanced Foundational CDM JSON with inclusion of aligned Guardrails mappings.
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
        
        # Generate timestamp for all output files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.replace(' ', '_')
        
        # Extract and save processing feedback to separate file
        feedback = result.pop('processing_feedback', None)
        if feedback:
            feedback_file = outdir / f"step2c_feedback_{domain_safe}_{timestamp}.txt"
            
            with open(feedback_file, 'w', encoding='utf-8') as f:
                f.write(f"STEP 2C PROCESSING FEEDBACK\n")
                f.write(f"Generated: {datetime.now().isoformat()}\n")
                f.write(f"Domain: {config.cdm.domain}\n")
                f.write(f"=" * 80 + "\n\n")
                
                f.write(f"OVERALL DIFFICULTY: {feedback.get('overall_difficulty', 'not provided')}\n\n")
                
                prompt_clarity = feedback.get('prompt_clarity', {})
                f.write(f"PROMPT CLARITY: {prompt_clarity.get('rating', 'not provided')}\n")
                if prompt_clarity.get('issues'):
                    for issue in prompt_clarity['issues']:
                        f.write(f"  - {issue}\n")
                f.write("\n")
                
                input_quality = feedback.get('input_quality', {})
                f.write(f"INPUT QUALITY:\n")
                f.write(f"  Enhanced CDM from 2B: {input_quality.get('foundational_cdm_from_2b', 'not provided')}\n")
                f.write(f"  Guardrails Specs: {input_quality.get('guardrails_specifications', 'not provided')}\n")
                if input_quality.get('issues_encountered'):
                    f.write(f"  Issues:\n")
                    for issue in input_quality['issues_encountered']:
                        f.write(f"    - {issue}\n")
                f.write("\n")
                
                entity_challenges = feedback.get('entity_evaluation_challenges', {})
                f.write(f"ENTITY EVALUATION CHALLENGES:\n")
                f.write(f"  Ambiguous entities: {entity_challenges.get('ambiguous_entities', 0)}\n")
                f.write(f"  Difficult decisions: {entity_challenges.get('difficult_decisions', 0)}\n")
                if entity_challenges.get('examples'):
                    f.write(f"\n  Examples:\n")
                    for example in entity_challenges['examples']:
                        f.write(f"\n  {example.get('entity_name', 'N/A')}\n")
                        f.write(f"    Issue: {example.get('issue', 'N/A')}\n")
                        f.write(f"    Decision: {example.get('decision', 'N/A')}\n")
                        f.write(f"    Confidence: {example.get('confidence', 'N/A')}\n")
                f.write("\n")
                
                mapping_challenges = feedback.get('mapping_challenges', {})
                f.write(f"MAPPING CHALLENGES:\n")
                f.write(f"  Ambiguous attributes: {mapping_challenges.get('ambiguous_attributes', 0)}\n")
                f.write(f"  Difficult decisions: {mapping_challenges.get('difficult_decisions', 0)}\n")
                if mapping_challenges.get('examples'):
                    f.write(f"\n  Examples:\n")
                    for example in mapping_challenges['examples']:
                        f.write(f"\n  {example.get('attribute', 'N/A')}\n")
                        f.write(f"    Issue: {example.get('issue', 'N/A')}\n")
                        f.write(f"    Decision: {example.get('decision', 'N/A')}\n")
                        f.write(f"    Confidence: {example.get('confidence', 'N/A')}\n")
                f.write("\n")
                
                gap_analysis = feedback.get('cdm_gap_analysis', {})
                f.write(f"CDM GAP ANALYSIS:\n")
                f.write(f"  Missing entities identified: {gap_analysis.get('missing_entities_identified', 0)}\n")
                f.write(f"  Missing attributes identified: {gap_analysis.get('missing_attributes_identified', 0)}\n")
                f.write(f"  Out of scope attributes: {gap_analysis.get('out_of_scope_attributes', 0)}\n")
                if gap_analysis.get('notes'):
                    f.write(f"  Notes:\n")
                    for note in gap_analysis['notes']:
                        f.write(f"    - {note}\n")
                f.write("\n")
                
                recommendations = feedback.get('recommendations', [])
                if recommendations:
                    f.write(f"RECOMMENDATIONS:\n")
                    for i, rec in enumerate(recommendations, 1):
                        f.write(f"  {i}. {rec}\n")
                    f.write("\n")
                
                quality_note = feedback.get('quality_for_downstream', '')
                if quality_note:
                    f.write(f"QUALITY FOR DOWNSTREAM STEPS:\n")
                    f.write(f"  {quality_note}\n")
            
            print(f"  📝 Feedback saved: {feedback_file}")
        
        # Extract and save disposition report to separate file
        disposition_report = result.pop('guardrails_disposition_report', None)
        if disposition_report:
            disp_file = outdir / f"guardrails_disposition_report_{domain_safe}_{timestamp}.json"
            with open(disp_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "domain": config.cdm.domain,
                    "step": "2c",
                    "timestamp": timestamp,
                    "disposition_report": disposition_report
                }, f, indent=2)
            print(f"  📋 Disposition report saved: {disp_file}")
        else:
            print("  ⚠️  WARNING: Response missing 'guardrails_disposition_report'")
        
        # Extract unmapped fields (keep separate)
        unmapped_fields = result.pop('unmapped_fields', [])
        
        # Now result contains only clean CDM (entities + metadata)
        enhanced_cdm_output = result
        
        # Validate structure
        if 'entities' not in enhanced_cdm_output:
            print(f"  ❌ ERROR: Response missing 'entities' key")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm_output, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm_output
        
        # Validate no data loss - CRITICAL
        input_entity_count = len(enhanced_cdm.get('entities', []))
        output_entity_count = len(enhanced_cdm_output.get('entities', []))
        
        if output_entity_count < input_entity_count:
            print(f"  ❌ DATA LOSS: {output_entity_count} entities vs {input_entity_count} expected")
            print(f"  ⚠️  LLM removed {input_entity_count - output_entity_count} entities")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm_output, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm_output
        
        input_attr_count = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        output_attr_count = sum(len(e.get('attributes', [])) for e in enhanced_cdm_output.get('entities', []))
        
        if output_attr_count < input_attr_count:
            print(f"  ❌ DATA LOSS: {output_attr_count} attributes vs {input_attr_count} expected")
            print(f"  ⚠️  LLM removed {input_attr_count - output_attr_count} attributes")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm_output, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm_output
        
        print(f"  ✓ Validation passed: No data loss detected")
        
        # Field accounting validation (if disposition report available)
        if disposition_report:
            total_gr_attrs = sum(len(e.get('attributes', [])) for e in guardrails.get('rationalized_entities', []))
            
            if total_gr_attrs > 0:
                field_accounting = disposition_report.get('field_accounting', {})
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
        
        # Save clean enhanced CDM (without disposition report and feedback)
        cdm_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
        with open(cdm_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_cdm_output, f, indent=2)
        
        print(f"  ✓ Clean CDM saved: {cdm_file}")
        
        entity_count = len(enhanced_cdm_output.get('entities', []))
        total_attrs = sum(len(e.get('attributes', [])) for e in enhanced_cdm_output.get('entities', []))
        
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
        
        # Report disposition summary if available
        if disposition_report:
            disp = disposition_report.get('summary', {})
            if disp:
                print(f"\n  📋 Guardrails Disposition:")
                print(f"     Total entities: {disp.get('total_guardrails_entities_evaluated', 0)}")
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
        domain_safe = config.cdm.domain.replace(' ', '_')
        error_file = outdir / f"step2c_error_response_{domain_safe}_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"  💾 Full response saved to: {error_file}")
        
        return None
    except ValueError as e:
        print(f"  ❌ ERROR: {e}")
        return None