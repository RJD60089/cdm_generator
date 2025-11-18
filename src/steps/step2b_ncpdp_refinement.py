# src/steps/step2b_ncpdp_refinement.py
"""
Step 2b: NCPDP Refinement & Gap Analysis

Enhances foundation CDM from Step 2a by:
- Mapping NCPDP fields to existing CDM attributes
- Adding NCPDP-specific attributes when no FHIR equivalent exists
- Generating disposition report (mapped vs added vs not used)

Input: Foundation CDM JSON from Step 2a + NCPDP standards
Output: Enhanced CDM JSON with NCPDP crosswalk + gap analysis
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_prompt(config: AppConfig, foundation_cdm: dict, ncpdp_standards: dict) -> str:
    """Build Step 2b prompt for NCPDP refinement"""
    
    prompt = f"""You are an expert healthcare data modeler specializing in PBM, FHIR, and NCPDP standards.

Your task is to map a list of NCPDP fields and align it with a foundational canonical data model (CDM) based on FHIR resources.

The objective is to augment the original foundational CDM with mapping information to the NCPDP fields.

This task is one step out of many to produce an enterprise CDM core to the Enterprise Data Platform.

A previous step refined the list of NCPDP fields to those NCPDP Standards that best align to this CDM's Domain (below).

Following steps will include further refinement and establishing the Entity relationships - your focus in this step is to ensure that all semantically appropriate NCPDP fields are mapped to the Foundational CDM or in rare occasions added to the Foundational CDM.

This process as well as this prompt can be used for any CDM creation, the specific context for the CDM to be processed is below:

## CDM CONTEXT

**Domain:** {config.cdm.domain} - IMPORTANT TO REMEMBER THIS - WILL BE REFERENCED SEVERAL TIMES BELOW

**Description:** {config.cdm.description}

Overview of the approach for this task:
1. Map NCPDP fields to existing CDM attributes
2. Adding NCPDP-specific attributes ONLY when justified and no CDM equivalent exists
3. Generating disposition report: detailed for in-scope fields, grouped rules for out-of-scope
4. **NEVER removing or omitting any existing entities or attributes**

The Foundational CDM maintaining all entities and attributes is critical to the full process **YOU MUST PRESERVE THE FOUNDATIONAL CDM**
- **EVERY entity** from the foundation CDM input below must appear in your output
- **EVERY attribute** from the foundation CDM input must appear in your output
- **DO NOT SUMMARIZE** or condense the CDM structure itself

**YOUR TASK IS TO ADD, NOT SUBTRACT:**
- ADD the NCPDP mappings that align with the provided Foundational FHIR attributes
- It is expected that most NCPDP fields WILL map to the Foundation FHIR Entities and Attributes - ONLY ADD new NCPDP to the Foundational CDM when there is no match and there is semantic value relative to the CDM's DOMAIN

---

## DECISION FRAMEWORK

To ensure a complete processing of EVERY NCPDP field, start at the first field in the list and step through EACH AND EVERY NCPDP field, SKIP NONE.

**HOW TO PROCESS:**
Start with the first NCPDP field in the provided list.
Process sequentially: field 1, field 2, field 3, etc.
For each field, work through Steps 1-4 below.
Continue until all fields processed.

### Step 1: Does the NCPDP field map to a field in the FHIR based Foundation CDM?

**If YES** → add the mapping information that provides the alignment from the Foundational CDM to NCPDP fields

**If NO** → Proceed to Step 2

### Step 2: Is it materially important to PBM operations?

Ask yourself: Does this field materially contribute to the semantic value of the CDM for use in the CDM's domain?

**If NO** → Mark as "not_used" with justification
**If YES** → Proceed to Step 3

### Step 3: Add the new field within the appropriate Foundation CDM entity

### Step 4: Classify disposition

Every IN-SCOPE NCPDP field ({config.cdm.domain} relevant) must be one of:
1. **mapped** - Direct mapping to existing CDM attribute
2. **transformed** - Derived/combined from existing CDM attributes
3. **extension_attribute** - New attribute added to existing entity
4. **not_used** - Field not needed (with business justification)

For clearly OUT-OF-SCOPE fields, you MUST:
- Group them into a summarized disposition rule
- Example: "All SCRIPT message-header fields (MessageID, SentTime, etc.) are not_used for {config.cdm.domain} CDM"
- Document the grouping in the disposition report summary

---

## INPUTS

### FOUNDATIONAL FHIR BASED CDM
**This is the Foundation CDM that was created by several previous steps and is entrusted to you to enhance with NCPDP field alignment**
This file is structured by CDM Entity then Entity Attributes. Each field has a set of metadata that can be used with the NCPDP field metadata to complete mappings.

{json.dumps(foundation_cdm, indent=2)}

---

### NCPDP FIELDS SELECTED IN PREVIOUS STEP BASED ON CDM'S DOMAIN

**Structure HINTS for NCPDP files:**
- `_columns`: Explains what each abbreviated key means (i=FIELD, n=NAME, d=DEFINITION, etc.)
- `_standards`: Lookup table for standard codes (T=Telecommunication, A=Post Adjudication, etc.)
- `fields`: Array of select NCPDP fields based on CDM domain
  - Each field has `s` property listing which standards the NCPDP field is used in (e.g., "T,A" = used in Telecom & Post Adjudication)

"""
    
    # Add NCPDP General standards if present
    if ncpdp_standards.get('general'):
        prompt += f"""
**'GENERAL' includes NCPDP Fields from select NCPDP Standards excluding SCRIPT and SPECIALIZED:**
This file has only one instance of each field and has been scoped to the NCPDP Standards that are most appropriate for the CDM's Domain.

{json.dumps(ncpdp_standards['general'], indent=2)}
"""
    
    # Add NCPDP Script standards if present
    if ncpdp_standards.get('script'):
        prompt += f"""
**'SCRIPT' includes NCPDP Fields from SCRIPT and SPECIALIZED:**
This file has only one instance of each field and has been scoped to the NCPDP Standards that are most appropriate for the CDM's Domain.

{json.dumps(ncpdp_standards['script'], indent=2)}
"""
    
    prompt += """
---

## CRITICAL REQUIREMENTS

1. **PRESERVE EVERYTHING** - To allow following steps to be performed correctly, it is CRITICAL that every entity and attribute from foundation CDM must appear in output
2. **Focus on in-scope fields** - Prioritize the CDM's Domain relevant NCPDP fields for detailed disposition
3. **Map FIRST** - Try to map every relevant NCPDP field to existing FHIR based CDM field
4. **Justify additions** - Every new attribute/entity needs origin.justification explaining why mapping impossible
5. **ACCOUNT FOR ALL FIELDS** - You MUST account for 100% of NCPDP fields:
   - Count total NCPDP fields in the input
   - Every field must have EITHER detailed disposition OR be part of a grouped rule
   - Provide field_accounting reconciliation showing: total_input_fields = detailed + grouped
   - If your accounting does NOT equal 100%, you have FAILED the requirement
6. **Disposition requirements:**
   - Every IN-SCOPE NCPDP field must have a disposition classification
   - For OUT-OF-SCOPE fields, you MAY:
     - Define one or more grouped rules (e.g., "All prescriber-only identifiers = not_used for Plan & Benefit CDM")
     - You do NOT need to list every one individually
7. **Origin requirements:**
   - Preserve origin for all existing CDM attributes
   - Add origin for new attributes/entities
   - For grouped or summarized dispositions, it is sufficient to describe the rule, not each field
8. **Output ONLY valid JSON** - No markdown, no code blocks, no commentary
9. **Processing feedback** - Include a processing_feedback section in your JSON to help improve this process (see output format below)

---

Following is the REQUIRED OUTPUT FORMAT - CRITICAL DO NOT DEVIATE FROM THIS FORMAT

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{
  "cdm_metadata": {
    "domain": "Plan and Benefit",
    "version": "1.0",
    "description": "...",
    "foundation_standard": "FHIR",
    "generation_timestamp": "ISO_DATETIME",
    "generation_steps_completed": ["2a", "2b"]
  },
  
  "entities": [
    {
      "entity_name": "InsurancePlan",
      "classification": "Core",
      "business_definition": "...",
      "business_context": "...",
      "key_business_questions": ["..."],
      "fhir_source_entity": "InsurancePlan",
      
      "primary_key": {
        "type": "natural",
        "attributes": ["plan_identifier_value"]
      },
      
      "foreign_keys": [
        {
          "name": "fk_plan_product",
          "attributes": ["insurance_product_id"],
          "references_entity": "InsuranceProduct",
          "references_attributes": ["product_identifier_value"],
          "on_delete": "RESTRICT",
          "on_update": "CASCADE"
        }
      ],
      
      "attributes": [
        {
          "canonical_column": "plan_identifier_value",
          "source_column": "PLAN_IDENTIFIER_VALUE",
          "data_type": "VARCHAR",
          "size": 100,
          "nullable": false,
          "glossary_term": "...",
          "business_context": "...",
          "classification": "Operational",
          
          "origin": {
            "standard": "fhir",
            "created_in_step": "2a",
            "source_path": "InsurancePlan.identifier.value",
            "source_file": "insuranceplan.profile.json"
          },
          
          "source_mappings": {
            "fhir": {
              "path": "InsurancePlan.identifier.value",
              "fhir_type": "Identifier",
              "source_files": ["insuranceplan.profile.json"]
            },
            "ncpdp": {
              "disposition": "mapped",
              "standard": "D.0",
              "segment": "AM07",
              "field": "Plan_ID",
              "data_type": "AN",
              "max_length": 8,
              "added_in_step": "2b",
              "mapping_type": "direct"
            },
            "guardrails": null,
            "glue": null
          }
        },
        {
          "canonical_column": "dispense_as_written_code",
          "source_column": "DISPENSE_AS_WRITTEN_CODE",
          "data_type": "VARCHAR",
          "size": 2,
          "nullable": true,
          "glossary_term": "DAW code indicating prescriber intent for substitution...",
          "business_context": "Required for adjudication to determine if generic substitution allowed...",
          "classification": "Operational",
          
          "origin": {
            "standard": "ncpdp",
            "created_in_step": "2b",
            "source_path": "D.0.420-DK.DAW_Product_Selection_Code",
            "source_file": "ncpdp_general.json",
            "justification": "Pharmacy-specific code with no FHIR equivalent, critical for adjudication and pricing"
          },
          
          "source_mappings": {
            "fhir": null,
            "ncpdp": {
              "disposition": "extension_attribute",
              "standard": "D.0",
              "segment": "420-DK",
              "field": "DAW_Product_Selection_Code",
              "data_type": "N",
              "max_length": 1,
              "added_in_step": "2b",
              "valid_values": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
            },
            "guardrails": null,
            "glue": null
          }
        }
      ]
    }
  ],
  
  "business_capabilities": [],
  
  "ncpdp_disposition_report": {
    "summary": {
      "total_ncpdp_fields_evaluated": 0,
      "in_scope_fields_detailed": 0,
      "out_of_scope_fields_grouped": 0,
      "mapped_to_existing_cdm": 0,
      "mapped_via_transformation": 0,
      "extension_attributes_added": 0,
      "extension_entities_added": 0,
      "not_used": 0
    },
    "field_accounting": {
      "total_input_fields": 0,
      "detailed_disposition_count": 0,
      "grouped_disposition_count": 0,
      "total_accounted_for": 0,
      "accounting_complete": true,
      "note": "Must equal 100%: total_input_fields = detailed + grouped"
    },
    "details": [
      {
        "ncpdp_field": "302-C2.Cardholder_ID",
        "disposition": "mapped",
        "cdm_target": "Coverage.subscriber_id",
        "mapping_type": "direct",
        "notes": "Plan & Benefit relevant - member identification"
      },
      {
        "ncpdp_field": "512-FC.Accumulated_Deductible_Amount",
        "disposition": "extension_attribute",
        "cdm_target": "Coverage.deductible_accumulated",
        "mapping_type": "new_attribute",
        "justification": "Pharmacy-specific accumulator, no FHIR equivalent, required for benefit stage tracking"
      },
      {
        "disposition_group": "prescriber_identifiers",
        "disposition": "not_used",
        "fields_count": 15,
        "example_fields": ["401-D1.Prescriber_ID", "411-DB.Prescriber_Last_Name", "427-DR.DEA_Number"],
        "justification": "Prescriber entity outside Plan & Benefit CDM scope - belongs in Prescriber CDM"
      },
      {
        "disposition_group": "dur_clinical_details",
        "disposition": "not_used",
        "fields_count": 25,
        "example_fields": ["473-7E.DUR_Service_Reason_Code", "439-E4.Reason_For_Service_Code"],
        "justification": "Clinical DUR details out of scope for Plan & Benefit - belongs in Utilization Management CDM"
      }
    ]
  },
  
  "processing_feedback": {
    "overall_difficulty": "low | medium | high",
    "prompt_clarity": {
      "rating": "clear | somewhat_clear | confusing",
      "issues": ["description of any unclear instructions or empty array"]
    },
    "input_quality": {
      "foundation_cdm": "excellent | good | issues",
      "ncpdp_standards": "excellent | good | issues",
      "issues_encountered": ["description of any data quality problems or empty array"]
    },
    "mapping_challenges": {
      "ambiguous_fields": 0,
      "difficult_decisions": 0,
      "examples": [
        {
          "field_id": "XXX-YY",
          "field_name": "Field Name",
          "issue": "Description of why this was difficult to map",
          "decision": "How you resolved it",
          "confidence": "high | medium | low"
        }
      ]
    },
    "recommendations": [
      "Specific suggestions for improving prompt or process",
      "Additional context that would have been helpful",
      "Gaps in foundation CDM structure that affected mapping"
    ],
    "quality_for_downstream": "This section is critical - quality issues here cascade to Steps 2c (Guardrails), 3 (Relationships), 4 (DDL), and 5 (Excel). Note any concerns about output quality."
  }
}
```

---

Generate the enhanced Foundational CDM JSON with inclusion of aligned NCPDP FIELDS.
"""
    
    return prompt
def run_step2b(
    config: AppConfig,
    foundation_cdm_file: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[dict]:
    """
    Step 2b: Add NCPDP refinement to foundation CDM
    
    Args:
        config: Configuration object with CDM description
        foundation_cdm_file: Path to foundation CDM JSON from Step 2a
        outdir: Output directory for enhanced CDM JSON
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
    
    Returns:
        Enhanced CDM dict (None in dry run)
    """
    
    print(f"  📖 Loading foundation CDM from: {foundation_cdm_file}")
    
    # Load foundation CDM
    with open(foundation_cdm_file, 'r', encoding='utf-8') as f:
        foundation_cdm = json.load(f)
    
    entity_count = len(foundation_cdm.get('entities', []))
    print(f"  📊 Found {entity_count} entities in foundation CDM")
    
    # Load NCPDP standards - prefer rationalized versions from Step 1d
    print(f"  📖 Loading NCPDP standards...")
    ncpdp_standards = {}
    
    # Check for rationalized NCPDP files from Step 1d (in prep directory)
    prep_outdir = outdir.parent / "prep"
    rationalized_general = None
    rationalized_script = None
    
    if prep_outdir.exists():
        # Look for most recent rationalized files
        general_files = sorted(prep_outdir.glob("rationalized_ncpdp_general_*.json"))
        script_files = sorted(prep_outdir.glob("rationalized_ncpdp_script_*.json"))
        
        if general_files:
            rationalized_general = general_files[-1]
            print(f"    📁 Found rationalized general NCPDP from Step 1d")
        
        if script_files:
            rationalized_script = script_files[-1]
            print(f"    📁 Found rationalized script NCPDP from Step 1d")
    
    if hasattr(config.inputs, 'ncpdp') and config.inputs.ncpdp:
        # Load general NCPDP standards (rationalized if available, else full)
        if 'general' in config.inputs.ncpdp:
            if rationalized_general:
                with open(rationalized_general, 'r', encoding='utf-8') as f:
                    general_data = json.load(f)
                field_count = len(general_data.get('fields', []))
                if field_count > 0:
                    ncpdp_standards['general'] = general_data
                    print(f"    ✓ Using rationalized general NCPDP ({field_count:,} fields)")
                else:
                    print(f"    ⚠️  Rationalized general NCPDP is empty - skipping")
            else:
                general_file = Path(config.inputs.ncpdp['general'])
                if general_file.exists():
                    with open(general_file, 'r', encoding='utf-8') as f:
                        general_data = json.load(f)
                    field_count = len(general_data.get('fields', []))
                    if field_count > 0:
                        ncpdp_standards['general'] = general_data
                        print(f"    ✓ Loaded full general NCPDP ({field_count:,} fields)")
                    else:
                        print(f"    ⚠️  General NCPDP is empty - skipping")
        
        # Load SCRIPT standards (rationalized if available, else full)
        if 'script' in config.inputs.ncpdp:
            if rationalized_script:
                with open(rationalized_script, 'r', encoding='utf-8') as f:
                    script_data = json.load(f)
                field_count = len(script_data.get('fields', []))
                if field_count > 0:
                    ncpdp_standards['script'] = script_data
                    print(f"    ✓ Using rationalized SCRIPT NCPDP ({field_count:,} fields)")
                else:
                    print(f"    ⚠️  Rationalized SCRIPT NCPDP is empty - skipping")
            else:
                script_file = Path(config.inputs.ncpdp['script'])
                if script_file.exists():
                    with open(script_file, 'r', encoding='utf-8') as f:
                        script_data = json.load(f)
                    field_count = len(script_data.get('fields', []))
                    if field_count > 0:
                        ncpdp_standards['script'] = script_data
                        print(f"    ✓ Loaded full SCRIPT standards ({field_count:,} fields)")
                    else:
                        print(f"    ⚠️  SCRIPT standards is empty - skipping")
    
    if not ncpdp_standards:
        print(f"  ⚠️  WARNING: No NCPDP standards found in config")
        print(f"     Step 2b will add known PBM fields without NCPDP crosswalk")
    
    # Build prompt
    prompt = build_prompt(config, foundation_cdm, ncpdp_standards)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"step2b_ncpdp_refinement_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"  ✓ Prompt saved: {output_file}")
        print(f"    Characters: {len(prompt):,}")
        print(f"    Tokens (est): {len(prompt) // 4:,}")
        return None
    
    # Live mode - call LLM
    print(f"  🤖 Calling LLM to enhance CDM with NCPDP mappings...")
    
    # Note: LLMClient has a bug - it stores max_tokens but doesn't use it in API call
    # For now relying on model's default output limit
    # TODO: Fix LLMClient to add max_tokens to kwargs, or pass it explicitly here
    
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
            feedback_file = outdir / f"step2b_feedback_{domain_safe}_{timestamp}.txt"
            
            with open(feedback_file, 'w', encoding='utf-8') as f:
                f.write(f"STEP 2B PROCESSING FEEDBACK\n")
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
                f.write(f"  Foundation CDM: {input_quality.get('foundation_cdm', 'not provided')}\n")
                f.write(f"  NCPDP Standards: {input_quality.get('ncpdp_standards', 'not provided')}\n")
                if input_quality.get('issues_encountered'):
                    f.write(f"  Issues:\n")
                    for issue in input_quality['issues_encountered']:
                        f.write(f"    - {issue}\n")
                f.write("\n")
                
                mapping_challenges = feedback.get('mapping_challenges', {})
                f.write(f"MAPPING CHALLENGES:\n")
                f.write(f"  Ambiguous fields: {mapping_challenges.get('ambiguous_fields', 0)}\n")
                f.write(f"  Difficult decisions: {mapping_challenges.get('difficult_decisions', 0)}\n")
                if mapping_challenges.get('examples'):
                    f.write(f"\n  Examples:\n")
                    for example in mapping_challenges['examples']:
                        f.write(f"\n  {example.get('field_id', 'N/A')} - {example.get('field_name', 'N/A')}\n")
                        f.write(f"    Issue: {example.get('issue', 'N/A')}\n")
                        f.write(f"    Decision: {example.get('decision', 'N/A')}\n")
                        f.write(f"    Confidence: {example.get('confidence', 'N/A')}\n")
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
        disposition_report = result.pop('ncpdp_disposition_report', None)
        if disposition_report:
            disp_file = outdir / f"ncpdp_disposition_report_{domain_safe}_{timestamp}.json"
            with open(disp_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "domain": config.cdm.domain,
                    "step": "2b",
                    "timestamp": timestamp,
                    "disposition_report": disposition_report
                }, f, indent=2)
            print(f"  📋 Disposition report saved: {disp_file}")
        else:
            print("  ⚠️  WARNING: Response missing 'ncpdp_disposition_report'")
        
        # Now result contains only clean CDM (entities + metadata)
        enhanced_cdm = result
        
        # Validate structure
        if 'entities' not in enhanced_cdm:
            print(f"  ❌ ERROR: Response missing 'entities' key")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_ncpdp_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm
        
        # Validate no data loss - CRITICAL
        foundation_entity_count = len(foundation_cdm.get('entities', []))
        enhanced_entity_count = len(enhanced_cdm.get('entities', []))
        
        if enhanced_entity_count < foundation_entity_count:
            print(f"  ❌ DATA LOSS: {enhanced_entity_count} entities vs {foundation_entity_count} expected")
            print(f"  ⚠️  LLM removed {foundation_entity_count - enhanced_entity_count} entities")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_ncpdp_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm
        
        foundation_attr_count = sum(len(e.get('attributes', [])) for e in foundation_cdm.get('entities', []))
        enhanced_attr_count = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        
        if enhanced_attr_count < foundation_attr_count:
            print(f"  ❌ DATA LOSS: {enhanced_attr_count} attributes vs {foundation_attr_count} expected")
            print(f"  ⚠️  LLM removed {foundation_attr_count - enhanced_attr_count} attributes")
            # Save anyway for debugging
            output_file = outdir / f"enhanced_cdm_ncpdp_{domain_safe}_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_cdm, f, indent=2)
            print(f"  💾 Output saved for review: {output_file}")
            return enhanced_cdm
        
        print(f"  ✓ Validation passed: No data loss detected")
        
        # Field accounting validation (if disposition report available)
        if disposition_report:
            total_ncpdp_fields = 0
            if ncpdp_standards.get('general'):
                total_ncpdp_fields += len(ncpdp_standards['general'].get('fields', []))
            if ncpdp_standards.get('script'):
                total_ncpdp_fields += len(ncpdp_standards['script'].get('fields', []))
            
            if total_ncpdp_fields > 0:
                field_accounting = disposition_report.get('field_accounting', {})
                if field_accounting:
                    accounted = field_accounting.get('total_accounted_for', 0)
                    if accounted != total_ncpdp_fields:
                        print(f"  ⚠️  WARNING: Field accounting incomplete")
                        print(f"     Total NCPDP fields: {total_ncpdp_fields}")
                        print(f"     Fields accounted for: {accounted}")
                        print(f"     Missing: {total_ncpdp_fields - accounted} fields")
                    else:
                        print(f"  ✓ Field accounting complete: {accounted}/{total_ncpdp_fields} fields")
                else:
                    print(f"  ⚠️  WARNING: No field_accounting section in disposition report")
        
        # Save clean enhanced CDM (without disposition report and feedback)
        output_file = outdir / f"enhanced_cdm_ncpdp_{domain_safe}_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_cdm, f, indent=2)
        
        print(f"  ✓ Clean CDM saved: {output_file}")
        
        # Report results
        entity_count = len(enhanced_cdm.get('entities', []))
        total_attrs = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        
        print(f"  📊 Output entities: {entity_count}")
        print(f"  📊 Output attributes: {total_attrs}")
        
        # Report disposition summary if available
        if disposition_report:
            disp = disposition_report.get('summary', {})
            if disp:
                print(f"\n  📋 NCPDP Disposition:")
                print(f"     Total NCPDP fields: {disp.get('total_ncpdp_fields_evaluated', 0)}")
                print(f"     Mapped to existing: {disp.get('mapped_to_existing_cdm', 0)}")
                print(f"     New attributes added: {disp.get('extension_attributes_added', 0)}")
                print(f"     New entities added: {disp.get('extension_entities_added', 0)}")
                print(f"     Not used: {disp.get('not_used', 0)}")
        
        return enhanced_cdm
        
    except json.JSONDecodeError as e:
        print(f"  ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"  Response preview: {response[:500]}...")
        
        # Save failed response for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"step2b_error_response_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"  💾 Full response saved to: {error_file}")
        
        return None