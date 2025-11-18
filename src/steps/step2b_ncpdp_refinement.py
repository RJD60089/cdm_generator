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
    
    # Build NCPDP section with context
    ncpdp_section = "### NCPDP STANDARDS (Flattened Format)\n\n"
    
    if ncpdp_standards.get('general'):
        ncpdp_section += """**General Standards (NCPDP Telecom D.0 + Others):**
This is a DEDUPLICATED, flattened list of NCPDP fields where each field appears ONCE.

**Structure:**
- `_columns`: Explains what each abbreviated key means (i=FIELD, n=NAME, d=DEFINITION, etc.)
- `_standards`: Lookup table for standard codes (T=Telecommunication, A=Post Adjudication, etc.)
- `fields`: Array of 1,140+ unique NCPDP fields
  - Each field has `s` property listing which standards use it (e.g., "T,A" = used in Telecom & Post Adjudication)

**Key areas covered:**
- **Claims Processing:** Claim submission, reversals, billing
- **Pricing:** Ingredient cost, dispensing fees, tax, MAC pricing
- **Patient Information:** Member/cardholder demographics, coverage details
- **Prescriber Information:** Provider identifiers, DEA numbers
- **DUR/PPS:** Drug utilization review codes, professional service codes
- **Prior Authorization:** PA numbers, submission clarification codes
- **Compound Drugs:** Compound indicators, ingredient details
- **DAW Codes:** Dispense as written / product selection codes

**CRITICAL (SCOPED):**
Focus on NCPDP fields that are relevant to Plan & Benefit:
- Eligibility / coverage / member-card info
- Plan identifiers, group identifiers, BIN/PCN
- Pricing, cost-share, accumulators, benefit stages (deductible, gap, catastrophic)
- Formulary status, prior authorization, step therapy indicators
- Quantity limits, day supply limits
- Network tier codes, plan limitations

For obviously out-of-scope fields (e.g., prescriber-specific identifiers, clinical measurement fields, detailed DUR codes, message header/plumbing fields):
- You MAY handle them via a summarized rule in the disposition report instead of one-by-one
- Example: "All prescriber-only identifier fields (401-D1, 411-DB, etc.) are not_used for Plan & Benefit CDM"

"""
        ncpdp_section += f"```json\n{json.dumps(ncpdp_standards['general'], indent=2)}\n```\n\n"
    else:
        ncpdp_section += "**General Standards:** Not provided\n\n"
    
    if ncpdp_standards.get('script'):
        ncpdp_section += """**SCRIPT Standards (NCPDP SCRIPT - ePrescribing XML):**
These are XML-based ePrescribing messages between providers, pharmacies, and payers.

**Key message types:**
- **NewRx:** New prescription from provider to pharmacy
- **RefillRequest:** Pharmacy requests refill authorization
- **RxChange:** Prescription modifications
- **CancelRx:** Prescription cancellation
- **RxFill:** Dispensing notification

These are used for prescription routing and prior authorization workflows, NOT claims adjudication.

**CRITICAL (SCOPED):**
Focus on SCRIPT fields that relate to Plan & Benefit:
- Benefit verification indicators
- Prior authorization requirements and status
- Formulary status and restrictions
- Plan-level restrictions (quantity limits, step therapy)
- Coverage determination fields

For message header fields, routing fields, or provider-specific fields that don't affect Plan & Benefit structure:
- You MAY group these as out-of-scope in the disposition report

"""
        ncpdp_section += f"```json\n{json.dumps(ncpdp_standards['script'], indent=2)}\n```\n\n"
    else:
        ncpdp_section += "**SCRIPT Standards:** Not provided\n\n"
    
    if not ncpdp_standards.get('general') and not ncpdp_standards.get('script'):
        ncpdp_section += """**⚠️ NO NCPDP STANDARDS PROVIDED**

You will need to:
1. Add common PBM/pharmacy fields you know should exist (DAW, MAC, DUR codes)
2. Note in disposition report that comprehensive NCPDP mapping requires standards input

Proceed with adding well-known pharmacy-specific fields.

"""
    
    prompt = f"""You are an expert healthcare data modeler specializing in PBM, FHIR, and NCPDP standards.

## ⚠️ PRESERVE THE FOUNDATION CDM STRUCTURE

**YOU MUST PRESERVE THE FOUNDATION CDM:**
- **EVERY entity** from the foundation CDM input below must appear in your output
- **EVERY attribute** from the foundation CDM input must appear in your output
- You may REORDER entities/attributes, but do not REMOVE them
- **DO NOT SUMMARIZE** or condense the CDM structure itself

**YOUR TASK IS TO ADD, NOT SUBTRACT:**
- ADD NCPDP mappings to existing attributes (in source_mappings.ncpdp)
- ADD new NCPDP-specific attributes where justified
- ADD new entities ONLY if absolutely necessary
- PRESERVE everything from the foundation CDM

**If you approach response size limits:**
- PRIORITIZE keeping the CDM entities/attributes intact
- You MAY summarize some NCPDP dispositions (see below) rather than listing each one individually
- DO NOT return an artificial error; return the best complete CDM JSON you can within limits

---

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Enhance the FHIR-based CDM foundation by mapping NCPDP fields and identifying gaps.

**DEFAULT BIAS: MAP TO EXISTING CDM**
The burden of proof is on ADDING new attributes/entities, not on mapping. Preserve the FHIR foundation.

This is **Step 2b: NCPDP Refinement** - you are:
1. Mapping NCPDP fields to existing CDM attributes (add to source_mappings.ncpdp)
2. Adding NCPDP-specific attributes ONLY when justified and no CDM equivalent exists
3. Generating disposition report: detailed for in-scope fields, grouped rules for out-of-scope
4. **NEVER removing or omitting any existing entities or attributes**

---

## INPUTS

### FOUNDATION CDM (from Step 2a)

{json.dumps(foundation_cdm, indent=2)}

---

{ncpdp_section}

---

## DECISION FRAMEWORK

### Step 1: Can it map to existing FHIR CDM?

**If YES** → MAP IT (no CDM change)
- Same business concept, different terminology
- Example: NCPDP "Plan_ID" → CDM "plan_identifier_value"
- Action: Add NCPDP mapping to existing attribute's source_mappings

**If NO** → Proceed to Step 2

### Step 2: Is it materially important to PBM operations?

Ask: Does this field impact:
- Pricing engines (MAC, AWP, WAC)?
- Adjudication decisions?
- Accumulators (deductible, OOP)?
- Cost-share calculations?
- Network reimbursement logic?
- Benefit determination?

**If NO** → Mark as "not_used" with justification
**If YES** → Proceed to Step 3

### Step 3: Will it be reused across multiple entities/domains?

**If NO** → Add as extension attribute on single entity
**If YES** → Consider new standalone attribute/entity

### Step 4: Classify disposition

Every IN-SCOPE NCPDP field (Plan & Benefit relevant) must be one of:
1. **mapped** - Direct mapping to existing CDM attribute
2. **transformed** - Derived/combined from existing CDM attributes
3. **extension_attribute** - New attribute added to existing entity
4. **extension_entity** - New entity created
5. **not_used** - Field not needed (with business justification)

For clearly OUT-OF-SCOPE fields, you MAY:
- Group them into a summarized disposition rule
- Example: "All SCRIPT message-header fields (MessageID, SentTime, etc.) are not_used for Plan & Benefit CDM"
- Document the grouping in the disposition report summary

---

## DO NOT DUPLICATE THESE FHIR STRUCTURES

These already exist in the CDM - extend them, don't recreate:
- Coverage, CoverageClassification, CoverageMemberCostShare
- InsurancePlan, PlanSpecificCost, PlanGeneralCost
- InsuranceProduct, ProductCoverageBenefit
- All entities in the foundation CDM input above

If NCPDP needs something close to these, map to them or add attributes to them.

---

## ADD NEW ATTRIBUTE - ALL CONDITIONS REQUIRED

Only add new attribute if ALL are true:
☑ No existing FHIR CDM element can express it
☑ Materially important to PBM operations (see Step 2 above)
☑ Reused across multiple entities/domains OR critical single-entity field
☑ Improves long-term interoperability

If any condition fails → Map to existing or mark not_used

---

## ADD NEW ENTITY - ALL CONDITIONS REQUIRED

Only add new entity if ALL are true:
☑ No FHIR resource/backbone/extension could represent it
☑ Stable domain object (not transient feed structure)
☑ Required by multiple standards (NCPDP + X12 + internal systems)
☑ Core to PBM functions (pricing, accumulators, benefits, eligibility)
☑ Has 5+ related attributes that form cohesive object

If any condition fails → Extend existing entity or map

---

## PBM-SPECIFIC NCPDP FIELDS (High Priority for Addition)

These pharmacy-specific codes typically have NO FHIR equivalent and should be ADDED if missing from CDM:

**From Telecom D.0 (Adjudication/Claims):**
- DAW (Dispense As Written) codes - Product selection
- Compound drug indicators
- Submission clarification codes
- DUR/PPS (Drug Utilization Review) codes
- Product selection codes
- MAC (Maximum Allowable Cost) identifiers
- Pricing basis codes (AWP, WAC, MAC, NADAC)
- Basis of reimbursement codes
- Usual & Customary charge
- Ingredient cost submitted
- Pharmacy network tier codes
- Benefit stage indicators (deductible, gap, catastrophic)
- Plan limitations (quantity limits, day supply limits)

**From SCRIPT (ePrescribing):**
- Prior authorization requirement flags
- Formulary status codes
- Step therapy requirements
- Quantity limit indicators

**Add these if:**
1. They appear in the NCPDP standards provided
2. They don't already exist in the CDM
3. They're relevant to Plan & Benefit domain

---

## PRIMARY KEY & FOREIGN KEY DEFINITIONS

For each entity, define:

**Primary Key:**
- Natural key (business identifier) OR
- Surrogate key (system-generated)
- Example: InsurancePlan PK = plan_identifier_value (natural) or plan_id (surrogate)

**Foreign Keys:**
- Reference fields ending in _id
- Example: InsurancePlan.insurance_product_id → InsuranceProduct.product_id

**Constraints:**
- NOT NULL for PKs
- NOT NULL for required business fields
- CHECK constraints for valid value ranges

**CRITICAL:** Even if no NCPDP standards provided, you MUST define PK/FK based on FHIR structure and entity relationships.

---

## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "cdm_metadata": {{
    "domain": "{config.cdm.domain}",
    "version": "1.0",
    "description": "{config.cdm.description}",
    "foundation_standard": "FHIR",
    "generation_timestamp": "{datetime.now().isoformat()}",
    "generation_steps_completed": ["2a", "2b"]
  }},
  
  "entities": [
    {{
      "entity_name": "InsurancePlan",
      "classification": "Core",
      "business_definition": "...",
      "business_context": "...",
      "key_business_questions": ["..."],
      "fhir_source_entity": "InsurancePlan",
      
      "primary_key": {{
        "type": "natural",
        "attributes": ["plan_identifier_value"]
      }},
      
      "foreign_keys": [
        {{
          "name": "fk_plan_product",
          "attributes": ["insurance_product_id"],
          "references_entity": "InsuranceProduct",
          "references_attributes": ["product_identifier_value"],
          "on_delete": "RESTRICT",
          "on_update": "CASCADE"
        }}
      ],
      
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
            "source_file": "insuranceplan.profile.json"
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
            "guardrails": null,
            "glue": null
          }}
        }},
        {{
          "canonical_column": "dispense_as_written_code",
          "source_column": "DISPENSE_AS_WRITTEN_CODE",
          "data_type": "VARCHAR",
          "size": 2,
          "nullable": true,
          "glossary_term": "DAW code indicating prescriber intent for substitution...",
          "business_context": "Required for adjudication to determine if generic substitution allowed...",
          "classification": "Operational",
          
          "origin": {{
            "standard": "ncpdp",
            "created_in_step": "2b",
            "source_path": "D.0.420-DK.DAW_Product_Selection_Code",
            "source_file": "ncpdp_general.json",
            "justification": "Pharmacy-specific code with no FHIR equivalent, critical for adjudication and pricing"
          }},
          
          "source_mappings": {{
            "fhir": null,
            "ncpdp": {{
              "disposition": "extension_attribute",
              "standard": "D.0",
              "segment": "420-DK",
              "field": "DAW_Product_Selection_Code",
              "data_type": "N",
              "max_length": 1,
              "added_in_step": "2b",
              "valid_values": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
            }},
            "guardrails": null,
            "glue": null
          }}
        }}
      ]
    }}
  ],
  
  "business_capabilities": [],
  
  "ncpdp_disposition_report": {{
    "summary": {{
      "total_ncpdp_fields_evaluated": 0,
      "in_scope_fields_detailed": 0,
      "out_of_scope_fields_grouped": 0,
      "mapped_to_existing_cdm": 0,
      "mapped_via_transformation": 0,
      "extension_attributes_added": 0,
      "extension_entities_added": 0,
      "not_used": 0
    }},
    "field_accounting": {{
      "total_input_fields": 0,
      "detailed_disposition_count": 0,
      "grouped_disposition_count": 0,
      "total_accounted_for": 0,
      "accounting_complete": true,
      "note": "Must equal 100%: total_input_fields = detailed + grouped"
    }},
    "details": [
      {{
        "ncpdp_field": "302-C2.Cardholder_ID",
        "disposition": "mapped",
        "cdm_target": "Coverage.subscriber_id",
        "mapping_type": "direct",
        "notes": "Plan & Benefit relevant - member identification"
      }},
      {{
        "ncpdp_field": "512-FC.Accumulated_Deductible_Amount",
        "disposition": "extension_attribute",
        "cdm_target": "Coverage.deductible_accumulated",
        "mapping_type": "new_attribute",
        "justification": "Pharmacy-specific accumulator, no FHIR equivalent, required for benefit stage tracking"
      }},
      {{
        "disposition_group": "prescriber_identifiers",
        "disposition": "not_used",
        "fields_count": 15,
        "example_fields": ["401-D1.Prescriber_ID", "411-DB.Prescriber_Last_Name", "427-DR.DEA_Number"],
        "justification": "Prescriber entity outside Plan & Benefit CDM scope - belongs in Prescriber CDM"
      }},
      {{
        "disposition_group": "dur_clinical_details",
        "disposition": "not_used",
        "fields_count": 25,
        "example_fields": ["473-7E.DUR_Service_Reason_Code", "439-E4.Reason_For_Service_Code"],
        "justification": "Clinical DUR details out of scope for Plan & Benefit - belongs in Utilization Management CDM"
      }}
    ]
  }}
}}
```

---

## CRITICAL REQUIREMENTS

1. **PRESERVE EVERYTHING** - Every entity and attribute from foundation CDM must appear in output
2. **Focus on in-scope fields** - Prioritize Plan & Benefit relevant NCPDP fields for detailed disposition
3. **Map FIRST** - Try to map every relevant NCPDP field to existing CDM before adding new
4. **Justify additions** - Every new attribute/entity needs origin.justification explaining why mapping impossible
5. **ACCOUNT FOR ALL FIELDS** - You MUST account for 100% of NCPDP fields:
   - Count total NCPDP fields in the input
   - Every field must have EITHER detailed disposition OR be part of a grouped rule
   - Provide field_accounting reconciliation showing: total_input_fields = detailed + grouped
   - If your accounting does NOT equal 100%, you have FAILED the requirement
6. **Disposition requirements:**
   - Every IN-SCOPE NCPDP field (Plan & Benefit relevant) must have a disposition classification
   - For OUT-OF-SCOPE fields, you MAY:
     - Define one or more grouped rules (e.g., "All prescriber-only identifiers = not_used for Plan & Benefit CDM")
     - You do NOT need to list every one individually
7. **Origin requirements:**
   - Preserve origin for all existing CDM attributes
   - Add origin for new attributes/entities
   - For grouped or summarized dispositions, it is sufficient to describe the rule, not each field
8. **Output ONLY valid JSON** - No markdown, no code blocks, no commentary

---

Generate the enhanced CDM JSON with NCPDP crosswalk now.
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
        
        enhanced_cdm = json.loads(response_clean)
        
        # Validate structure
        if 'entities' not in enhanced_cdm:
            raise ValueError("Response missing 'entities' key")
        if 'ncpdp_disposition_report' not in enhanced_cdm:
            print("  ⚠️  WARNING: Response missing 'ncpdp_disposition_report'")
        
        # Validate no data loss - CRITICAL
        foundation_entity_count = len(foundation_cdm.get('entities', []))
        enhanced_entity_count = len(enhanced_cdm.get('entities', []))
        
        if enhanced_entity_count < foundation_entity_count:
            raise ValueError(
                f"❌ DATA LOSS DETECTED: Enhanced CDM has {enhanced_entity_count} entities "
                f"but foundation had {foundation_entity_count}. "
                f"LLM removed {foundation_entity_count - enhanced_entity_count} entities. REJECTING OUTPUT."
            )
        
        foundation_attr_count = sum(len(e.get('attributes', [])) for e in foundation_cdm.get('entities', []))
        enhanced_attr_count = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        
        if enhanced_attr_count < foundation_attr_count:
            raise ValueError(
                f"❌ DATA LOSS DETECTED: Enhanced CDM has {enhanced_attr_count} attributes "
                f"but foundation had {foundation_attr_count}. "
                f"LLM removed {foundation_attr_count - enhanced_attr_count} attributes. REJECTING OUTPUT."
            )
        
        # Validate field accounting - CRITICAL
        total_ncpdp_fields = 0
        if ncpdp_standards.get('general'):
            total_ncpdp_fields += len(ncpdp_standards['general'].get('fields', []))
        if ncpdp_standards.get('script'):
            total_ncpdp_fields += len(ncpdp_standards['script'].get('fields', []))
        
        if total_ncpdp_fields > 0:
            field_accounting = enhanced_cdm.get('ncpdp_disposition_report', {}).get('field_accounting', {})
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
        
        print(f"  ✓ Validation passed: No data loss detected")
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.replace(' ', '_')
        output_file = outdir / f"enhanced_cdm_ncpdp_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_cdm, f, indent=2)
        
        entity_count = len(enhanced_cdm.get('entities', []))
        total_attrs = sum(len(e.get('attributes', [])) for e in enhanced_cdm.get('entities', []))
        
        # Report disposition summary
        disp = enhanced_cdm.get('ncpdp_disposition_report', {}).get('summary', {})
        
        print(f"  ✓ Enhanced CDM generated")
        print(f"  📁 Output: {output_file}")
        print(f"  📊 Entities: {entity_count}")
        print(f"  📊 Total attributes: {total_attrs}")
        
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
        
        raise
    except ValueError as e:
        print(f"  ❌ ERROR: {e}")
        raise