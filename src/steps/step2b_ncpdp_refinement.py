# src/steps/step2b_ncpdp_refinement.py
"""
Step 2b: NCPDP Refinement & Gap Analysis

Enhances foundation CDM from Step 2a by:
- Mapping NCPDP fields to existing CDM attributes
- Adding NCPDP-specific attributes when no FHIR equivalent exists
- Defining PK/FK relationships
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
    ncpdp_section = "### NCPDP STANDARDS\n\n"
    
    if ncpdp_standards.get('general'):
        ncpdp_section += """**General Standards (NCPDP Telecom D.0):**
These are real-time pharmacy claims transaction standards used for adjudication.

Key areas:
- **Claims Processing:** Claim submission, reversals, billing
- **Pricing:** Ingredient cost, dispensing fees, tax, MAC pricing
- **Patient Information:** Member/cardholder demographics, coverage details
- **Prescriber Information:** Provider identifiers, DEA numbers
- **DUR/PPS:** Drug utilization review codes, professional service codes
- **Prior Authorization:** PA numbers, submission clarification codes
- **Compound Drugs:** Compound indicators, ingredient details
- **DAW Codes:** Dispense as written / product selection codes

These fields are CRITICAL for PBM adjudication, pricing engines, and billing reconciliation.

"""
        ncpdp_section += f"```json\n{json.dumps(ncpdp_standards['general'], indent=2)}\n```\n\n"
    else:
        ncpdp_section += "**General Standards:** Not provided\n\n"
    
    if ncpdp_standards.get('script'):
        ncpdp_section += """**SCRIPT Standards (NCPDP SCRIPT - ePrescribing XML):**
These are XML-based ePrescribing messages between providers, pharmacies, and payers.

Key message types:
- **NewRx:** New prescription from provider to pharmacy
- **RefillRequest:** Pharmacy requests refill authorization
- **RxChange:** Prescription modifications
- **CancelRx:** Prescription cancellation
- **RxFill:** Dispensing notification

These are used for prescription routing and prior authorization workflows, NOT claims adjudication.

Focus on SCRIPT fields that relate to:
- Benefit verification
- Prior authorization requirements
- Formulary status
- Plan-level restrictions

"""
        ncpdp_section += f"```json\n{json.dumps(ncpdp_standards['script'], indent=2)}\n```\n\n"
    else:
        ncpdp_section += "**SCRIPT Standards:** Not provided\n\n"
    
    if not ncpdp_standards.get('general') and not ncpdp_standards.get('script'):
        ncpdp_section += """**⚠️ NO NCPDP STANDARDS PROVIDED**

You will need to:
1. Define Primary Keys and Foreign Keys based on FHIR structure
2. Add common PBM/pharmacy fields you know should exist (DAW, MAC, DUR codes)
3. Note in disposition report that comprehensive NCPDP mapping requires standards input

Proceed with PK/FK definitions and add well-known pharmacy-specific fields.

"""
    
    prompt = f"""You are an expert healthcare data modeler specializing in PBM, FHIR, and NCPDP standards.

## CDM CONTEXT

**Domain:** {config.cdm.domain}

**Description:** {config.cdm.description}

## YOUR TASK

Enhance the FHIR-based CDM foundation with NCPDP D.0 mappings and identify gaps.

**DEFAULT BIAS: MAP TO EXISTING CDM**
The burden of proof is on ADDING new attributes/entities, not on mapping. Preserve the FHIR foundation.

This is **Step 2b: NCPDP Refinement** - you are:
1. Mapping NCPDP fields to existing CDM attributes
2. Adding NCPDP-specific attributes ONLY when justified
3. Defining Primary Keys and Foreign Keys
4. Generating complete disposition report for ALL NCPDP fields

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

### Step 4: Classify disposition (REQUIRED for ALL NCPDP fields)

Every NCPDP field must be one of:
1. **mapped** - Direct mapping to existing CDM attribute
2. **transformed** - Derived/combined from existing CDM attributes
3. **extension_attribute** - New attribute added to existing entity
4. **extension_entity** - New entity created
5. **not_used** - Field not needed (with business justification)

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
  
  "relationships": [
    {{
      "parent_entity": "InsuranceProduct",
      "child_entity": "InsurancePlan",
      "relationship_type": "one_to_many",
      "description": "One product can have multiple plans"
    }}
  ],
  
  "business_capabilities": [],
  
  "ncpdp_disposition_report": {{
    "summary": {{
      "total_ncpdp_fields_evaluated": 0,
      "mapped_to_existing_cdm": 0,
      "mapped_via_transformation": 0,
      "extension_attributes_added": 0,
      "extension_entities_added": 0,
      "not_used": 0
    }},
    "details": [
      {{
        "ncpdp_field": "AM07.Plan_ID",
        "disposition": "mapped",
        "cdm_target": "InsurancePlan.plan_identifier_value",
        "mapping_type": "direct",
        "notes": null
      }},
      {{
        "ncpdp_field": "420-DK.DAW_Product_Selection_Code",
        "disposition": "extension_attribute",
        "cdm_target": "Coverage.dispense_as_written_code",
        "mapping_type": "new_attribute",
        "justification": "Pharmacy-specific, no FHIR equivalent, required for adjudication"
      }},
      {{
        "ncpdp_field": "401-D1.Prescriber_ID",
        "disposition": "not_used",
        "cdm_target": null,
        "mapping_type": null,
        "justification": "Prescriber entity outside Plan & Benefit CDM scope"
      }}
    ]
  }}
}}
```

---

## CRITICAL REQUIREMENTS

1. **Map FIRST** - Try to map every NCPDP field to existing CDM before adding new
2. **Justify additions** - Every new attribute/entity needs origin.justification explaining why mapping impossible
3. **Complete disposition** - Every NCPDP field in the standard must have disposition classification
4. **Define PK/FK** - Every entity needs primary_key and foreign_keys (if applicable) - EVEN IF NO NCPDP FILES PROVIDED
5. **Preserve origin** - Keep all origin tracking from Step 2a, add for new fields
6. **Output ONLY valid JSON** - No markdown, no code blocks, no commentary

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
    
    # Load NCPDP standards
    print(f"  📖 Loading NCPDP standards...")
    ncpdp_standards = {}
    
    if hasattr(config.inputs, 'ncpdp') and config.inputs.ncpdp:
        # Load general NCPDP standards
        if 'general' in config.inputs.ncpdp:
            general_file = Path(config.inputs.ncpdp['general'])
            if general_file.exists():
                with open(general_file, 'r', encoding='utf-8') as f:
                    ncpdp_standards['general'] = json.load(f)
                print(f"    ✓ Loaded general NCPDP standards")
        
        # Load SCRIPT standards
        if 'script' in config.inputs.ncpdp:
            script_file = Path(config.inputs.ncpdp['script'])
            if script_file.exists():
                with open(script_file, 'r', encoding='utf-8') as f:
                    ncpdp_standards['script'] = json.load(f)
                print(f"    ✓ Loaded SCRIPT standards")
    
    if not ncpdp_standards:
        print(f"  ⚠️  WARNING: No NCPDP standards found in config")
        print(f"     Step 2b will focus on PK/FK definitions and add known PBM fields")
    
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