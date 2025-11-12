"""
Step 0: Input Consolidation
Consolidate FHIR files separately from Guardrails files.
Creates two outputs: consolidated_fhir.json and consolidated_guardrails.json
"""
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from src.core.llm_client import LLMClient
from src.core.run_state import RunState


def run_step0(
    domain: str,
    inputs_json: Dict[str, List[Dict[str, str]]],
    llm: LLMClient,
    outdir: str
) -> Tuple[Optional[RunState], Optional[RunState]]:
    """
    Step 0: Consolidate FHIR and Guardrails files separately.
    
    Args:
        domain: CDM domain name
        inputs_json: Dictionary with 'fhir' and 'guardrails' lists
        llm: LLM client instance
        outdir: Output directory
        
    Returns:
        Tuple of (fhir_state, guardrails_state)
    """
    print("\n=== Step 0: Input Consolidation ===")
    
    fhir_state = None
    guardrails_state = None
    
    # Step 0a: Consolidate FHIR files
    if 'fhir' in inputs_json and inputs_json['fhir']:
        print(f"\nStep 0a: Consolidating {len(inputs_json['fhir'])} FHIR file(s)...")
        fhir_state = _consolidate_fhir(domain, inputs_json['fhir'], llm, outdir)
        print(f"  ✓ FHIR consolidation complete: {fhir_state.output_file}")
    
    # Step 0b: Consolidate Guardrails files
    if 'guardrails' in inputs_json and inputs_json['guardrails']:
        print(f"\nStep 0b: Consolidating {len(inputs_json['guardrails'])} Guardrails file(s)...")
        guardrails_state = _consolidate_guardrails(domain, inputs_json['guardrails'], llm, outdir)
        print(f"  ✓ Guardrails consolidation complete: {guardrails_state.output_file}")
    
    return fhir_state, guardrails_state


def _consolidate_fhir(
    domain: str,
    fhir_files: List[Dict[str, str]],
    llm: LLMClient,
    outdir: str
) -> RunState:
    """Consolidate multiple FHIR files into one."""
    
    prompt = _build_fhir_consolidation_prompt(domain, fhir_files)
    
    print(f"  Calling LLM for FHIR consolidation...")
    response = llm.call(prompt)
    
    consolidated_json = _extract_json(response)
    
    # Save consolidated FHIR
    output_file = Path(outdir) / f"step0a_consolidated_fhir_{domain.replace(' ', '_')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(consolidated_json, f, indent=2)
    
    state = RunState(
        domain=domain,
        step="0a",
        prompt=prompt,
        response=json.dumps(consolidated_json, indent=2),
        output_file=str(output_file),
        metadata={
            'source': 'fhir',
            'files_processed': len(fhir_files),
            'resources_count': len(consolidated_json.get('resources', []))
        }
    )
    
    return state


def _consolidate_guardrails(
    domain: str,
    guardrails_files: List[Dict[str, str]],
    llm: LLMClient,
    outdir: str
) -> RunState:
    """Consolidate multiple Guardrails files into one."""
    
    prompt = _build_guardrails_consolidation_prompt(domain, guardrails_files)
    
    print(f"  Calling LLM for Guardrails consolidation...")
    response = llm.call(prompt)
    
    consolidated_json = _extract_json(response)
    
    # Save consolidated Guardrails
    output_file = Path(outdir) / f"step0b_consolidated_guardrails_{domain.replace(' ', '_')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(consolidated_json, f, indent=2)
    
    state = RunState(
        domain=domain,
        step="0b",
        prompt=prompt,
        response=json.dumps(consolidated_json, indent=2),
        output_file=str(output_file),
        metadata={
            'source': 'guardrails',
            'files_processed': len(guardrails_files),
            'entities_count': len(consolidated_json.get('entities', []))
        }
    )
    
    return state


def _build_fhir_consolidation_prompt(domain: str, fhir_files: List[Dict[str, str]]) -> str:
    """Build prompt for consolidating FHIR files only."""
    
    prompt = f"""You are a FHIR expert consolidating multiple FHIR resource profiles for a PBM CDM.

**Domain:** {domain}

**Reasoning Instructions:**
- Think creatively about how different FHIR resources relate to each other
- Consider multiple interpretations of element definitions
- Identify non-obvious relationships between resources
- Propose alternative attribute names where appropriate
- Question assumptions and suggest improvements

**Your Task:**
Analyze the {len(fhir_files)} FHIR profile files provided and produce a SINGLE consolidated JSON that:

1. **Identifies all FHIR resources** (InsurancePlan, Coverage, etc.)
2. **Extracts all elements** from each resource with paths, types, and cardinality
3. **Documents relationships** between resources (references)
4. **Preserves FHIR metadata** (descriptions, bindings, constraints)
5. **Maintains FHIR terminology** (use FHIR element names as-is)

**Output Format:**
```json
{{
  "domain": "{domain}",
  "fhir_version": "R4",
  "resources": [
    {{
      "resource_type": "InsurancePlan",
      "profile_url": "http://hl7.org/fhir/StructureDefinition/InsurancePlan",
      "description": "Details of a Health Insurance product/plan provided by an organization",
      "source_files": ["FHIR_insuranceplan.profile.json", "FHIR_insuranceplan-example.json"],
      "elements": [
        {{
          "path": "InsurancePlan.identifier",
          "cardinality": "0..*",
          "type": "Identifier",
          "short": "Business identifier for the plan",
          "definition": "Business identifiers assigned to this health insurance plan...",
          "example_value": {{"system": "http://example.org/plans", "value": "12345"}}
        }},
        {{
          "path": "InsurancePlan.status",
          "cardinality": "0..1",
          "type": "code",
          "binding": "PublicationStatus",
          "required": false,
          "possible_values": ["draft", "active", "retired", "unknown"]
        }},
        {{
          "path": "InsurancePlan.type",
          "cardinality": "0..*",
          "type": "CodeableConcept",
          "short": "Type of plan"
        }},
        {{
          "path": "InsurancePlan.coverage",
          "cardinality": "0..*",
          "type": "BackboneElement",
          "description": "Details about the coverage offered by the insurance product"
        }}
      ],
      "references": [
        {{
          "path": "InsurancePlan.ownedBy",
          "target_resource": "Organization",
          "cardinality": "0..1"
        }},
        {{
          "path": "InsurancePlan.administeredBy",
          "target_resource": "Organization",
          "cardinality": "0..1"
        }}
      ]
    }}
  ],
  "resource_relationships": [
    {{
      "from_resource": "Coverage",
      "from_path": "Coverage.insurance.coverage",
      "to_resource": "InsurancePlan",
      "relationship": "many-to-one"
    }}
  ]
}}
```

---

## FHIR Profile Files

"""
    
    for i, fhir_data in enumerate(fhir_files, 1):
        prompt += f"### FHIR File {i}: {fhir_data['filename']}\n\n```json\n{fhir_data['content']}\n```\n\n"
    
    prompt += """
---

**INSTRUCTIONS:**

1. **Extract all FHIR resources** across all profile and example files
2. **Consolidate elements** - if same resource appears in multiple files, merge element definitions
3. **Preserve FHIR paths** - use standard FHIR dot notation (e.g., InsurancePlan.identifier)
4. **Document cardinality** - capture min..max (0..1, 1..1, 0..*, etc.)
5. **Extract examples** - use example files to show real data values
6. **Map references** - identify all Resource references between FHIR resources
7. **Keep FHIR terminology** - don't translate to business terms (that happens in Step 1)

**Output ONLY valid JSON.** No markdown, no explanations.

Generate the consolidated FHIR JSON now.
"""
    
    return prompt


def _build_guardrails_consolidation_prompt(domain: str, guardrails_files: List[Dict[str, str]]) -> str:
    """Build prompt for consolidating Guardrails files only."""
    
    prompt = f"""You are a business analyst consolidating multiple API specifications and business requirements for a PBM CDM.

**Domain:** {domain}

**Reasoning Instructions:**
- Think creatively about entity relationships and business rules
- Consider alternative interpretations of business requirements
- Identify implicit requirements not explicitly stated
- Question assumptions and propose improvements
- Consider edge cases and special scenarios

**Your Task:**
Analyze the {len(guardrails_files)} Guardrails specification files and produce a SINGLE consolidated JSON that:

1. **Identifies all business entities** across all API specifications
2. **Consolidates field definitions** for each entity
3. **Extracts business rules** and validation requirements
4. **Documents API contracts** and data governance requirements
5. **Uses business terminology** (as defined in specifications)

**Output Format:**
```json
{{
  "domain": "{domain}",
  "source": "guardrails",
  "entities": [
    {{
      "entity_name": "Plan",
      "api_context": "Hierarchy API, Benefit Setup API",
      "description": "Insurance plan product definition",
      "source_files": ["GR_Navitus_DGBee_v1_5_Plan_and_Benefit.xlsx", "GR_Hierarchy_Gen1_API.xlsx"],
      "attributes": [
        {{
          "field_name": "PlanID",
          "data_type": "string",
          "required": true,
          "max_length": 50,
          "description": "Unique identifier for the insurance plan",
          "api_endpoint": "GET /hierarchy/plans",
          "validation_rules": ["Required", "Unique", "Alphanumeric"],
          "source_file": "GR_Hierarchy_Gen1_API.xlsx"
        }},
        {{
          "field_name": "PlanName",
          "data_type": "string",
          "required": true,
          "max_length": 200,
          "description": "Marketing name of the plan",
          "api_endpoint": "GET /hierarchy/plans"
        }},
        {{
          "field_name": "PlanType",
          "data_type": "code",
          "required": true,
          "allowed_values": ["Medical", "Pharmacy", "Dental", "Vision"],
          "description": "Type of insurance plan"
        }}
      ],
      "business_rules": [
        {{
          "rule": "Plan must have at least one active benefit package",
          "source": "GR_BenefitSetupSvc_Gen1_API.xlsx"
        }}
      ],
      "relationships": [
        {{
          "related_entity": "BenefitPackage",
          "relationship_type": "one-to-many",
          "foreign_key": "PlanID"
        }}
      ]
    }}
  ],
  "apis": [
    {{
      "api_name": "Hierarchy API",
      "version": "v1.5",
      "endpoints": [
        {{
          "path": "GET /hierarchy/plans",
          "returns": "Plan[]"
        }}
      ],
      "source_file": "GR_Navitus_DGBee_v1_5_Plan_and_Benefit.xlsx"
    }}
  ],
  "governance": [
    {{
      "requirement": "All plan data must be audit logged",
      "source": "GR_P&B_APIs_Data_Governance.xlsx"
    }}
  ]
}}
```

---

## Guardrails Specification Files

"""
    
    for i, gr_data in enumerate(guardrails_files, 1):
        prompt += f"### Guardrails File {i}: {gr_data['filename']}\n\n```json\n{gr_data['content']}\n```\n\n"
    
    prompt += """
---

**INSTRUCTIONS:**

1. **Extract all business entities** from all API specifications
2. **Consolidate field definitions** - if same entity appears in multiple APIs, merge definitions
3. **Preserve business terminology** - use the field names as defined in specs
4. **Extract validation rules** - required fields, allowed values, formats, etc.
5. **Document business rules** - constraints, calculations, dependencies
6. **Map API endpoints** - which entities are exposed through which APIs
7. **Include governance requirements** - audit, security, data retention policies
8. **Note relationships** - how entities relate to each other

**Output ONLY valid JSON.** No markdown, no explanations.

Generate the consolidated Guardrails JSON now.
"""
    
    return prompt


def _extract_json(response: str) -> dict:
    """
    Extract JSON from LLM response.
    Handles cases where JSON is wrapped in markdown code blocks.
    """
    cleaned = response.strip()
    
    # Remove markdown code blocks if present
    if cleaned.startswith('```'):
        lines = cleaned.split('\n')
        lines = lines[1:]  # Remove first line
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]  # Remove last line
        cleaned = '\n'.join(lines)
    
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON from LLM response: {e}")
        print(f"Raw response: {response[:500]}...")
        raise ValueError(f"LLM did not return valid JSON: {e}")