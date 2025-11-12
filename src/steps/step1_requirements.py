"""
Step 1: Requirements Gathering
Enhanced to accept JSON inputs from various sources
"""
from pathlib import Path
from typing import Dict, Optional
from src.core.llm_client import LLMClient
from src.core.run_state import RunState


def run_step1(
    domain: str,
    inputs_json: Dict[str, str],
    llm: LLMClient,
    outdir: str
) -> RunState:
    """
    Step 1: Gather requirements for CDM generation.
    
    Args:
        domain: CDM domain name (e.g., "Plan and Benefit")
        inputs_json: Dictionary of JSON strings from input files
                    Keys: 'fhir', 'guardrails', 'ddl', 'naming_standard'
        llm: LLM client instance
        outdir: Output directory
        
    Returns:
        RunState object with results
    """
    # Build prompt with JSON inputs
    prompt = _build_requirements_prompt(domain, inputs_json)
    
    print(f"Calling LLM for requirements gathering...")
    print(f"  Prompt length: {len(prompt)} characters")
    
    # Call LLM
    response = llm.call(prompt)
    
    # Save output
    output_file = Path(outdir) / f"step1_requirements_{domain.replace(' ', '_')}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(response)
    
    # Create run state
    state = RunState(
        domain=domain,
        step=1,
        prompt=prompt,
        response=response,
        output_file=str(output_file),
        metadata={
            'inputs_provided': list(inputs_json.keys()),
            'has_fhir': 'fhir' in inputs_json,
            'has_guardrails': 'guardrails' in inputs_json,
            'has_ddl': 'ddl' in inputs_json,
            'has_naming_standard': 'naming_standard' in inputs_json
        }
    )
    
    return state


def _build_requirements_prompt(domain: str, inputs_json: Dict[str, str]) -> str:
    """
    Build requirements gathering prompt with JSON inputs.
    
    Includes base Prompt 1 template plus JSON data from input files.
    """
    # Base prompt from original Prompt 1
    base_prompt = f"""You are a senior data architect at a Pharmacy Benefit Manager (PBM) that operates primarily in a **passthrough** business model (transparent pricing, minimal spread, heavy on accurate network, plan, and claim data).

We are defining a **new Canonical Data Model (CDM)** for this PBM.

**CDM Name:** {domain}

Please produce the **domain context** for this CDM with the following sections:

1. **Business intent** -- why PBM operations need this domain, in a passthrough model.

2. **Scope / out-of-scope** -- what belongs in this CDM vs what should stay in other CDMs (e.g., Member/Person, Provider/Pharmacy, Plan/Benefit, Claim).

3. **Core business capabilities it supports** -- e.g., adjudication, client reporting, rebate settlement, network management, MAC lists, accumulator updates.

4. **Primary data sources / standards** -- e.g., NCPDP, internal adjudication system, client eligibility feeds, network contracts, formulary systems.

5. **Key entities (names only for now)** -- list what you expect to become tables.

6. **PBM-specific considerations** -- especially those that are unique to passthrough (e.g., we must retain contract and rate detail as data, not just apply pricing).

Format the output in markdown so it can be copy/pasted into the "Overview" / "Definition" tab of an Excel workbook.
"""
    
    # Add input data sections
    input_sections = []
    
    if 'fhir' in inputs_json:
        input_sections.append(f"""
## FHIR Profile Data (Industry Standard)

The following FHIR resource profile is available as a foundation:
```json
{inputs_json['fhir']}
```

Use this as a baseline entity structure, adapting it for PBM passthrough model specifics.
""")
    
    if 'guardrails' in inputs_json:
        input_sections.append(f"""
## Guardrails / Business Requirements

The following business analyst specifications are available:
```json
{inputs_json['guardrails']}
```

These represent known business requirements and entity definitions for the {domain} domain.
Consider these requirements when defining scope and entities.
""")
    
    if 'ddl' in inputs_json:
        input_sections.append(f"""
## Current System DDL (Production Reality Check)

The following table schemas exist in the current production system:
```json
{inputs_json['ddl']}
```

This shows what currently exists. The new CDM may extend, replace, or incorporate these structures.
Note any gaps or opportunities for improvement.
""")
    
    if 'naming_standard' in inputs_json:
        input_sections.append(f"""
## Enterprise Naming Standards

The following naming conventions must be followed:
```json
{inputs_json['naming_standard']}
```

Ensure all entity and attribute names conform to these standards.
""")
    
    # Combine prompt
    if input_sections:
        full_prompt = f"""{base_prompt}

---

# AVAILABLE INPUT DATA

You have been provided with the following input data to inform your requirements:

{''.join(input_sections)}

---

# YOUR TASK

Using the input data provided above along with your knowledge of PBM systems and the passthrough model:

1. Generate comprehensive requirements for the **{domain}** CDM
2. Reference specific elements from the input data where relevant
3. Identify any gaps or missing information
4. Ensure the requirements support PBM passthrough operations

Generate the requirements document now.
"""
    else:
        # No inputs - just use base prompt
        full_prompt = base_prompt
    
    return full_prompt