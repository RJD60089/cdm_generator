# src/cdm_full/postprocess_cde.py
"""
Post-processing: Critical Data Element (CDE) Identification

Identifies Critical Data Elements in the CDM using AI with full CDM context.

CDE Definition (per analyst review):
1. SECURITY-SENSITIVE: PHI, PII, confidential financial/business data
2. ESSENTIAL FOR PROCESSING: Minimal fields to complete transaction "with paper and pencil"

CDEs are NOT just any important field - they require extra security scrutiny and governance.

Run after Full CDM is built in Step 6 (and after sensitivity analysis).
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# =============================================================================
# PROMPT TEMPLATE
# =============================================================================

CDE_IDENTIFICATION_PROMPT = """You are a data governance expert identifying Critical Data Elements (CDEs) for a Pharmacy Benefit Management (PBM) Canonical Data Model.

IMPORTANT: CDEs require MORE security scrutiny and governance than regular data elements. This is a selective list, not comprehensive.

FULL CDM (includes entities, attributes, descriptions, business rules, relationships, PHI/PII flags):
{cdm_json}

=== CDE SELECTION CRITERIA ===

A data element is a CDE if it meets ONE OR MORE of these criteria:

1. SECURITY-SENSITIVE DATA
   - PHI (Protected Health Information under HIPAA) - check is_phi flag
   - PII (Personally Identifiable Information) - check is_pii flag
   - Confidential financial data (pricing, costs, rates, fees, payment amounts)
   - Confidential business references (contract terms, discount rates, proprietary codes)

2. ESSENTIAL FOR END-TO-END PROCESSING
   - The minimal set of fields required to complete a business transaction "with paper and pencil"
   - Example: To process a pharmacy claim you minimally need: member ID, drug (NDC), prescriber (NPI), pharmacy (NPI), quantity, date of service
   - If this field is missing or wrong, the transaction cannot complete

=== ENTITY SCOPE ===

ONLY select CDEs from entities with classification="Core".
Do NOT select CDEs from:
- Reference entities (lookup/code tables like AccountType, ObjectStatus)
- Junction entities (M:M relationship tables like GroupPlanAssignment)

=== WHAT IS NOT A CDE ===

Do NOT select fields just because they are:
- Primary keys or foreign keys (unless they are also business identifiers like BIN, PCN, NPI, NDC)
- Status fields or flags
- Audit timestamps (created_at, updated_at)
- System-generated IDs with no business meaning
- Descriptive text fields
- From Reference or Junction classification entities

=== SOURCE CONTEXT ===

When evaluating, consider source coverage (in source_lineage):
- Fields emphasized in Guardrails are likely business-critical
- Fields in Glue represent current system reality
- Fields in multiple sources are more likely to be essential

=== OUTPUT FORMAT ===

Return a JSON object with your identified CDEs. Be selective - typically 15-30 elements for a domain.

{{
  "critical_data_elements": [
    {{
      "entity": "EntityName",
      "attribute": "attribute_name",
      "cde_category": "phi|pii|financial|business_confidential|essential_process",
      "justification": "Brief explanation of why this meets CDE criteria"
    }}
  ]
}}

Focus on QUALITY over quantity. Every CDE should clearly meet the criteria above."""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_cde_response(response_text: str) -> List[Dict[str, str]]:
    """Parse LLM response to extract CDEs."""
    text = response_text.strip()
    
    # Look for JSON block
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        text = text[start:end].strip()
    
    # Parse JSON
    try:
        data = json.loads(text)
        return data.get("critical_data_elements", [])
    except json.JSONDecodeError:
        # Try to find just the array
        if "[" in text and "]" in text:
            start = text.find("[")
            end = text.rfind("]") + 1
            try:
                return json.loads(text[start:end])
            except:
                pass
    
    return []


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def identify_cdes(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False
) -> List[Dict[str, str]]:
    """
    Identify Critical Data Elements using AI with full CDM context.
    
    Args:
        cdm: Full CDM dictionary
        llm: LLM client for API calls
        dry_run: If True, show prompt but do not call API
    
    Returns:
        List of CDE dictionaries with entity, attribute, cde_category, justification
    """
    
    # Build prompt with full CDM
    prompt = CDE_IDENTIFICATION_PROMPT.format(
        cdm_json=json.dumps(cdm, indent=2, default=str)
    )
    
    if dry_run:
        print(f"\n{'='*60}")
        print("CDE IDENTIFICATION PROMPT (DRY RUN)")
        print(f"{'='*60}")
        print(prompt[:2000] + "..." if len(prompt) > 2000 else prompt)
        print(f"{'='*60}")
        return []
    
    # Call LLM
    print(f"   Identifying Critical Data Elements (full CDM context)...")
    
    response, _ = llm.chat(
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Parse response
    cdes = parse_cde_response(response)
    
    print(f"   Identified {len(cdes)} CDEs")
    
    return cdes


def run_cde_postprocess(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Run CDE post-processing and add to CDM.
    
    Args:
        cdm: Full CDM dictionary (will be modified)
        llm: LLM client
        dry_run: If True, show prompt only
    
    Returns:
        Updated CDM with critical_data_elements added
    """
    
    print(f"\n   POST-PROCESSING: CDE Identification")
    print(f"   {'-'*40}")
    
    cdes = identify_cdes(cdm, llm, dry_run)
    
    if cdes:
        cdm["critical_data_elements"] = cdes
        
        # Count by category
        categories = {}
        for cde in cdes:
            cat = cde.get("cde_category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1
        
        # Print summary
        print(f"\n   CDEs identified: {len(cdes)}")
        print(f"   By category:")
        for cat, count in sorted(categories.items()):
            print(f"      - {cat}: {count}")
        
        print(f"\n   Sample CDEs:")
        for cde in cdes[:5]:
            cat = cde.get('cde_category', '')
            print(f"      - {cde.get('entity')}.{cde.get('attribute')} [{cat}]")
        if len(cdes) > 5:
            print(f"      ... and {len(cdes) - 5} more")
    
    return cdm