# src/cdm_full/postprocess_sensitivity.py
"""
Sensitivity Post-Processor

Uses AI to identify sensitive data attributes in the Full CDM.
Categorizes attributes as containing personal identifiers, health-related
information, or both for internal data governance documentation.

Pattern matches postprocess_cde.py which works reliably.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from src.core.llm_client import LLMClient


@dataclass
class SensitivityResult:
    """Result of sensitivity analysis for an attribute."""
    entity_name: str
    attribute_name: str
    is_pii: bool
    is_phi: bool
    pii_reason: str
    phi_reason: str


# =============================================================================
# PROMPT TEMPLATE (matches CDE pattern)
# =============================================================================

SENSITIVITY_PROMPT = """You are a data governance specialist categorizing sensitive data attributes in a Pharmacy Benefit Management (PBM) Canonical Data Model.

IMPORTANT: Only identify attributes that ARE sensitive. Non-sensitive attributes should be omitted from your response.

FULL CDM (includes entities, attributes, descriptions, business rules, relationships):
{cdm_json}

=== SENSITIVITY CATEGORIES ===

1. PERSONAL IDENTIFIERS (maps to PII)
   Data that could identify a specific individual:
   - Names (first, last, full name)
   - Contact information (address, phone, email, fax)
   - Government identifiers (SSN, license numbers)
   - Member/subscriber/patient identifiers  
   - Account numbers tied to individuals
   - Biometric data
   - Device identifiers linked to individuals

2. HEALTH RELATED (maps to PHI)
   Data about medical conditions or treatments:
   - Diagnoses and conditions
   - Medications and prescriptions (NDC, GPI, drug names)
   - Clinical notes and observations
   - Treatment plans and procedures
   - Lab results and vitals
   - Healthcare provider information when linked to patient care

=== CONTEXT RULES ===

- Consider the ENTITY NAME - "member_id" in a "Member" or "Episode" entity identifies individuals
- Drug codes (NDC, GPI) ARE health-related when associated with a member/episode
- Reference/lookup data (drug classifications without member context, status codes) is NOT sensitive
- Timestamps and audit fields are NOT sensitive
- Plan/Group/Carrier identifiers are generally NOT sensitive

=== WHAT TO SKIP ===

Do NOT include:
- Primary keys or system-generated IDs (unless they identify individuals)
- Status fields, flags, or codes
- Audit timestamps (created_at, updated_at)
- Reference/lookup table attributes
- Descriptive metadata fields

=== OUTPUT FORMAT ===

Return a JSON object with identified sensitive attributes. Be selective - typically 20-60 attributes.

{{
  "sensitive_attributes": [
    {{
      "entity": "EntityName",
      "attribute": "attribute_name",
      "has_personal_identifiers": true,
      "has_health_related": false,
      "personal_reason": "Brief explanation if personal identifier",
      "health_reason": "Brief explanation if health related"
    }}
  ]
}}

Focus on QUALITY over quantity. Every attribute should clearly be sensitive."""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_sensitivity_response(response_text: str) -> List[Dict[str, Any]]:
    """Parse LLM response to extract sensitive attributes."""
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
        return data.get("sensitive_attributes", [])
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


def classify_with_ai(
    cdm: Dict[str, Any],
    llm: LLMClient
) -> List[SensitivityResult]:
    """Use AI to identify sensitive attributes using full CDM context."""
    
    prompt = SENSITIVITY_PROMPT.format(
        cdm_json=json.dumps(cdm, indent=2, default=str)
    )
    
    # Match CDE pattern: no system message, just user prompt
    response, _ = llm.chat(
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Parse response using robust parser
    results = parse_sensitivity_response(response)
    
    if not results:
        print(f"      Warning: No sensitive attributes parsed from response")
        print(f"      Response preview: {response[:300]}...")
        return []
    
    # Convert to SensitivityResult objects
    sensitivity_results = []
    for item in results:
        if not isinstance(item, dict):
            continue
        sensitivity_results.append(SensitivityResult(
            entity_name=item.get("entity", ""),
            attribute_name=item.get("attribute", ""),
            is_pii=item.get("has_personal_identifiers", False),
            is_phi=item.get("has_health_related", False),
            pii_reason=item.get("personal_reason", ""),
            phi_reason=item.get("health_reason", "")
        ))
    
    return sensitivity_results


def run_sensitivity_postprocess(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Run sensitivity analysis on Full CDM to add sensitivity flags using AI.
    
    Args:
        cdm: Full CDM dictionary (will be modified)
        llm: LLM client for API calls
        dry_run: If True, show info but do not call API
    
    Returns:
        Updated CDM with is_pii, is_phi flags on attributes
    """
    
    print(f"\n   POST-PROCESSING: Sensitivity Analysis (PHI/PII)")
    print(f"   {'-'*40}")
    
    # Count attributes
    total_attrs = sum(
        len(entity.get("attributes", []))
        for entity in cdm.get("entities", [])
    )
    
    print(f"   Analyzing {total_attrs} attributes for sensitivity...")
    
    if dry_run:
        print(f"   (Dry run - skipping API calls)")
        return cdm
    
    # Classify using full CDM
    results = classify_with_ai(cdm, llm)
    
    if not results:
        print(f"   ⚠️  WARNING: AI returned no sensitive attributes")
        return cdm
    
    # Build lookup from results (case-insensitive matching)
    result_lookup = {
        (r.entity_name.lower(), r.attribute_name.lower()): r 
        for r in results
    }
    
    # Update CDM with results - default all to False, then set True for matches
    stats = {
        "total_attributes": total_attrs,
        "sensitive_identified": len(results),
        "pii_flagged": 0,
        "phi_flagged": 0,
        "both_flagged": 0,
        "matched": 0
    }
    
    for entity in cdm.get("entities", []):
        entity_name = entity.get("entity_name", "")
        for attr in entity.get("attributes", []):
            attr_name = attr.get("attribute_name", "")
            
            # Default to not sensitive
            attr["is_pii"] = False
            attr["is_phi"] = False
            
            # Case-insensitive lookup
            result = result_lookup.get((entity_name.lower(), attr_name.lower()))
            if result:
                stats["matched"] += 1
                attr["is_pii"] = result.is_pii
                attr["is_phi"] = result.is_phi
                
                if result.is_pii or result.is_phi:
                    attr["sensitivity_details"] = {
                        "pii_reason": result.pii_reason,
                        "phi_reason": result.phi_reason
                    }
                    
                    if result.is_pii:
                        stats["pii_flagged"] += 1
                    if result.is_phi:
                        stats["phi_flagged"] += 1
                    if result.is_pii and result.is_phi:
                        stats["both_flagged"] += 1
    
    # Update CDM metadata
    cdm["sensitivity_analysis"] = {
        "processed_date": datetime.now().isoformat(),
        "method": "ai_selective",
        "stats": stats
    }
    
    print(f"   Sensitivity analysis complete:")
    print(f"      Total attributes: {stats['total_attributes']}")
    print(f"      Sensitive identified: {stats['sensitive_identified']}")
    print(f"      Matched to CDM: {stats['matched']}")
    print(f"      PII flagged: {stats['pii_flagged']}")
    print(f"      PHI flagged: {stats['phi_flagged']}")
    print(f"      Both: {stats['both_flagged']}")
    
    return cdm