# src/cdm_full/postprocess_sensitivity.py
"""
PHI/PII Sensitivity Post-Processor

Uses AI to identify Protected Health Information (PHI) and 
Personally Identifiable Information (PII) in the Full CDM.

AI knows:
- HIPAA Safe Harbor 18 identifiers (legal PHI definition)
- Standard PII definitions
- Context from descriptions, entity relationships, and business rules
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
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


SENSITIVITY_PROMPT = """You are a data privacy expert with deep knowledge of:
- HIPAA Safe Harbor method (18 PHI identifiers)
- PII definitions under GDPR, CCPA, and US privacy laws
- Healthcare/pharmacy data sensitivity

Analyze the CDM below and classify EACH attribute for PHI and PII.

FULL CDM:
{cdm_json}

For EVERY attribute in EVERY entity, respond with a JSON array where each item has:
- entity: entity name
- attribute: attribute name  
- is_pii: true/false - Is this Personally Identifiable Information?
- is_phi: true/false - Is this Protected Health Information under HIPAA?
- pii_reason: Brief reason if PII (empty string if not)
- phi_reason: Brief reason if PHI (empty string if not)

PHI includes the HIPAA 18: names, geographic data smaller than state, dates (except year), phone/fax, email, SSN, medical record numbers, health plan beneficiary numbers, account numbers, certificate/license numbers, vehicle identifiers, device identifiers, URLs, IP addresses, biometric identifiers, photos, and any other unique identifying number.

PII includes: name, address, email, phone, SSN, DOB, financial account numbers, government IDs, biometrics.

IMPORTANT CONTEXT RULES:
- Consider the ENTITY CONTEXT - "member_id" in a "Member" entity is PII, but "id" in a "DrugClass" entity is not
- Consider BUSINESS RULES and DESCRIPTIONS for additional context
- Consider RELATIONSHIPS - an FK to a member table may indicate indirect PII
- Plan/Group/Carrier identifiers are generally NOT PII unless they can identify individuals
- Effective dates, status fields, and audit timestamps are NOT PHI/PII

Respond ONLY with valid JSON array, no other text."""


def classify_with_ai(
    cdm: Dict[str, Any],
    llm: LLMClient
) -> List[SensitivityResult]:
    """Use AI to classify all attributes for PHI/PII using full CDM context."""
    
    prompt = SENSITIVITY_PROMPT.format(
        cdm_json=json.dumps(cdm, indent=2, default=str)
    )
    
    response, _ = llm.chat(
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Parse JSON response
    try:
        # Clean response - remove markdown if present
        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        clean = clean.strip()
        
        results = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"      Warning: Failed to parse AI response: {e}")
        return []
    
    # Handle wrapped response (e.g., {"results": [...]})
    if isinstance(results, dict):
        # Try common wrapper keys
        for key in ["results", "attributes", "classifications", "data"]:
            if key in results:
                results = results[key]
                break
        else:
            # No known wrapper, can't parse
            print(f"      Warning: Unexpected response format (dict without known wrapper)")
            return []
    
    # Ensure we have a list
    if not isinstance(results, list):
        print(f"      Warning: Expected list, got {type(results).__name__}")
        return []
    
    # Convert to SensitivityResult objects
    sensitivity_results = []
    for item in results:
        if not isinstance(item, dict):
            # Skip non-dict items
            continue
        sensitivity_results.append(SensitivityResult(
            entity_name=item.get("entity", ""),
            attribute_name=item.get("attribute", ""),
            is_pii=item.get("is_pii", False),
            is_phi=item.get("is_phi", False),
            pii_reason=item.get("pii_reason", ""),
            phi_reason=item.get("phi_reason", "")
        ))
    
    return sensitivity_results


def run_sensitivity_postprocess(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Run sensitivity analysis on Full CDM to add PHI/PII flags using AI.
    
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
    
    print(f"   Classifying {total_attrs} attributes with AI (full CDM context)...")
    
    if dry_run:
        print(f"   (Dry run - skipping API calls)")
        return cdm
    
    # Classify using full CDM
    results = classify_with_ai(cdm, llm)
    
    # Build lookup from results
    result_lookup = {(r.entity_name, r.attribute_name): r for r in results}
    
    # Update CDM with results
    stats = {
        "total_attributes": total_attrs,
        "classified": len(results),
        "pii_flagged": 0,
        "phi_flagged": 0,
        "both_flagged": 0
    }
    
    for entity in cdm.get("entities", []):
        entity_name = entity.get("entity_name", "")
        for attr in entity.get("attributes", []):
            attr_name = attr.get("attribute_name", "")
            
            result = result_lookup.get((entity_name, attr_name))
            if result:
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
        "method": "ai_full_cdm",
        "stats": stats
    }
    
    print(f"   Sensitivity analysis complete:")
    print(f"      Total attributes: {stats['total_attributes']}")
    print(f"      Classified by AI: {stats['classified']}")
    print(f"      PII flagged: {stats['pii_flagged']}")
    print(f"      PHI flagged: {stats['phi_flagged']}")
    print(f"      Both PHI+PII: {stats['both_flagged']}")
    
    return cdm