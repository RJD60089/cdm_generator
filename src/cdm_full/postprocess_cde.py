# src/cdm_full/postprocess_cde.py
"""
Post-processing: Critical Data Element (CDE) Identification

Identifies Critical Data Elements in the CDM using AI with full CDM context.

CDE Definition ("Front Page Test"):
A data element whose exposure, corruption, or loss would cause the most severe
organizational consequences — financial penalties, legal liability, reputational
damage, or loss of business credibility. CDEs are rare by definition.

CDEs are NOT operationally important fields, NOT all PHI/PII, and NOT any field
that is merely sensitive. They are the subset whose compromise creates outsized,
headline-level harm.

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

CDE_IDENTIFICATION_PROMPT = """You are a data governance expert identifying \
Critical Data Elements (CDEs) for a Pharmacy Benefit Management (PBM) \
Canonical Data Model.

IMPORTANT DEFINITION — THE "FRONT PAGE TEST":
A Critical Data Element is a data element that, if exposed, corrupted, or \
lost, would cause the MOST SEVERE consequences to the organization in terms of:
  - Financial cost (fines, penalties, lawsuits, remediation)
  - Legal/regulatory liability (HIPAA violations, breach notification \
obligations, regulatory action)
  - Reputational damage (public embarrassment, loss of client/member trust)
  - Business credibility (loss of contracts, market position, partner \
confidence)

Apply the "New York Times front page" test: if this specific data element \
appeared in a headline breach story, would it cause significant organizational \
harm? Only select elements where the answer is clearly YES.

CDEs are the WORST OF THE WORST — not merely important, not merely sensitive, \
but the elements whose compromise would be most damaging. CDEs are rare by \
definition. If your list feels long, your threshold is too low.

FULL CDM (includes entities, attributes, descriptions, business rules, \
relationships, PHI/PII flags):
{cdm_json}

=== SELECTION GUIDANCE ===

PII, PHI, and operational flags in the CDM are INPUTS to your judgment, not \
automatic qualifiers. Most PHI fields are sensitive but routine. A CDE is the \
subset where exposure creates outsized harm:

- DIRECT MEMBER IDENTIFIERS that enable identity theft or HIPAA breach \
(e.g., SSN, full name + DOB + address combination, Member ID paired with \
clinical data)
- CLINICAL DATA that reveals conditions, diagnoses, or treatment details — \
exposure is deeply personal and triggers HIPAA breach notification
- FINANCIAL DATA whose exposure reveals proprietary pricing, contractual \
terms, or payment amounts that would damage competitive position or client \
relationships
- CREDENTIALS OR KEYS that grant unauthorized system access or enable fraud \
at scale
- CREDIBILITY-CRITICAL DATA whose corruption or exposure would undermine \
organizational trustworthiness — data that clients, regulators, or partners \
rely on as authoritative (e.g., accreditation identifiers, contract-governing \
codes, benefit configuration that determines member out-of-pocket costs)

=== ENTITY SCOPE ===

ONLY select CDEs from entities with classification="Core".
If no classification value, assume Core.
Do NOT select from Reference or Junction entities.

=== WHAT IS NOT A CDE ===

Do NOT select fields just because they are:
- PHI or PII flagged (these flags are inputs, not conclusions)
- Primary keys or foreign keys (unless they are also direct business \
identifiers with real-world exposure risk, like SSN or DEA number)
- Status fields, flags, or enumerated types
- Audit timestamps (created_at, updated_at)
- System-generated IDs with no business meaning
- Descriptive or free-text fields
- From Reference or Junction classification entities
- Operationally important but not damaging if exposed in isolation \
(e.g., quantity dispensed, days supply, date of service, plan name, zip code)

=== SOURCE CONTEXT ===

When evaluating, consider source coverage (in source_lineage):
- Fields emphasized in Guardrails are likely business-critical, but \
business-critical does not automatically mean CDE
- Fields in Glue represent current system reality
- Fields appearing across multiple sources may warrant closer scrutiny

=== PBM-SPECIFIC CONTEXT ===

This is a pass-through PBM model (not spread). Consider that:
- Pricing transparency is a business differentiator, but contractual rate \
details remain confidential
- Member clinical and identity data carries the highest breach risk
- Prescriber DEA numbers and NPI paired with prescribing patterns can be \
sensitive
- BIN/PCN combinations can be used to route fraudulent claims

=== OUTPUT FORMAT ===

Return a JSON object with your identified CDEs.

{{
  "critical_data_elements": [
    {{
      "entity": "EntityName",
      "attribute": "attribute_name",
      "cde_category": "member_identity|clinical|financial_confidential|\
access_credential|fraud_risk|credibility",
      "justification": "Brief explanation of why this meets the front-page test"
    }}
  ]
}}

=== FINAL GATE ===

For each candidate CDE, ask yourself:
  1. "Could I justify EXCLUDING this?" — If yes, exclude it.
  2. "Is the front-page harm specific and severe, or just generally bad?" — \
If general, exclude it.
  3. "Would a reasonable executive lose sleep over THIS specific field being \
exposed?" — If not, exclude it.

Only elements that survive all three questions belong on the list.
When in doubt, leave it out. An empty list is a valid result — it means the \
domain has no elements that meet this threshold.

Do not force elements onto the list to avoid returning an empty result."""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# Valid CDE categories for validation
VALID_CDE_CATEGORIES = {
    "member_identity",
    "clinical",
    "financial_confidential",
    "access_credential",
    "fraud_risk",
    "credibility",
}


def parse_cde_response(response_text: str) -> List[Dict[str, str]]:
    """Parse LLM response to extract CDEs.

    Handles JSON wrapped in markdown fences, bare JSON objects,
    and bare arrays. Returns an empty list on parse failure.
    """
    text = response_text.strip()

    # Strip markdown fences
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        text = text[start:end].strip()

    # Attempt full JSON object parse
    try:
        data = json.loads(text)
        return data.get("critical_data_elements", [])
    except json.JSONDecodeError:
        pass

    # Fallback: extract bare array
    if "[" in text and "]" in text:
        start = text.find("[")
        end = text.rfind("]") + 1
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    return []


def validate_cdes(cdes: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Validate and clean parsed CDEs.

    Ensures each CDE has required fields and a recognized category.
    Drops malformed entries rather than failing the run.
    """
    required_fields = {"entity", "attribute", "cde_category", "justification"}
    valid = []

    for cde in cdes:
        # Check required fields present and non-empty
        if not all(cde.get(f) for f in required_fields):
            continue

        # Normalize category
        category = cde["cde_category"].lower().strip()
        if category not in VALID_CDE_CATEGORIES:
            # Keep it but flag — don't silently drop legitimate CDEs
            # over a minor category label mismatch
            cde["cde_category"] = category
            cde["_category_warning"] = (
                f"Unrecognized category '{category}'. "
                f"Expected one of: {', '.join(sorted(VALID_CDE_CATEGORIES))}"
            )

        valid.append(cde)

    return valid


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def identify_cdes(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    """Identify Critical Data Elements using AI with full CDM context.

    Args:
        cdm: Full CDM dictionary
        llm: LLM client for API calls
        dry_run: If True, save prompt but do not call API

    Returns:
        List of validated CDE dicts with entity, attribute, cde_category,
        and justification.
    """
    # Build prompt with full CDM
    prompt = CDE_IDENTIFICATION_PROMPT.format(
        cdm_json=json.dumps(cdm, indent=2, default=str)
    )

    if dry_run:
        print(f"\n{'='*60}")
        print("CDE IDENTIFICATION PROMPT (DRY RUN)")
        print(f"{'='*60}")
        print(prompt[:3000] + "\n... [truncated]" if len(prompt) > 3000 else prompt)
        print(f"\n{'='*60}")
        return []

    # Call LLM
    print("   Identifying Critical Data Elements (front-page test)...")

    response, _ = llm.chat(
        messages=[{"role": "user", "content": prompt}]
    )

    # Parse and validate
    raw_cdes = parse_cde_response(response)
    cdes = validate_cdes(raw_cdes)

    dropped = len(raw_cdes) - len(cdes)
    if dropped:
        print(f"   ⚠️  Dropped {dropped} malformed CDE entries")

    print(f"   Identified {len(cdes)} CDEs")

    return cdes


def run_cde_postprocess(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Run CDE post-processing and add results to CDM.

    Args:
        cdm: Full CDM dictionary (will be modified in place)
        llm: LLM client
        dry_run: If True, show prompt only

    Returns:
        Updated CDM with critical_data_elements added (or empty list).
    """
    print(f"\n   POST-PROCESSING: CDE Identification (Front Page Test)")
    print(f"   {'-'*50}")

    cdes = identify_cdes(cdm, llm, dry_run)

    # Always write the key — empty list is a valid, intentional result
    # Strip internal keys (e.g., _category_warning) before persisting
    clean_cdes = [
        {k: v for k, v in cde.items() if not k.startswith("_")}
        for cde in cdes
    ]
    cdm["critical_data_elements"] = clean_cdes

    if cdes:
        # Count by category
        categories: Dict[str, int] = {}
        for cde in cdes:
            cat = cde.get("cde_category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1

        # Summary
        print(f"\n   CDEs identified: {len(cdes)}")
        print(f"   By category:")
        for cat, count in sorted(categories.items()):
            print(f"      - {cat}: {count}")

        # Show all — list is intentionally short
        print(f"\n   CDEs:")
        for cde in cdes:
            cat = cde.get("cde_category", "")
            print(f"      • {cde.get('entity')}.{cde.get('attribute')} [{cat}]")

            # Surface category warnings if present
            if "_category_warning" in cde:
                print(f"        ⚠️  {cde['_category_warning']}")
    else:
        print(f"\n   No CDEs identified for this domain.")
        if not dry_run:
            print(f"   This is a valid result — not all domains have front-page-level elements.")

    return cdm