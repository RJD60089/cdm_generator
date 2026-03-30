# src/cdm_full/postprocess_rematch.py
"""
Post-processing: Unmapped Field Re-Match

A focused second-pass that attempts to resolve source fields that were left
unmapped (with no explicit reason) during the initial Step 6 match generation.

Why this helps:
  - Initial pass processes 150 attrs simultaneously; some fall through the cracks
  - This pass sends only the unresolved fields with the full CDM as context
  - Forces an explicit disposition on every field - no silent non-matches
  - Deduplicates structurally identical entities (e.g. PaidHistory/Revhistory/
    IncyclePaid/Incycledeleted) so each unique attr is only sent once, then
    the resolved mapping is applied back to all matching entities

Input:  Full CDM JSON + gaps JSON file
Output: Updated Full CDM JSON (new lineage entries) + updated gaps JSON
        (resolved entries removed from unmapped_fields, moved to rematch_resolved)
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REMATCH_BATCH_SIZE = 60      # Attrs per LLM call - smaller for focused attention
MAX_CDM_ATTR_DESC = 120      # Chars to include per CDM attribute description


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

REMATCH_SYSTEM_PROMPT = (
    "You are a senior data architect mapping source system fields to a "
    "Canonical Data Model (CDM) for a Pharmacy Benefit Management company "
    "using a pass-through (not spread) pricing model. "
    "You return ONLY valid JSON. No prose, no markdown fences."
)

REMATCH_PROMPT = """These source fields were NOT mapped to the {domain} CDM in an initial automated pass.
Your task is to review each one carefully and provide a definitive disposition.

FULL CDM CATALOG:
{cdm_catalog}

SOURCE FIELDS TO REMAP (source_type | source_entity | attribute_name | description):
{source_fields}

INSTRUCTIONS:
1. For each source field, carefully consider ALL CDM attributes - look for:
   - Semantic equivalents (e.g. submit_pharm_num → pharmacy_nabp_number)
   - Submitted vs adjudicated variants (e.g. submit_quantity → quantity_dispensed)
   - Abbreviations and alternate names (e.g. grp_num → group_number)
   - Cross-entity mappings if the field clearly belongs to a different CDM entity

2. Every field MUST receive one of these dispositions:
   - "mapped"   : A confident CDM match was found
   - "unmapped" : No CDM match exists - MUST provide an explicit reason

3. For "unmapped" provide the reason category:
   - "excluded_by_design"  : Field belongs to a domain this CDM explicitly excludes
                             (e.g. member PII in Claims CDM, drug master in Claims CDM)
   - "cross_domain"        : Field belongs to a different CDM domain
   - "technical_metadata"  : ETL/DW technical field with no business meaning in CDM
   - "no_cdm_equivalent"   : Legitimate business field but CDM has no matching attribute
   - "bare_code_stub"      : Field code with no recoverable business semantics

OUTPUT FORMAT - return exactly this JSON structure:
{{
  "rematch_results": [
    {{
      "source_type": "edw",
      "source_entity": "PaidHistory",
      "source_attribute": "submit_pharm_num",
      "disposition": "mapped",
      "cdm_entity": "ClaimTransaction",
      "cdm_attribute": "pharmacy_nabp_number",
      "confidence": "high",
      "reasoning": "NABP/NPI pharmacy identifier submitted on claim"
    }},
    {{
      "source_type": "edw",
      "source_entity": "PaidHistory",
      "source_attribute": "un003_procare_conv_clm_ind",
      "disposition": "unmapped",
      "cdm_entity": null,
      "cdm_attribute": null,
      "confidence": null,
      "reason_category": "no_cdm_equivalent",
      "reasoning": "Navitus-internal ProCare conversion indicator not modeled in CDM"
    }}
  ]
}}"""


# ---------------------------------------------------------------------------
# CDM catalog builder (token-efficient)
# ---------------------------------------------------------------------------

def _build_rematch_catalog(cdm: Dict) -> str:
    """Build compact CDM catalog string for the rematch prompt."""
    lines = [f"Domain: {cdm.get('domain', 'Unknown')}", ""]

    for entity in cdm.get("entities", []):
        ename = entity.get("entity_name", "")
        edesc = (entity.get("description") or "")[:80]
        lines.append(f"ENTITY: {ename} — {edesc}")

        for attr in entity.get("attributes", []):
            aname = attr.get("attribute_name", "")
            dtype = attr.get("data_type", "")
            desc  = (attr.get("description") or "")[:MAX_CDM_ATTR_DESC]
            pk    = " [PK]" if attr.get("pk") else ""
            lines.append(f"  {aname} ({dtype}){pk}: {desc}")

        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Gaps file helpers
# ---------------------------------------------------------------------------

def _find_gaps_file(outdir: Path, domain: str) -> Optional[Path]:
    """Find the latest gaps file for the domain."""
    domain_safe = domain.lower().replace(" ", "_")
    full_cdm_dir = outdir / "full_cdm"
    if not full_cdm_dir.exists():
        return None
    matches = sorted(full_cdm_dir.glob(f"gaps_{domain_safe}_*.json"), reverse=True)
    return matches[0] if matches else None


def _find_full_cdm(outdir: Path, domain: str) -> Optional[Path]:
    """Find the latest Full CDM JSON."""
    domain_safe = domain.lower().replace(" ", "_")
    full_cdm_dir = outdir / "full_cdm"
    if not full_cdm_dir.exists():
        return None
    matches = sorted(full_cdm_dir.glob(f"cdm_{domain_safe}_full_*.json"), reverse=True)
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def _deduplicate_unmapped(
    unmapped_fields: List[Dict]
) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
    """
    Deduplicate unmapped fields by (source_type, attribute_name).

    Structurally identical entities (e.g. PaidHistory/Revhistory) produce the
    same attribute names unmapped multiple times.  We pick one representative
    row per unique (source_type, attr_name) and record all originals so we can
    fan the resolved mapping back out.

    Returns:
        unique_fields   : one representative row per unique combination
        attr_to_originals : maps (source_type, attr_name) → all original rows
    """
    attr_to_originals: Dict[str, List[Dict]] = defaultdict(list)
    unique_fields: List[Dict] = []
    seen: set = set()

    for field in unmapped_fields:
        src_type = field.get("source_type", "")
        attr_name = field.get("source_attribute", "")
        key = (src_type, attr_name)

        attr_to_originals[str(key)].append(field)

        if key not in seen:
            seen.add(key)
            unique_fields.append(field)

    return unique_fields, attr_to_originals


# ---------------------------------------------------------------------------
# LLM call + parse
# ---------------------------------------------------------------------------

def _call_rematch_llm(
    batch: List[Dict],
    cdm_catalog: str,
    domain: str,
    llm: LLMClient
) -> List[Dict]:
    """Send one batch to the LLM and return parsed rematch_results."""

    # Format source fields table
    field_lines = []
    for f in batch:
        parts = [
            f.get("source_type", ""),
            f.get("source_entity", ""),
            f.get("source_attribute", ""),
            (f.get("description") or "")[:100]
        ]
        field_lines.append(" | ".join(parts))

    prompt = REMATCH_PROMPT.format(
        domain=domain,
        cdm_catalog=cdm_catalog,
        source_fields="\n".join(field_lines)
    )

    response, _ = llm.chat(
        messages=[
            {"role": "system", "content": REMATCH_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ]
    )

    # Parse JSON
    text = response.strip()
    # Strip markdown fences if present
    if "```json" in text:
        text = text[text.find("```json") + 7: text.rfind("```")].strip()
    elif "```" in text:
        text = text[text.find("```") + 3: text.rfind("```")].strip()

    try:
        data = json.loads(text)
        return data.get("rematch_results", [])
    except json.JSONDecodeError:
        # Try to find just the array
        start = text.find("[")
        end   = text.rfind("]") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except Exception:
                pass
    print(f"      ⚠️  Failed to parse rematch response for batch")
    return []


# ---------------------------------------------------------------------------
# Apply resolved mappings back to CDM
# ---------------------------------------------------------------------------

def _apply_rematch_to_cdm(
    cdm: Dict,
    resolved: List[Dict],
    attr_to_originals: Dict[str, List[Dict]]
) -> Tuple[Dict, int]:
    """
    Add source_lineage entries to CDM attributes for successfully re-matched fields.

    Because of deduplication, one resolved result may represent multiple source
    entities (e.g. PaidHistory AND Revhistory both have submit_pharm_num).
    We fan the mapping out to all original entities.

    Returns:
        updated CDM, count of lineage entries added
    """
    # Build entity + attribute lookup
    entity_lookup: Dict[str, Dict] = {
        e.get("entity_name", "").lower(): e
        for e in cdm.get("entities", [])
    }

    applied = 0

    for result in resolved:
        if result.get("disposition") != "mapped":
            continue

        cdm_entity_name = result.get("cdm_entity", "")
        cdm_attr_name   = result.get("cdm_attribute", "")
        src_type        = result.get("source_type", "")
        src_attr        = result.get("source_attribute", "")
        confidence      = result.get("confidence", "high")
        reasoning       = result.get("reasoning", "")

        if not cdm_entity_name or not cdm_attr_name:
            continue

        cdm_entity = entity_lookup.get(cdm_entity_name.lower())
        if not cdm_entity:
            continue

        # Find the CDM attribute
        cdm_attr = next(
            (a for a in cdm_entity.get("attributes", [])
             if a.get("attribute_name", "").lower() == cdm_attr_name.lower()),
            None
        )
        if not cdm_attr:
            continue

        # Ensure source_lineage key exists
        if "source_lineage" not in cdm_attr:
            cdm_attr["source_lineage"] = {}
        if src_type not in cdm_attr["source_lineage"]:
            cdm_attr["source_lineage"][src_type] = []

        # Fan out to all original source entities for this (source_type, attr_name)
        key = str((src_type, src_attr))
        originals = attr_to_originals.get(key, [{"source_entity": result.get("source_entity", "")}])

        for orig in originals:
            lineage_entry = {
                "source_entity":    orig.get("source_entity", ""),
                "source_attribute": src_attr,
                "rematch":          True,
                "confidence":       confidence,
                "reasoning":        reasoning
            }
            # Avoid exact duplicates
            if lineage_entry not in cdm_attr["source_lineage"][src_type]:
                cdm_attr["source_lineage"][src_type].append(lineage_entry)
                applied += 1

    return cdm, applied


# ---------------------------------------------------------------------------
# Update gaps file
# ---------------------------------------------------------------------------

def _update_gaps_file(
    gaps: Dict,
    resolved: List[Dict],
    attr_to_originals: Dict[str, List[Dict]]
) -> Tuple[Dict, int, int]:
    """
    Remove resolved fields from unmapped_fields.
    Add them to a new rematch_resolved section.
    Update summary counts.

    Returns:
        updated gaps, resolved_count, still_unmapped_count
    """
    resolved_keys = set()
    rematch_resolved = []

    for result in resolved:
        src_type = result.get("source_type", "")
        src_attr = result.get("source_attribute", "")

        if result.get("disposition") == "mapped":
            key = str((src_type, src_attr))
            resolved_keys.add(key)

            # Fan out — one resolved entry covers all duplicate entities
            originals = attr_to_originals.get(key, [])
            for orig in originals:
                rematch_resolved.append({
                    "source_type":      src_type,
                    "source_entity":    orig.get("source_entity", ""),
                    "source_attribute": src_attr,
                    "cdm_entity":       result.get("cdm_entity"),
                    "cdm_attribute":    result.get("cdm_attribute"),
                    "confidence":       result.get("confidence"),
                    "reasoning":        result.get("reasoning")
                })

        elif result.get("disposition") == "unmapped":
            # Now has an explicit reason — update existing unmapped entries
            reason_cat = result.get("reason_category", "")
            reasoning  = result.get("reasoning", "")
            src_type   = result.get("source_type", "")
            src_attr   = result.get("source_attribute", "")
            for field in gaps.get("unmapped_fields", []):
                if (field.get("source_type") == src_type and
                        field.get("source_attribute") == src_attr and
                        not field.get("reason")):
                    field["reason"] = f"[{reason_cat}] {reasoning}"

    # Remove resolved from unmapped_fields
    original_unmapped = gaps.get("unmapped_fields", [])
    remaining = [
        f for f in original_unmapped
        if str((f.get("source_type", ""), f.get("source_attribute", "")))
        not in resolved_keys
    ]

    gaps["unmapped_fields"] = remaining
    gaps["rematch_resolved"] = gaps.get("rematch_resolved", []) + rematch_resolved
    gaps["summary"]["total_unmapped"] = len(remaining)
    gaps["summary"]["rematch_resolved"] = len(gaps["rematch_resolved"])

    # Recount unmapped_by_source
    by_source: Dict[str, int] = {}
    for f in remaining:
        src = f.get("source_type", "unknown")
        by_source[src] = by_source.get(src, 0) + 1
    gaps["summary"]["unmapped_by_source"] = by_source

    return gaps, len(rematch_resolved), len(remaining)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_rematch_postprocess(
    cdm: Dict[str, Any],
    llm: LLMClient,
    dry_run: bool = False,
    gaps_path: Optional[Path] = None,
    outdir: Optional[Path] = None,
    domain: str = ""
) -> Dict[str, Any]:
    """
    Run unmapped field re-match post-processing.

    Args:
        cdm:        Full CDM dictionary (will be modified in place)
        llm:        LLM client
        dry_run:    If True, show prompt for first batch only, no API calls
        gaps_path:  Path to gaps JSON file (auto-found from outdir/domain if None)
        outdir:     Base output directory (used to find/save gaps file)
        domain:     CDM domain name

    Returns:
        Updated CDM dictionary
    """
    print(f"\n   POST-PROCESSING: Unmapped Field Re-Match")
    print(f"   {'-'*40}")

    # --- Locate gaps file ---
    if gaps_path is None and outdir and domain:
        gaps_path = _find_gaps_file(outdir, domain)

    if not gaps_path or not gaps_path.exists():
        print(f"   ⚠️  No gaps file found — skipping rematch")
        return cdm

    with open(gaps_path, "r", encoding="utf-8") as f:
        gaps = json.load(f)

    unmapped_fields = gaps.get("unmapped_fields", [])
    print(f"   Total unmapped in gaps file: {len(unmapped_fields)}")

    # --- Filter to no-reason unmapped only ---
    no_reason = [f for f in unmapped_fields if not f.get("reason")]
    print(f"   No-reason unmapped (re-match candidates): {len(no_reason)}")

    if not no_reason:
        print(f"   ✓ Nothing to re-match — all unmapped fields have explicit reasons")
        return cdm

    # --- Deduplicate ---
    unique_fields, attr_to_originals = _deduplicate_unmapped(no_reason)
    print(f"   Unique attribute names after deduplication: {len(unique_fields)}")

    # --- Build CDM catalog ---
    cdm_catalog = _build_rematch_catalog(cdm)

    # --- Batch and call ---
    batches = [
        unique_fields[i: i + REMATCH_BATCH_SIZE]
        for i in range(0, len(unique_fields), REMATCH_BATCH_SIZE)
    ]
    n_batches = len(batches)
    print(f"   Batches: {n_batches} × {REMATCH_BATCH_SIZE} attrs")

    if dry_run:
        print(f"\n{'='*60}")
        print("REMATCH PROMPT — DRY RUN (first batch only)")
        print(f"{'='*60}")
        field_lines = [
            " | ".join([
                f.get("source_type", ""),
                f.get("source_entity", ""),
                f.get("source_attribute", ""),
                (f.get("description") or "")[:80]
            ])
            for f in batches[0]
        ]
        preview = REMATCH_PROMPT.format(
            domain=domain or cdm.get("domain", ""),
            cdm_catalog=cdm_catalog[:1000] + "\n... [truncated] ...",
            source_fields="\n".join(field_lines)
        )
        print(preview[:3000])
        print(f"{'='*60}")
        return cdm

    # --- Live run ---
    all_results: List[Dict] = []
    total_mapped = 0

    for i, batch in enumerate(batches, 1):
        print(f"   Batch {i}/{n_batches} ({len(batch)} attrs)...", end="", flush=True)
        results = _call_rematch_llm(
            batch=batch,
            cdm_catalog=cdm_catalog,
            domain=domain or cdm.get("domain", ""),
            llm=llm
        )
        mapped_in_batch = sum(1 for r in results if r.get("disposition") == "mapped")
        total_mapped += mapped_in_batch
        all_results.extend(results)
        print(f" mapped: {mapped_in_batch}/{len(batch)}")

    print(f"\n   Re-match complete: {total_mapped} resolved of {len(unique_fields)} unique attrs")

    # --- Apply to CDM ---
    cdm, lineage_added = _apply_rematch_to_cdm(cdm, all_results, attr_to_originals)
    print(f"   Lineage entries added to CDM: {lineage_added}")

    # --- Update gaps file ---
    gaps, resolved_count, still_unmapped = _update_gaps_file(
        gaps, all_results, attr_to_originals
    )
    print(f"   Removed from unmapped:        {resolved_count}")
    print(f"   Remaining unmapped:           {still_unmapped}")

    # --- Save updated gaps file ---
    if outdir and domain:
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = domain.lower().replace(" ", "_")
        gaps_outdir = outdir / "full_cdm"
        gaps_outdir.mkdir(parents=True, exist_ok=True)
        new_gaps_path = gaps_outdir / f"gaps_{domain_safe}_{timestamp}.json"
        with open(new_gaps_path, "w", encoding="utf-8") as f:
            json.dump(gaps, f, indent=2)
        print(f"   Updated gaps file: {new_gaps_path.name}")

    return cdm