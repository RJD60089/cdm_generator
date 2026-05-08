# src/cdm_full/postprocess_edw_lineage.py
"""
Post-processing: EDW Lineage Enrichment

Backfills the actual SQL identifiers (OLTP / NI / NP column and table names)
into every EDW source_lineage entry of the Full CDM.

Why this exists:
    match_applier.py constructs each EDW lineage entry from a fixed set of
    fields and never copies forward the rationalizer's source_metadata block.
    As a result, source_lineage["edw"][*] only carries the rationalized
    business-friendly aliases (e.g. "trankey", "Rejects") — not the actual
    SQL identifiers (TRANKEY, NP_REJECTS, etc.) needed for Mapping/Collibra
    artifacts. This step closes that gap without re-running the AI matcher.

For each attribute-level lineage entry, adds:
    source_column   - OLTP column name from rationalized source_metadata
    ni_column       - NI staging column name
    np_column       - NP persisted column name
    raw_data_type   - original Oracle/SQL type from the catalog

For each entity-level lineage entry, adds:
    source_table    - OLTP table name (already present as `table`, kept for symmetry)
    ni_table        - NI staging table name
    np_table        - NP persisted table name

No LLM calls — purely a structural lookup and copy from
rationalized_edw_<domain>_*.json. Safe to run repeatedly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# File finders (mirror postprocess_field_codes.py / postprocess_ancillary.py)
# ---------------------------------------------------------------------------

def _find_rationalized_edw(outdir: Path, domain: str) -> Optional[Path]:
    """Find the latest rationalized_edw_<domain>_*.json (with attributes)."""
    rat_dir = outdir / "rationalized"
    if not rat_dir.exists():
        return None
    domain_safe = domain.lower().replace(" ", "_")
    matches = sorted(
        [
            p for p in rat_dir.glob(f"rationalized_edw_{domain_safe}_*.json")
            # The "entities-only" Pass 1 output is named with an extra
            # "entities" segment — skip it; we want the full P1+2+3 file.
            if "entities" not in p.stem.split("_")
        ],
        reverse=True,
    )
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def _build_lookups(rat_path: Path) -> Tuple[
    Dict[Tuple[str, str], Dict[str, Any]],
    Dict[str, Dict[str, Any]],
]:
    """
    Read the rationalized EDW JSON and return two indexes:

        attr_lookup:   (entity_name_lower, attr_name_lower)
                       -> {source_column, ni_column, np_column, raw_data_type}

        entity_lookup: entity_name_lower
                       -> {source_table, ni_table, np_table, source_database,
                           source_schema}

    Missing keys remain absent (callers use .get() with defaults).
    """
    with open(rat_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    attr_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    entity_lookup: Dict[str, Dict[str, Any]] = {}

    for entity in data.get("entities", []) or []:
        ent_name = (entity.get("entity_name") or "").strip()
        if not ent_name:
            continue
        info = entity.get("source_info") or {}
        entity_lookup[ent_name.lower()] = {
            "source_table":    info.get("source_table"),
            "ni_table":        info.get("ni_table"),
            "np_table":        info.get("np_table"),
            "source_database": info.get("source_database"),
            "source_schema":   info.get("source_schema") or info.get("schema"),
        }

        for attr in entity.get("attributes", []) or []:
            attr_name = (attr.get("attribute_name") or "").strip()
            if not attr_name:
                continue
            sm = attr.get("source_metadata") or {}
            attr_lookup[(ent_name.lower(), attr_name.lower())] = {
                "source_column": sm.get("source_column"),
                "ni_column":     sm.get("ni_column"),
                "np_column":     sm.get("np_column"),
                "raw_data_type": sm.get("raw_data_type"),
            }

    return attr_lookup, entity_lookup


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def _enrich(
    cdm: Dict[str, Any],
    attr_lookup: Dict[Tuple[str, str], Dict[str, Any]],
    entity_lookup: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Any], int, int, int, int]:
    """
    Walk every CDM entity/attribute. For each EDW lineage entry, add the
    SQL identifier fields from the lookups when not already present.

    Returns:
        (cdm, attr_entries_enriched, attr_entries_missed,
              entity_entries_enriched, entity_entries_missed)
    """
    attr_hit = attr_miss = ent_hit = ent_miss = 0

    for entity in cdm.get("entities", []) or []:

        # Entity-level EDW lineage
        ent_lineage = (entity.get("source_lineage") or {}).get("edw") or []
        if isinstance(ent_lineage, dict):
            ent_lineage = [ent_lineage]
        for entry in ent_lineage:
            if not isinstance(entry, dict):
                continue
            src_ent = (entry.get("source_entity") or "").strip().lower()
            tables = entity_lookup.get(src_ent)
            if not tables:
                ent_miss += 1
                continue
            for k, v in tables.items():
                if v is not None and entry.get(k) in (None, ""):
                    entry[k] = v
            ent_hit += 1

        # Attribute-level EDW lineage
        # Each attribute lineage entry gets BOTH the column identifiers
        # (from attr_lookup) and the table identifiers (from entity_lookup
        # keyed by source_entity). Storing tables on every attribute entry
        # is mildly redundant but lets the Mapping tab read everything it
        # needs from a single dict without traversing back to the entity.
        for attr in entity.get("attributes", []) or []:
            attr_lineage = (attr.get("source_lineage") or {}).get("edw") or []
            if isinstance(attr_lineage, dict):
                attr_lineage = [attr_lineage]
            for entry in attr_lineage:
                if not isinstance(entry, dict):
                    continue
                src_ent_key = (entry.get("source_entity") or "").strip().lower()
                key = (
                    src_ent_key,
                    (entry.get("source_attribute") or "").strip().lower(),
                )
                cols = attr_lookup.get(key)
                tables = entity_lookup.get(src_ent_key)

                if not cols and not tables:
                    attr_miss += 1
                    continue

                if cols:
                    for k, v in cols.items():
                        if v is not None and entry.get(k) in (None, ""):
                            entry[k] = v
                if tables:
                    # Only the table names are useful at attribute scope —
                    # source_database/source_schema would just duplicate
                    # entity-level info on every row.
                    for k in ("source_table", "ni_table", "np_table"):
                        v = tables.get(k)
                        if v is not None and entry.get(k) in (None, ""):
                            entry[k] = v
                attr_hit += 1

    return cdm, attr_hit, attr_miss, ent_hit, ent_miss


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_edw_lineage_postprocess(
    cdm: Dict[str, Any],
    llm: Any,                   # unused; kept for registry signature compat
    dry_run: bool = False,
    gaps_path: Optional[Path] = None,
    outdir: Optional[Path] = None,
    domain: str = "",
) -> Dict[str, Any]:
    """
    Backfill EDW source/NI/NP column and table names into source_lineage.

    Args:
        cdm:     Full CDM dict (modified in place)
        llm:     Unused — present for registry interface compat
        dry_run: If True, report only — no changes
        outdir:  Base output directory (locates rationalized/ + artifacts/)
        domain:  CDM domain name (locates rationalized files)

    Returns:
        Updated CDM dictionary
    """
    print(f"\n   POST-PROCESSING: EDW Lineage Enrichment (source/NI/NP identifiers)")
    print(f"   {'-' * 40}")

    rat_path = _find_rationalized_edw(outdir, domain) if outdir else None
    if not rat_path:
        print(f"   ⚠️  No rationalized_edw_{domain}_*.json found in {outdir}/rationalized/ — skipping")
        return cdm

    print(f"   Source: {rat_path.name}")

    attr_lookup, entity_lookup = _build_lookups(rat_path)
    print(f"   Indexed: {len(attr_lookup)} attributes, {len(entity_lookup)} entities")

    if dry_run:
        print(f"\n   DRY RUN — no changes applied")
        sample = list(attr_lookup.items())[:5]
        print(f"\n   Sample attribute lookups:")
        for (ent, attr), cols in sample:
            print(f"      {ent}.{attr} -> {cols}")
        return cdm

    cdm, attr_hit, attr_miss, ent_hit, ent_miss = _enrich(
        cdm, attr_lookup, entity_lookup,
    )

    print(f"   Attribute lineage entries enriched: {attr_hit}  (misses: {attr_miss})")
    print(f"   Entity lineage entries enriched   : {ent_hit}  (misses: {ent_miss})")

    if attr_miss or ent_miss:
        print(
            f"   ℹ️  Misses occur when a lineage entry references an entity/attribute "
            f"that is no longer present in the rationalized file (renamed or removed)."
        )

    # Sample enriched entries for visibility
    print(f"\n   Sample enriched attribute lineage:")
    shown = 0
    for entity in cdm.get("entities", []) or []:
        for attr in entity.get("attributes", []) or []:
            edw = (attr.get("source_lineage") or {}).get("edw") or []
            for e in edw:
                if not isinstance(e, dict):
                    continue
                if e.get("np_column") or e.get("source_column") or e.get("ni_column"):
                    print(
                        f"      {entity['entity_name']}.{attr['attribute_name']}"
                        f" ← src={e.get('source_column')!r}"
                        f" ni={e.get('ni_column')!r}"
                        f" np={e.get('np_column')!r}"
                    )
                    shown += 1
                    break
            if shown >= 5:
                break
        if shown >= 5:
            break

    print(
        f"\n   ℹ️  Re-run Step 7 (Generate Artifacts) to rebuild the Excel "
        f"workbook with the enriched lineage."
    )

    return cdm
