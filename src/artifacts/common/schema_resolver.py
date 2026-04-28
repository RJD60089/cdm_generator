# src/artifacts/common/schema_resolver.py
"""
Per-source schema resolution for the Mapping tab.

Resolves the SOURCE schema for every row written to the Mapping tab,
keyed by (mapping_source, source_entity).  Sources of truth, in order:

  - "edw"  →  rationalized_edw_<domain>_*.json   (entities[*].source_info.source_schema)
  - "ancillary-*" with a SQL DDL file →  parse `CREATE TABLE <schema>.<table>` patterns
  - anything else (or extraction failure) →  fall back to config.mapping.source_schema

This keeps users from having to maintain a per-source schema in config.json
when the schema is already present in the source artifacts.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.config.config_parser import AppConfig
from src.config import config_utils


# ---------------------------------------------------------------------------
# SQL DDL schema extractor
# ---------------------------------------------------------------------------

# Matches CREATE TABLE [schema].[table] in a wide range of SQL dialects:
#   CREATE TABLE schema.table (
#   CREATE TABLE [schema].[table] (
#   CREATE TABLE "schema"."table" (
#   CREATE TABLE `schema`.`table` (
#   CREATE TABLE IF NOT EXISTS schema.table (
# Schema and table identifiers may contain letters, digits, underscores.
_DDL_CREATE_TABLE_RE = re.compile(
    r"""
    \bCREATE \s+ TABLE \s+ (?:IF \s+ NOT \s+ EXISTS \s+)?
    (?:\[ ([\w]+) \] | " ([\w]+) " | ` ([\w]+) ` | ([\w]+))      # schema (g1-4)
    \s* \. \s*
    (?:\[ ([\w]+) \] | " ([\w]+) " | ` ([\w]+) ` | ([\w]+))      # table  (g5-8)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def extract_ddl_schemas(ddl_text: str) -> Dict[str, str]:
    """
    Return ``{table_name_lower: schema}`` for every CREATE TABLE
    statement found in the DDL text.  Tables without a schema prefix
    are absent from the result (the caller handles fallback).
    """
    out: Dict[str, str] = {}
    for m in _DDL_CREATE_TABLE_RE.finditer(ddl_text):
        schema = next((g for g in m.groups()[:4] if g), "")
        table  = next((g for g in m.groups()[4:] if g), "")
        if schema and table:
            out[table.lower()] = schema
    return out


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


# ---------------------------------------------------------------------------
# EDW schema reader
# ---------------------------------------------------------------------------

def _find_rationalized_edw(outdir: Path, domain: str) -> Optional[Path]:
    rat_dir = outdir / "rationalized"
    if not rat_dir.exists():
        return None
    domain_safe = domain.lower().replace(" ", "_")
    matches = sorted(
        rat_dir.glob(f"rationalized_edw_{domain_safe}_*.json"),
        reverse=True,
    )
    return matches[0] if matches else None


def edw_schemas_from_rationalized(outdir: Path, domain: str) -> Dict[str, str]:
    """
    Read the most-recent ``rationalized_edw_<domain>_*.json`` and return
    ``{entity_name_lower: source_schema}`` for every entity that carries
    a schema.  Empty dict when nothing is available.
    """
    path = _find_rationalized_edw(outdir, domain)
    if not path:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: Dict[str, str] = {}
    for entity in data.get("entities", []):
        name = (entity.get("entity_name") or "").strip()
        if not name:
            continue
        info = entity.get("source_info") or {}
        # Some older runs used "schema", newer ones "source_schema"
        schema = info.get("source_schema") or info.get("schema") or ""
        if schema:
            out[name.lower()] = schema
    return out


# ---------------------------------------------------------------------------
# Ancillary DDL schema reader
# ---------------------------------------------------------------------------

def _ancillary_entry_for(config: AppConfig, source_id: str) -> Optional[Dict]:
    for entry in config.ancillary or []:
        if entry.get("source_id") == source_id:
            return entry
    return None


def ancillary_schemas_from_ddl(
    config: AppConfig,
    source_id: str,
) -> Dict[str, str]:
    """
    For an ancillary entry whose ``file_type == "ddl"``, locate the raw
    DDL file under ``input/business/cdm_<name>/ancillary/source/`` and
    return ``{table_lower: schema}`` from its CREATE TABLE statements.
    Returns ``{}`` when the source isn't a DDL file or the file can't be
    read.
    """
    entry = _ancillary_entry_for(config, source_id)
    if not entry:
        return {}
    if (entry.get("file_type") or "").lower() != "ddl":
        return {}
    filename = entry.get("file") or ""
    if not filename:
        return {}
    try:
        path = config_utils.resolve_ancillary_file(
            cdm_name=config.cdm.domain,
            filename=filename,
            preprocessed=False,
        )
    except Exception:
        return {}
    text = _read_text(path)
    if not text:
        return {}
    return extract_ddl_schemas(text)


# ---------------------------------------------------------------------------
# Ancillary attribute index — preserves original schema.table.column refs
# ---------------------------------------------------------------------------

def _parse_source_ref(s: str) -> Optional[Dict[str, str]]:
    """
    Parse a rationalized source-ref string into its parts.

    Expected formats (with or without the file prefix):
      "<file>::<schema>.<table>::<column>"
      "<file>::<table>::<column>"
      "<file>::<schema>.<table>"
      "<schema>.<table>.<column>"
      "<schema>.<table>"

    Returns dict with 'schema'/'table'/'column' (any may be empty), or
    None if the string can't be split sensibly.
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    parts = s.split("::")
    column = ""
    schema_table = ""
    if len(parts) >= 3:
        schema_table = parts[1]
        column = parts[2]
    elif len(parts) == 2:
        # could be "<file>::<schema>.<table>" or "<file>::<table>"
        schema_table = parts[1]
    else:
        # No "::" — assume "<schema>.<table>(.<column>)"
        schema_table = s
    schema = ""
    table = ""
    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
        # If table itself contains a dot, the last segment may be a column
        if "." in table and not column:
            table, column = table.rsplit(".", 1)
    else:
        table = schema_table
    return {
        "schema": schema.strip(),
        "table":  table.strip(),
        "column": column.strip(),
    }


def ancillary_attribute_index(
    outdir: Path,
    domain: str,
    source_id: str,
) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """
    Build a {(rationalized_entity_lower, rationalized_attr_lower):
              [{schema, table, column}, ...]} lookup for one ancillary
    source by parsing its rationalized JSON's per-attribute
    ``source_attribute`` list.

    This recovers the original schema.table.column references the
    rationalizer captures for each attribute even after it renames
    entities to business-friendly names.
    """
    rat_dir = outdir / "rationalized"
    if not rat_dir.exists():
        return {}
    domain_safe = domain.lower().replace(" ", "_")
    matches = sorted(
        rat_dir.glob(f"rationalized_{source_id}_{domain_safe}_*.json"),
        reverse=True,
    )
    if not matches:
        return {}
    try:
        data = json.loads(matches[0].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    # Tuple type alias hidden to avoid runtime cost
    out: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for entity in data.get("entities", []):
        ent = (entity.get("entity_name") or "").strip().lower()
        for attr in entity.get("attributes", []) or []:
            aname = (attr.get("attribute_name") or "").strip().lower()
            if not ent or not aname:
                continue
            raw_refs = attr.get("source_attribute") or []
            if isinstance(raw_refs, str):
                raw_refs = [raw_refs]
            entries: List[Dict[str, str]] = []
            for ref in raw_refs:
                parsed = _parse_source_ref(ref)
                if parsed and (parsed["table"] or parsed["column"]):
                    entries.append(parsed)
            if entries:
                out[(ent, aname)] = entries
    return out


# ---------------------------------------------------------------------------
# Public resolver
# ---------------------------------------------------------------------------

class SchemaResolver:
    """
    Build once per Mapping-tab generation; lookup per row.

    Usage:
        sr = SchemaResolver(config, outdir)
        schema = sr.resolve(source_key="edw", source_entity="PaidHistory")
    """

    def __init__(self, config: AppConfig, outdir: Path):
        self.config = config
        self.outdir = outdir
        self._fallback = (config.mapping.source_schema or "").strip()
        # Per source_key -> {entity_lower: schema}
        self._maps: Dict[str, Dict[str, str]] = {}

    def _lookup_for(self, source_key: str) -> Dict[str, str]:
        if source_key in self._maps:
            return self._maps[source_key]
        if source_key.lower() == "edw":
            self._maps[source_key] = edw_schemas_from_rationalized(
                self.outdir, self.config.cdm.domain
            )
        elif source_key.startswith("ancillary"):
            self._maps[source_key] = ancillary_schemas_from_ddl(
                self.config, source_key
            )
        else:
            self._maps[source_key] = {}
        return self._maps[source_key]

    def resolve(self, source_key: str, source_entity: str) -> str:
        """Return the schema for one source row, or the fallback when missing."""
        if source_entity:
            lookup = self._lookup_for(source_key)
            schema = lookup.get(source_entity.lower())
            if schema:
                return schema
        return self._fallback

    def stats(self) -> Dict[str, int]:
        """How many entries each source contributed to its lookup."""
        return {src: len(m) for src, m in self._maps.items()}


# ---------------------------------------------------------------------------
# Public helpers for tab generators
# ---------------------------------------------------------------------------

def format_ancillary_source_refs(
    ancillary_index: Optional[Dict[Tuple[str, str], List[Dict[str, str]]]],
    lineage_entries,
    include_schema: bool = True,
) -> List[str]:
    """
    Render an ancillary source's lineage entries as a list of
    formatted source-reference strings, using the per-attribute index
    to recover original source references when the rationalizer
    renamed entities.

    With ``include_schema=True`` (default) the format is
    ``schema.table.column`` (or ``table.column`` when schema is empty).
    With ``include_schema=False`` the schema is dropped — useful on
    tabs where schema is just clutter (Data Dictionary, Cross-Reference)
    while the Mapping tab keeps schema for Collibra ingestion.

    When the index is empty or has no match for a given
    (rationalized_entity, rationalized_attribute), falls back to the
    rationalized "<source_entity>.<source_attribute>" rendering so
    tabs degrade gracefully on older runs without an attr-index.

    Args:
        ancillary_index: result of ancillary_attribute_index() for this
            specific source — or None to force pure-fallback behaviour.
        lineage_entries: the raw list of source_lineage entries for one
            ancillary source on one CDM attribute.
        include_schema: if False, drop the schema prefix from the
            rendered string.

    Returns: ordered list of formatted strings (deduped while preserving
    first-seen order).
    """
    if not isinstance(lineage_entries, list):
        if isinstance(lineage_entries, dict):
            lineage_entries = [lineage_entries]
        else:
            return []

    out: List[str] = []
    seen: set = set()

    def _add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    for e in lineage_entries:
        if not isinstance(e, dict):
            continue
        rent = (e.get("source_entity") or "").strip()
        rattr = (e.get("source_attribute") or "").strip()

        # Try the index first
        hit_originals = []
        if ancillary_index is not None:
            hit_originals = ancillary_index.get((rent.lower(), rattr.lower()), [])

        if hit_originals:
            for o in hit_originals:
                schema = (o.get("schema") or "").strip()
                table  = (o.get("table") or "").strip()
                column = (o.get("column") or "").strip()
                use_schema = include_schema and schema
                if use_schema and table and column:
                    _add(f"{schema}.{table}.{column}")
                elif table and column:
                    _add(f"{table}.{column}")
                elif column:
                    _add(column)
        else:
            # Fallback: the rationalized form
            if rent and rattr:
                _add(f"{rent}.{rattr}")
            elif rattr:
                _add(rattr)

    return out
