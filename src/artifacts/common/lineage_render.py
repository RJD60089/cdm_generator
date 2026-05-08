# src/artifacts/common/lineage_render.py
"""
Shared rendering helpers for source_lineage entries.

Used by tab_mapping.py and tab_collibra.py — anywhere we need to choose
which SQL identifier (OLTP / NI / NP) to display for an EDW lineage entry.

EDW lineage entries enriched by `postprocess_edw_lineage` carry these
real SQL identifier fields:
    source_table  / source_column  - OLTP layer
    ni_table      / ni_column      - NI staging layer
    np_table      / np_column      - NP persisted layer

The preference tuples below are the global render policy. The first
non-empty value wins. Defaulting to the OLTP layer keeps each row
coherent with the OLTP schema (e.g. SQLMGR) returned by SchemaResolver
for EDW. Edit these tuples to switch the layer globally —
e.g. ("np_table", "ni_table", "source_table") to render the persisted
layer instead.

When none of these keys are present (un-enriched CDMs), callers fall
back to the rationalized business-friendly names from `source_entity` /
`source_attribute`.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable


EDW_TABLE_PREFERENCE  = ("source_table", "np_table",  "ni_table")
EDW_COLUMN_PREFERENCE = ("source_column", "np_column", "ni_column")


def first_present(d: Dict[str, Any], keys: Iterable[str]) -> str:
    """Return the first non-empty string value for any of `keys` in `d`."""
    for k in keys:
        v = d.get(k)
        if v:
            return str(v)
    return ""
