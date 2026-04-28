# src/config/config_gen_mapping.py
"""
Mapping-block configuration for Step 0.

Builds (or refreshes) the top-level ``mapping`` section in config.json:

  "mapping": {
    "source_application": "<APP_NAME>",
    "source_schema":      "<fallback>",
    "mapping_sources":    ["edw", "ancillary-<slug>", ...]
  }

Behavior:
  - Iterates ancillary entries with file_type == "ddl" and asks the user
    whether each one is a Collibra mapping source. Files the user
    confirms get added to mapping_sources via their source_id.
  - Auto-adds "edw" to mapping_sources when input_files.edw is non-empty.
  - Prompts once for source_application (defaults to the CDM domain).
  - Leaves source_schema as whatever the user already had — if absent,
    sets it to "" so the auto-extraction in the Mapping tab handles it.
  - Existing mapping_sources entries are preserved when the user says N
    to a particular DDL ancillary; the prompt only adds, never removes.

No LLM calls. Pure prompts + config manipulation.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .config_gen_core import prompt_user_choice


def _is_ddl_ancillary(entry: Dict[str, Any]) -> bool:
    return (entry.get("file_type") or "").lower() == "ddl"


def run_mapping_config(source_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Walk the user through Mapping-tab configuration. Returns the new
    mapping block as a dict. Caller is responsible for writing it back
    into the config.

    Returns the existing mapping block unchanged if the user opts out.
    """
    print(f"\n   {'-'*40}")
    print(f"   MAPPING TAB CONFIGURATION (Collibra)")
    print(f"   {'-'*40}")

    existing = dict(source_config.get("mapping") or {})

    # Build the mapping_sources list. Start from whatever's there so we
    # don't lose user-added entries.
    sources: List[str] = list(existing.get("mapping_sources") or [])

    # --- EDW: auto-add if EDW input files are configured ---
    edw_files = (source_config.get("input_files") or {}).get("edw") or []
    if edw_files and "edw" not in sources:
        sources.append("edw")
        print(f"   ✓ EDW configured ({len(edw_files)} files) — added 'edw' to mapping_sources")
    elif "edw" in sources and not edw_files:
        print(f"   ⚠️  'edw' is in mapping_sources but no EDW input files are configured")
    elif edw_files:
        print(f"   ✓ EDW already in mapping_sources")
    else:
        print(f"   • No EDW input files — skipping EDW")

    # --- DDL ancillaries: ask one Y/N per file ---
    ancillaries = (source_config.get("input_files") or {}).get("ancillary") or []
    ddl_ancillaries = [a for a in ancillaries if _is_ddl_ancillary(a)]

    if not ddl_ancillaries:
        print(f"   • No DDL-type ancillaries to consider")
    else:
        print(f"\n   {len(ddl_ancillaries)} DDL ancillary file(s) detected.")
        print(f"   Mark each one as a mapping source (Collibra source-to-target):")
        for entry in ddl_ancillaries:
            sid = entry.get("source_id") or ""
            fname = entry.get("file") or ""
            already = sid in sources
            default = "Y" if already else "N"
            label = f"     Use '{fname}' ({sid}) as a mapping source?"
            if prompt_user_choice(label, default=default):
                if sid and sid not in sources:
                    sources.append(sid)
            else:
                # If the user says N and it was already in sources, drop it
                if sid in sources:
                    sources.remove(sid)

    # --- source_application ---
    domain = (source_config.get("cdm") or {}).get("domain") or ""
    default_app = existing.get("source_application") or domain
    while True:
        prompt = (
            f"\n   Source Application name [{default_app}]: "
            if default_app else "\n   Source Application name: "
        )
        raw = input(prompt).strip()
        app = raw or default_app
        if app:
            break
        print(f"      ⚠️  source_application is required — please enter a value")

    # --- source_schema (fallback only; auto-extraction handles per-row) ---
    schema = existing.get("source_schema", "")

    block: Dict[str, Any] = {
        "source_application": app,
        "source_schema":      schema,
        "mapping_sources":    sources,
    }

    print(f"\n   Mapping block:")
    print(f"     source_application: {app!r}")
    print(f"     source_schema:      {schema!r} (fallback; per-row auto-extracted)")
    print(f"     mapping_sources:    {sources}")
    return block
