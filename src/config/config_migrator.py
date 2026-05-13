"""
Config Migrator

Runs at orchestrator startup before any pipeline step.  Detects legacy
config layouts that predate per-source processing_mode and stamps
sensible defaults so old configs keep working without manual edits.

Responsibilities:
  * Ensure `processing_modes` dict exists at config root with one entry
    per configured non-ancillary source type (default: "refiner").
  * Ensure every ancillary entry has a `processing_mode` field (default:
    "refiner", matching the prior implicit behavior).
  * Validate: at most one source across all types may declare
    `processing_mode == "foundational"`.  Hard error on violation.
  * Warn (but accept) if `processing_mode == "driver"` coexists with a
    foundational source — driver becomes a no-op in anchored mode.

Behavior:
  * Reads the config JSON, applies migrations in memory, and writes back
    only if at least one field changed.  Idempotent on already-migrated
    configs.
  * Prints a one-line summary on first migration; silent on subsequent
    runs.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple


# Non-ancillary source-type keys that may carry a processing_mode entry.
_NON_ANCILLARY_TYPES = ("fhir", "ncpdp", "guardrails", "glue", "edw")


def _has_section(input_files: dict, source_type: str) -> bool:
    """Return True if the config has any files for the given source type."""
    if source_type == "fhir":
        return bool(input_files.get("fhir_igs"))
    if source_type == "ncpdp":
        return bool(
            input_files.get("ncpdp_general_standards")
            or input_files.get("ncpdp_script_standards")
        )
    return bool(input_files.get(source_type))


def migrate_config(config_path: Path) -> Tuple[bool, List[str]]:
    """Apply migrations to the config file at config_path.

    Args:
        config_path: Path to the JSON config file.

    Returns:
        (changed, messages) — `changed` is True if the file was rewritten;
        `messages` is a list of human-readable migration notes.

    Raises:
        SystemExit: if validation fails (e.g., multiple foundational
            sources configured).  The orchestrator should not proceed.
    """
    if not config_path.exists():
        return False, [f"Config not found: {config_path}"]

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    changed = False
    messages: List[str] = []

    # --- 1. Ensure processing_modes dict exists for non-ancillary sources.
    input_files = data.get("input_files") or {}
    modes = data.get("processing_modes")
    if not isinstance(modes, dict):
        modes = {}
        data["processing_modes"] = modes
        changed = True

    for st in _NON_ANCILLARY_TYPES:
        if _has_section(input_files, st) and st not in modes:
            modes[st] = "refiner"
            changed = True
            messages.append(f"{st}: defaulted processing_mode → refiner")

    # --- 2. Ensure every ancillary entry has a processing_mode.
    ancillary = input_files.get("ancillary") or []
    for entry in ancillary:
        if not isinstance(entry, dict):
            continue
        if not entry.get("processing_mode"):
            entry["processing_mode"] = "refiner"
            changed = True
            sid = entry.get("source_id", "<unnamed>")
            messages.append(f"{sid}: defaulted processing_mode → refiner")

    # --- 3. Validate singleton foundational.
    foundational_ids: List[str] = []
    for st in _NON_ANCILLARY_TYPES:
        if modes.get(st) == "foundational":
            foundational_ids.append(st)
    for entry in ancillary:
        if isinstance(entry, dict) and entry.get("processing_mode") == "foundational":
            foundational_ids.append(entry.get("source_id", "<ancillary>"))

    if len(foundational_ids) > 1:
        print(
            f"\n❌ Config error: multiple foundational sources configured: "
            f"{', '.join(foundational_ids)}",
            file=sys.stderr,
        )
        print(
            "   At most one source may declare processing_mode=\"foundational\". "
            "Anchored mode requires a single CDM author.",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- 4. Driver + foundational coexistence (warn only).
    anchored = len(foundational_ids) == 1
    if anchored:
        driver_ids: List[str] = []
        for entry in ancillary:
            if isinstance(entry, dict) and entry.get("processing_mode") == "driver":
                driver_ids.append(entry.get("source_id", "<ancillary>"))
        if driver_ids:
            messages.append(
                f"NOTE: driver mode is a no-op in anchored mode "
                f"(ignored: {', '.join(driver_ids)})"
            )

    # --- 5. Persist if anything changed.
    if changed:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    return changed, messages


def maybe_migrate(config_path: Path) -> None:
    """Run migrations and print a one-line summary on the first migration.

    Intended for orchestrator startup.  Silent on already-migrated
    configs.  Exits the process if validation fails.
    """
    changed, messages = migrate_config(config_path)
    if changed and messages:
        print(f"\n   ⚙️  Config migrated: {len(messages)} default(s) stamped")
        for msg in messages:
            print(f"      • {msg}")
    elif messages:
        # No changes but informational notes (e.g., driver-in-anchored warning).
        for msg in messages:
            print(f"   ℹ️  {msg}")
