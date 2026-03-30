# src/cdm_full/run_postprocess.py
"""
Post-Processing Orchestrator for Full CDM

Runs post-processing steps after Full CDM is built.
Designed to be expandable with additional post-process steps.

Current steps:
1. Rematch         - Focused second-pass on no-reason unmapped fields
2. Sensitivity     - PHI/PII flagging (AI-driven)
3. CDE             - Critical Data Element identification (AI-driven)

Future steps may include:
- Data quality rules generation
- Lineage documentation
- Naming standards validation
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient
from src.cdm_full.postprocess_sensitivity import run_sensitivity_postprocess
from src.cdm_full.postprocess_cde import run_cde_postprocess
from src.cdm_full.postprocess_rematch import run_rematch_postprocess
from src.cdm_full.postprocess_field_codes import run_field_codes_postprocess


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def find_full_cdm(outdir: Path, domain: str) -> Optional[Path]:
    """Find latest Full CDM file."""
    domain_safe = domain.lower().replace(' ', '_')
    full_cdm_dir = outdir / "full_cdm"

    if not full_cdm_dir.exists():
        return None

    pattern = f"cdm_{domain_safe}_full_*.json"
    matches = list(full_cdm_dir.glob(pattern))

    if not matches:
        return None

    matches.sort(reverse=True)
    return matches[0]


def find_gaps_file(outdir: Path, domain: str) -> Optional[Path]:
    """Find the latest gaps file for the domain."""
    domain_safe = domain.lower().replace(' ', '_')
    full_cdm_dir = outdir / "full_cdm"

    if not full_cdm_dir.exists():
        return None

    # Prefer updated rematch gaps over original if both exist
    matches = sorted(
        full_cdm_dir.glob(f"gaps_{domain_safe}_*.json"),
        reverse=True
    )
    return matches[0] if matches else None


def prompt_yes_no(prompt: str, default: str = "Y") -> bool:
    """Simple Y/N prompt."""
    suffix = "[Y/n]" if default.upper() == "Y" else "[y/N]"
    response = input(f"   {prompt} {suffix}: ").strip().upper()
    if not response:
        return default.upper() == "Y"
    return response == "Y"


# =============================================================================
# POST-PROCESS REGISTRY
# =============================================================================

# Registry of available post-process steps.
# Each entry: (key, display_name, function, requires_llm, needs_gaps, needs_context)
#
# needs_gaps    : step receives gaps_path, outdir, domain
# needs_context : step receives outdir, domain (but not gaps_path)
# neither       : step receives only (cdm, llm, dry_run)
#
# ORDER MATTERS:
#   rematch     → runs first so downstream steps benefit from improved lineage
#   field_codes → no LLM, enriches CDM and updates Excel in-place
#   sensitivity → flags PHI/PII so CDE step can reference those flags
#   cde         → uses sensitivity flags and full lineage

POSTPROCESS_STEPS = [
    (
        "rematch",
        "Unmapped Field Re-Match (second-pass for no-reason unmapped)",
        run_rematch_postprocess,
        True,   # requires_llm
        True,   # needs_gaps
        False,  # needs_context
    ),
    (
        "field_codes",
        "Field Code Enrichment — NCPDP / EDW (no AI)",
        run_field_codes_postprocess,
        False,  # requires_llm
        False,  # needs_gaps
        True,   # needs_context — requires outdir + domain to find rationalized files and Excel
    ),
    (
        "sensitivity",
        "Sensitivity Analysis (PHI/PII flagging)",
        run_sensitivity_postprocess,
        True,   # requires_llm
        False,  # needs_gaps
        False,  # needs_context
    ),
    (
        "cde",
        "Critical Data Element Identification",
        run_cde_postprocess,
        True,   # requires_llm
        False,  # needs_gaps
        False,  # needs_context
    ),
]


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_postprocessing(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient] = None,
    cdm_file: Optional[Path] = None,
    steps_to_run: Optional[List[str]] = None,
    dry_run: bool = False
) -> Optional[Path]:
    """
    Run post-processing on Full CDM.

    Args:
        config:       App configuration
        outdir:       Base output directory (e.g., output/plan)
        llm:          LLM client (required for AI steps)
        cdm_file:     Path to Full CDM (auto-finds if None)
        steps_to_run: List of step keys to run (runs all if None)
        dry_run:      If True, show prompts only

    Returns:
        Path to updated CDM file, or None if no changes
    """

    # --- Find Full CDM ---
    if cdm_file is None:
        cdm_file = find_full_cdm(outdir, config.cdm.domain)

    if not cdm_file or not cdm_file.exists():
        print(f"   No Full CDM found. Run Step 6 (Build Full CDM) first.")
        return None

    print(f"   Source CDM : {cdm_file.name}")

    # --- Find gaps file (used by rematch step) ---
    gaps_path = find_gaps_file(outdir, config.cdm.domain)
    if gaps_path:
        print(f"   Gaps file  : {gaps_path.name}")
    else:
        print(f"   Gaps file  : not found (rematch step will be skipped)")

    # --- Load CDM ---
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)

    entity_count = len(cdm.get("entities", []))
    attr_count   = sum(len(e.get("attributes", [])) for e in cdm.get("entities", []))
    print(f"   Entities   : {entity_count}  |  Attributes: {attr_count}")

    # --- Determine which steps to run ---
    if steps_to_run is None:
        steps_to_run = [step[0] for step in POSTPROCESS_STEPS]

    # --- Run selected steps ---
    modified = False

    for step_key, step_desc, step_func, requires_llm, needs_gaps, needs_context in POSTPROCESS_STEPS:

        if step_key not in steps_to_run:
            continue

        # LLM check
        if requires_llm and llm is None and not dry_run:
            print(f"\n   ⚠️  Skipping '{step_desc}' — LLM required but not initialised")
            continue

        # Gaps check
        if needs_gaps and not gaps_path and not dry_run:
            print(f"\n   ⚠️  Skipping '{step_desc}' — no gaps file found")
            continue

        print(f"\n   {'='*50}")
        print(f"   RUNNING: {step_desc}")
        print(f"   {'='*50}")

        if needs_gaps:
            cdm = step_func(
                cdm,
                llm,
                dry_run=dry_run,
                gaps_path=gaps_path,
                outdir=outdir,
                domain=config.cdm.domain
            )
        elif needs_context:
            # field_codes and similar steps need outdir/domain but not gaps
            cdm = step_func(
                cdm,
                llm,
                dry_run=dry_run,
                outdir=outdir,
                domain=config.cdm.domain
            )
        else:
            cdm = step_func(cdm, llm, dry_run)

        modified = True

        # After rematch runs, refresh the gaps path so subsequent steps
        # pick up the newly written gaps file (with resolved entries removed)
        if step_key == "rematch" and not dry_run:
            refreshed = find_gaps_file(outdir, config.cdm.domain)
            if refreshed and refreshed != gaps_path:
                gaps_path = refreshed
                print(f"   ↻ Gaps file refreshed: {gaps_path.name}")

    # --- Save updated CDM ---
    if modified and not dry_run:
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.lower().replace(' ', '_')

        output_dir = outdir / "full_cdm"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"cdm_{domain_safe}_full_{timestamp}.json"

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cdm, f, indent=2, default=str)

        print(f"\n   ✓ Updated CDM saved: {output_file.name}")
        return output_file

    return cdm_file


# =============================================================================
# INTERACTIVE ENTRY POINT  (called from orchestrator)
# =============================================================================

def interactive_postprocessing(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient] = None,
    dry_run: bool = False
) -> Optional[Path]:
    """
    Interactive post-processing with per-step Y/N prompts.
    Called from cdm_orchestrator.py after Step 6.

    Args:
        config:   App configuration
        outdir:   Base output directory
        llm:      LLM client
        dry_run:  If True, show prompts only

    Returns:
        Path to updated CDM file, or None
    """

    # Find Full CDM
    cdm_file = find_full_cdm(outdir, config.cdm.domain)
    if not cdm_file:
        print(f"\n   ⚠️  No Full CDM found — run Step 6 first")
        return None

    print(f"\n   Source: {cdm_file.name}")

    # Check for gaps file and warn if missing (affects rematch)
    gaps_path = find_gaps_file(outdir, config.cdm.domain)
    if not gaps_path:
        print(f"   ⚠️  No gaps file found — Rematch step will be unavailable")

    # --- Per-step selection ---
    print(f"\n   {'-'*50}")
    print(f"   SELECT POST-PROCESSING STEPS")
    print(f"   {'-'*50}")

    steps_to_run = []

    for step_key, step_desc, _, requires_llm, needs_gaps, needs_context in POSTPROCESS_STEPS:
        notes = []
        if requires_llm:
            notes.append("AI")
        if needs_gaps and not gaps_path:
            notes.append("gaps file missing — will skip")
        note_str = f"  ({', '.join(notes)})" if notes else ""

        if prompt_yes_no(f"Run: {step_desc}?{note_str}", default="Y"):
            steps_to_run.append(step_key)

    if not steps_to_run:
        print("\n   No post-processing steps selected.")
        return None

    # --- Run ---
    print(f"\n   {'-'*50}")
    print(f"   RUNNING POST-PROCESSING  [{', '.join(steps_to_run)}]")
    print(f"   {'-'*50}")

    return run_postprocessing(
        config=config,
        outdir=outdir,
        llm=llm,
        cdm_file=cdm_file,
        steps_to_run=steps_to_run,
        dry_run=dry_run
    )