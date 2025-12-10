# src/cdm_full/run_postprocess.py
"""
Post-Processing Orchestrator for Full CDM

Runs post-processing steps after Full CDM is built.
Designed to be expandable with additional post-process steps.

Current steps:
1. Sensitivity Analysis (PHI/PII flagging) - AI-driven
2. CDE Identification - AI-driven

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

# Registry of available post-process steps
# Each entry: (name, description, function, requires_llm)
# ORDER MATTERS: sensitivity should run before CDE so PHI/PII flags are available
POSTPROCESS_STEPS = [
    (
        "sensitivity",
        "Sensitivity Analysis (PHI/PII flagging)",
        run_sensitivity_postprocess,
        True  # Requires LLM
    ),
    (
        "cde",
        "Critical Data Element Identification",
        run_cde_postprocess,
        True  # Requires LLM
    ),
    # Future steps:
    # ("dq_rules", "Data Quality Rules Generation", run_dq_postprocess, True),
    # ("naming", "Naming Standards Validation", run_naming_postprocess, False),
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
        config: App configuration
        outdir: Base output directory (e.g., output/plan)
        llm: LLM client (required for AI steps)
        cdm_file: Path to Full CDM (auto-finds if None)
        steps_to_run: List of step names to run (runs all if None)
        dry_run: If True, show prompts only
    
    Returns:
        Path to updated CDM file, or None if no changes
    """
    
    # Find Full CDM
    if cdm_file is None:
        cdm_file = find_full_cdm(outdir, config.cdm.domain)
    
    if not cdm_file or not cdm_file.exists():
        print(f"   No Full CDM found. Run Step 6 (Build Full CDM) first.")
        return None
    
    print(f"   Source: {cdm_file.name}")
    
    # Load CDM
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    entity_count = len(cdm.get("entities", []))
    print(f"   Entities: {entity_count}")
    
    # Determine which steps to run
    if steps_to_run is None:
        steps_to_run = [step[0] for step in POSTPROCESS_STEPS]
    
    # Run selected steps
    modified = False
    
    for step_name, step_desc, step_func, requires_llm in POSTPROCESS_STEPS:
        if step_name not in steps_to_run:
            continue
        
        # Check LLM requirement
        if requires_llm and llm is None and not dry_run:
            print(f"   Warning: Skipping {step_desc} - requires LLM")
            continue
        
        # Run step
        cdm = step_func(cdm, llm, dry_run)
        modified = True
    
    # Save updated CDM
    if modified and not dry_run:
        # Save with new timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.lower().replace(' ', '_')
        
        output_dir = outdir / "full_cdm"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"cdm_{domain_safe}_full_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cdm, f, indent=2, default=str)
        
        print(f"\n   Updated CDM saved: {output_file.name}")
        return output_file
    
    return cdm_file


def interactive_postprocessing(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient] = None,
    dry_run: bool = False
) -> Optional[Path]:
    """
    Interactive post-processing with Y/N prompts per step.
    Called from orchestrator.
    
    Args:
        config: App configuration
        outdir: Base output directory
        llm: LLM client
        dry_run: If True, show prompts only
    
    Returns:
        Path to updated CDM file, or None
    """
    
    # Find Full CDM
    cdm_file = find_full_cdm(outdir, config.cdm.domain)
    
    if not cdm_file:
        print(f"\n   No Full CDM found. Run Step 6 (Build Full CDM) first.")
        return None
    
    print(f"\n   Source: {cdm_file.name}")
    
    # Prompt for each step
    print(f"\n   {'-'*50}")
    print(f"   SELECT POST-PROCESSING STEPS")
    print(f"   {'-'*50}")
    
    steps_to_run = []
    
    for step_name, step_desc, _, requires_llm in POSTPROCESS_STEPS:
        llm_note = " (requires AI)" if requires_llm else ""
        if prompt_yes_no(f"Run {step_desc}?{llm_note}", default="Y"):
            steps_to_run.append(step_name)
    
    if not steps_to_run:
        print("\n   No post-processing steps selected.")
        return None
    
    # Run selected steps
    print(f"\n   {'-'*50}")
    print(f"   RUNNING POST-PROCESSING")
    print(f"   {'-'*50}")
    
    result = run_postprocessing(
        config=config,
        outdir=outdir,
        llm=llm,
        cdm_file=cdm_file,
        steps_to_run=steps_to_run,
        dry_run=dry_run
    )
    
    return result
