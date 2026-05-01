# cdm_orchestrator.py
"""
Unified CDM Generation Orchestrator

Interactive flow:
  1. Config generation or refresh (Step 0)
  2. Dry run or live mode selection
  3. Model selection (if live)
  4. Step-by-step execution with granular control

Steps:
  0 - Config Generation (FHIR, NCPDP, Glue, EDW, Ancillary analysis)
  1 - Rationalize Input Sources (FHIR, NCPDP, Guardrails, Glue, EDW, Ancillary)
  2 - Build Foundational CDM (CDM JSON)
  3 - Refinement - Consolidation (merge overlapping entities)
  4 - Refinement - PK/FK Validation (validate keys & relationships)
  5 - Build Full CDM (source mapping + lineage; includes Refiner gate)
      5-POST - Post-Processing (interactive per-step menu):
               • Rematch     — second-pass on no-reason unmapped fields
               • Ancillary   — ancillary source enrichment
               • Sensitivity — PHI/PII flagging
               • CDE         — Critical Data Element identification
  5p - Post-Processing ONLY (standalone re-run)
  6 - Generate Artifacts (DDL, LucidChart CSV, Excel, Word)
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Use shared config utilities
from src.config import (
    load_config,
    find_latest_config,
    find_base_config,
    get_config_dir,
    safe_cdm_name,
)
from src.core.llm_client import LLMClient
from src.core.model_selector import MODEL_OPTIONS, select_model, prompt_user

load_dotenv()


def find_existing_full_cdm(base_outdir: Path, domain: str) -> Path | None:
    """Check if Full CDM exists for this domain."""
    domain_safe = domain.lower().replace(' ', '_')
    full_cdm_dir = base_outdir / "full_cdm"
    
    if not full_cdm_dir.exists():
        return None
    
    pattern = f"cdm_{domain_safe}_full_*.json"
    matches = list(full_cdm_dir.glob(pattern))
    
    if not matches:
        return None
    
    matches.sort(reverse=True)
    return matches[0]


def run_step0_config_generation(cdm_name: str, llm: Optional[LLMClient] = None, dry_run: bool = False) -> Optional[Path]:
    """Run Step 0: Config Generation.
    
    Args:
        cdm_name: CDM name
        llm: LLM client (optional for dry run)
        dry_run: If True, save prompts only
        
    Returns:
        Path to config file, or None if not found
    """
    from src.config.config_generator import ConfigGenerator
    
    print(f"\n{'='*60}")
    print(f"STEP 0: CONFIG GENERATION")
    print(f"{'='*60}")
    
    # Check current config state
    latest = find_latest_config(cdm_name)
    base = find_base_config(cdm_name)
    
    if latest:
        print(f"\n   Source config: {latest.name}")
    elif base:
        print(f"\n   Source config: {base.name} (base)")
    else:
        print(f"\n   ❌ No config found for CDM: {cdm_name}")
        config_dir = get_config_dir(cdm_name)
        safe_name = safe_cdm_name(cdm_name)
        print(f"      Expected: {config_dir}/config_{safe_name}.json")
        sys.exit(1)
    
    # Run config generation
    generator = ConfigGenerator(cdm_name, llm_client=llm)
    new_config = generator.run(dry_run=dry_run)
    
    # Return new config if generated, otherwise source
    if new_config:
        return new_config
    
    return latest or base


def run_auto(
    cdm_name: str,
    model_key: str = "gpt-5",
    workers: int = 16,
    steps_to_run: Optional[set] = None,
    gap_threshold: float = 0.8,
    reject_all_gaps: bool = False,
) -> None:
    """Unattended end-to-end CDM build (Steps 1–6) using config-driven defaults.

    Preconditions:
      Step 0 (config generation) has already produced a valid config for
      <cdm_name>.  Auto mode does NOT run config-gen — it fails fast with
      a clear error if no config is found.

    Pipeline behaviour:
      Step 1  — Run every rationalizer the config has data for, in parallel.
      Step 2  — Build foundational CDM (single LLM call).
      Step 3  — Consolidation refinement: auto-reject all recommendations.
      Step 4  — PK/FK validation: auto-reject all findings.
      Step 5  — Build Full CDM with `Remap All` mode and gap analysis.  The
                refiner gate (if triggered) uses confidence-threshold review
                via `auto_threshold` (default 0.8).
      Step 5p — Run every post-process step (rematch, field_codes, ancillary,
                sensitivity, cde).
      Step 6  — Generate every artifact (Excel, DDL SQL, LucidChart CSV, Word
                DDL) plus AI rule consolidation.

    Args:
        cdm_name: CDM name matching an existing config.
        model_key: Key into MODEL_OPTIONS — default 'gpt-5'.
        workers: Concurrent LLM workers for per-entity match generation
            and rule consolidation.  Default 16 (assumes Tier 4 OpenAI).
        steps_to_run: Set of step ints (1–6) to execute.  Default = all.
        gap_threshold: Confidence threshold for auto-approving gap-driven
            refinement recommendations (0.0–1.0).  Default 0.8.
        reject_all_gaps: If True, sets the threshold to 1.01 — rejects all
            gap recommendations.  Equivalent to --reject-all-gaps CLI flag.
    """
    if steps_to_run is None:
        steps_to_run = {1, 2, 3, 4, 5, 6}

    if reject_all_gaps:
        gap_threshold = 1.01  # > 1.0 means nothing passes

    print(f"\n{'='*60}")
    print(f"CDM AUTO ORCHESTRATION (unattended)")
    print(f"{'='*60}")
    print(f"   CDM         : {cdm_name}")
    print(f"   Model       : {model_key}")
    print(f"   Workers     : {workers}")
    print(f"   Steps       : {sorted(steps_to_run)}")
    print(f"   Gap thresh. : {'reject-all' if reject_all_gaps else f'{gap_threshold:.0%}'}")
    print(f"{'='*60}")

    # --- Find existing config (auto mode does NOT run Step 0) ---
    config_file = find_latest_config(cdm_name) or find_base_config(cdm_name)
    if not config_file:
        print(f"\n❌ No config found for CDM: {cdm_name}", file=sys.stderr)
        print(f"   Auto mode requires Step 0 to have already run.", file=sys.stderr)
        print(f"   Run interactively first: python cdm_orchestrator.py {cdm_name}", file=sys.stderr)
        sys.exit(1)
    print(f"\n   Config: {config_file.name}")

    config = load_config(str(config_file))
    print(f"   Domain: {config.cdm.domain}")

    # --- Build LLM client ---
    if model_key not in MODEL_OPTIONS:
        print(f"\n❌ Unknown model: {model_key}", file=sys.stderr)
        print(f"   Available: {', '.join(MODEL_OPTIONS.keys())}", file=sys.stderr)
        sys.exit(1)
    model_config = MODEL_OPTIONS[model_key]
    llm = LLMClient(
        model=model_config['model'],
        base_url=model_config['base_url'](),
        temperature=0.2,
        timeout=1800,
    )
    print(f"   LLM   : {llm.model}")

    base_outdir = Path(config.output.directory)
    base_outdir.mkdir(parents=True, exist_ok=True)

    # ============================================================
    # STEP 1: RATIONALIZE — parallel by source
    # ============================================================
    if 1 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 1: INPUT RATIONALIZATION (auto)")
        print(f"{'='*60}")

        rationalized_outdir = base_outdir / "rationalized"
        rationalized_outdir.mkdir(parents=True, exist_ok=True)

        from concurrent.futures import ThreadPoolExecutor, as_completed
        from functools import partial
        from src.rationalizers import (
            run_fhir_rationalization,
            run_ncpdp_rationalization,
            run_guardrails_rationalization,
            run_glue_rationalization,
            run_edw_rationalization,
        )
        from src.rationalizers.rationalize_ancillary import run_ancillary_rationalization

        common_kwargs = dict(
            config=config,
            outdir=rationalized_outdir,
            llm=llm,
            dry_run=False,
            config_path=str(config_file),
        )

        tasks = []
        if config.has_fhir():
            tasks.append(("FHIR",       partial(run_fhir_rationalization,       **common_kwargs)))
        if config.has_ncpdp():
            tasks.append(("NCPDP",      partial(run_ncpdp_rationalization,      **common_kwargs)))
        if config.has_guardrails():
            tasks.append(("Guardrails", partial(run_guardrails_rationalization, **common_kwargs)))
        if config.has_glue():
            tasks.append(("Glue",       partial(run_glue_rationalization,       **common_kwargs)))
        if config.has_edw():
            tasks.append(("EDW",        partial(run_edw_rationalization,        **common_kwargs)))
        if config.has_ancillary():
            tasks.append(("Ancillary",  partial(run_ancillary_rationalization,  **common_kwargs)))

        if not tasks:
            print("   ⚠️  No rationalizable sources found in config — skipping Step 1")
        elif len(tasks) == 1:
            label, fn = tasks[0]
            print(f"\n   {label} (single source — running inline)")
            fn()
        else:
            print(f"\n   Running {len(tasks)} rationalizers in parallel: {', '.join(t[0] for t in tasks)}")
            failures = []
            with ThreadPoolExecutor(max_workers=len(tasks)) as ex:
                futures = {ex.submit(fn): label for label, fn in tasks}
                for fut in as_completed(futures):
                    label = futures[fut]
                    try:
                        fut.result()
                        print(f"   ✓ {label}: rationalization complete")
                    except Exception as e:
                        print(f"   ✗ {label}: FAILED — {e}")
                        failures.append((label, e))
            if failures:
                print(f"\n   ⚠️  {len(failures)} rationalizer(s) failed — auto mode continues, but downstream steps may have gaps")

    # ============================================================
    # STEP 2: BUILD FOUNDATIONAL CDM
    # ============================================================
    if 2 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 2: BUILD FOUNDATIONAL CDM (auto)")
        print(f"{'='*60}")

        cdm_outdir = base_outdir / "foundational_cdm"
        cdm_outdir.mkdir(parents=True, exist_ok=True)

        from src.cdm_builder.build_foundational_cdm import run_step3a
        run_step3a(
            config=config,
            outdir=cdm_outdir,
            llm=llm,
            dry_run=False,
            rationalized_dir=base_outdir / "rationalized",
        )

    # ============================================================
    # STEP 3: CONSOLIDATION (auto-reject all)
    # ============================================================
    if 3 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 3: CONSOLIDATION (auto — reject all)")
        print(f"{'='*60}")

        cdm_outdir = base_outdir / "foundational_cdm"
        from src.refinement.refine_consolidation import run_consolidation_refinement
        run_consolidation_refinement(
            config=config,
            cdm_file=None,
            outdir=cdm_outdir,
            llm=llm,
            dry_run=False,
            auto_reject_all=True,
        )

    # ============================================================
    # STEP 4: PK/FK VALIDATION (auto-reject all)
    # ============================================================
    if 4 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 4: PK/FK VALIDATION (auto — reject all)")
        print(f"{'='*60}")

        cdm_outdir = base_outdir / "foundational_cdm"
        from src.refinement.refine_pk_fk_validation import run_pk_fk_validation
        run_pk_fk_validation(
            config=config,
            cdm_file=None,
            outdir=cdm_outdir,
            llm=llm,
            dry_run=False,
            auto_reject_all=True,
        )

    # ============================================================
    # STEP 5: BUILD FULL CDM + post-processing
    # ============================================================
    if 5 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 5: BUILD FULL CDM (auto — Remap All, gap-thresh={gap_threshold:.0%})")
        print(f"{'='*60}")

        from src.cdm_full.build_full_cdm import run_build_full_cdm
        # Determine source types from config so we can pass an explicit
        # remap-all list rather than relying on the orchestrator's
        # interactive default.
        source_types = []
        if config.has_fhir():       source_types.append("fhir")
        if config.has_ncpdp():      source_types.append("ncpdp")
        if config.has_guardrails(): source_types.append("guardrails")
        if config.has_glue():       source_types.append("glue")
        if config.has_edw():        source_types.append("edw")
        if config.has_ancillary():
            for anc in config.input_files.ancillary or []:
                sid = anc.get("source_id")
                if sid:
                    source_types.append(sid)

        run_build_full_cdm(
            config=config,
            cdm_file=None,
            outdir=base_outdir,
            llm=llm,
            dry_run=False,
            sources_to_map=source_types,    # remap all
            skip_mapping=False,
            generate_cdm=True,
            run_gap_analysis=True,
            match_workers=workers,
            auto_threshold=gap_threshold,
        )

        # Step 5p — auto-run all post-processing steps non-interactively
        print(f"\n{'='*60}")
        print(f"STEP 5P: POST-PROCESSING (auto — run all)")
        print(f"{'='*60}")

        from src.cdm_full.run_postprocess import run_postprocessing
        # `steps_to_run=None` defaults to "all registered post-process
        # steps" inside run_postprocessing — see POSTPROCESS_STEPS list.
        run_postprocessing(
            config=config,
            outdir=base_outdir,
            llm=llm,
            cdm_file=None,
            steps_to_run=None,
            dry_run=False,
        )

    # ============================================================
    # STEP 6: GENERATE ARTIFACTS
    # ============================================================
    if 6 in steps_to_run:
        print(f"\n{'='*60}")
        print(f"STEP 6: GENERATE ARTIFACTS (auto — all)")
        print(f"{'='*60}")

        from src.artifacts.run_artifacts import run_artifact_generation, find_full_cdm
        cdm_file = find_full_cdm(base_outdir, config.cdm.domain)
        if not cdm_file:
            print(f"   ⚠️  No Full CDM found — skipping artifacts")
        else:
            run_artifact_generation(
                config=config,
                outdir=base_outdir,
                cdm_file=cdm_file,
                generate_excel_flag=True,
                generate_ddl_word_flag=False,    # off by default — Word generation is slower and rarely consumed
                generate_ddl_sql_flag=True,
                generate_lucidchart_flag=True,
                dialect="sqlserver",
                schema="dbo",
                run_rule_consolidation_flag=True,
                rule_consolidation_workers=workers,
                llm=llm,
                dry_run=False,
            )

    print(f"\n{'='*60}")
    print(f"AUTO ORCHESTRATION COMPLETE")
    print(f"{'='*60}\n")


def main():
    ap = argparse.ArgumentParser(
        description="CDM Generation Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cdm_orchestrator.py plan
  python cdm_orchestrator.py formulary
  python cdm_orchestrator.py "plan and benefit"

  # Unattended end-to-end (kicks off Steps 1–6, returns when done)
  python cdm_orchestrator.py plan --auto
  python cdm_orchestrator.py plan --auto --workers 16 --gap-threshold 0.8
  python cdm_orchestrator.py plan --auto --steps 5,6 --reject-all-gaps
        """
    )

    ap.add_argument("cdm_name", help="CDM name (e.g., 'plan', 'formulary', 'Plan and Benefit')")
    ap.add_argument("--auto", action="store_true",
                    help="Unattended end-to-end run with all defaults — no prompts.  "
                         "Requires Step 0 (config) to have already been run.")
    ap.add_argument("--model", default="gpt-5",
                    help="Model key (auto mode only).  Default: gpt-5")
    ap.add_argument("--workers", type=int, default=16,
                    help="Concurrent LLM workers for per-entity matching and rule consolidation.  "
                         "Auto mode only.  Default: 16")
    ap.add_argument("--steps", default="1,2,3,4,5,6",
                    help="Comma-separated step list (auto mode only).  Default: 1,2,3,4,5,6")
    ap.add_argument("--gap-threshold", type=float, default=0.8,
                    help="Confidence threshold for auto-approving gap-driven refinement "
                         "recommendations (0.0–1.0).  Auto mode only.  Default: 0.8")
    ap.add_argument("--reject-all-gaps", action="store_true",
                    help="Reject every gap-refinement recommendation regardless of confidence.  "
                         "Auto mode only.  Equivalent to --gap-threshold 1.01")

    args = ap.parse_args()
    cdm_name = args.cdm_name

    # Auto mode short-circuits the interactive flow
    if args.auto:
        try:
            steps = set()
            for token in args.steps.split(","):
                token = token.strip()
                if token:
                    steps.add(int(token))
        except ValueError:
            print(f"ERROR: --steps must be comma-separated integers, got: {args.steps}", file=sys.stderr)
            sys.exit(1)

        run_auto(
            cdm_name=cdm_name,
            model_key=args.model,
            workers=args.workers,
            steps_to_run=steps,
            gap_threshold=args.gap_threshold,
            reject_all_gaps=args.reject_all_gaps,
        )
        return
    
    try:
        print(f"\n{'='*60}")
        print(f"CDM GENERATION ORCHESTRATOR")
        print(f"{'='*60}")
        print(f"   CDM: {cdm_name}")
        
        # === 1. DRY RUN OR LIVE? ===
        print(f"\n{'='*60}")
        dry_run = prompt_user("Run in DRY RUN mode (review prompts only)?", default="N")
        
        mode_str = "DRY RUN" if dry_run else "LIVE"
        print(f"✓ Mode: {mode_str}")
        print(f"{'='*60}")
        
        # === 2. MODEL SELECTION (if live) ===
        llm = None
        if not dry_run:
            print(f"\n{'='*60}")
            selected_model = select_model()
            model_config = MODEL_OPTIONS[selected_model]
            
            print(f"✓ Selected model: {model_config['name']}")
            
            llm = LLMClient(
                model=model_config['model'],
                base_url=model_config['base_url'](),
                temperature=0.2,
                timeout=1800
            )
            print(f"✓ LLM initialized: {llm.model}")
            print(f"{'='*60}")
        
        # === STEP 0: CONFIG GENERATION ===
        run_config_gen = prompt_user("\nRun Step 0: Config Generation?", default="N")
        
        if run_config_gen:
            config_file = run_step0_config_generation(cdm_name, llm, dry_run)
            if not config_file:
                print(f"\n❌ Config generation failed for CDM: {cdm_name}")
                sys.exit(1)
        else:
            # Find existing config
            config_file = find_latest_config(cdm_name)
            if not config_file:
                print(f"\n❌ No config found for CDM: {cdm_name}")
                print(f"   Run Step 0 to generate configuration")
                sys.exit(1)
        
        print(f"\nUsing configuration: {config_file}")
        
        # Load configuration
        config = load_config(str(config_file))
        print(f"✓ Configuration loaded")
        print(f"  Domain: {config.cdm.domain}")
        print(f"  Type: {config.cdm.type}")
        print(f"  Description: {config.cdm.description}")
        
        # === 3. STEP SELECTION ===
        print(f"\n{'='*60}")
        print("Available steps:")
        print()
        print("  Config & Rationalization")
        print("    1  - Rationalize Input Sources (FHIR, NCPDP, Guardrails, Glue, EDW, Ancillary)")
        print()
        print("  Build CDM")
        print("    2  - Build Foundational CDM")
        print()
        print("  Refinement")
        print("    3  - Consolidation (merge overlapping entities)")
        print("    4  - PK/FK Validation (validate keys & relationships)")
        print()
        print("  Full CDM & Mapping")
        print("    5  - Build Full CDM (source mapping + lineage; includes Refiner gate)")
        print("        └─ Post-Processing runs automatically after Step 5 (interactive menu):")
        print("              • Rematch     — second-pass on no-reason unmapped fields")
        print("              • Ancillary   — ancillary source enrichment")
        print("              • Sensitivity — PHI/PII flagging")
        print("              • CDE         — Critical Data Element identification")
        print("    5p - Post-Processing Only (standalone re-run)")
        print()
        print("  Artifacts")
        print("    6  - Generate Artifacts (DDL, LucidChart CSV, Excel, Word)")

        steps_input = input(
            "\nEnter steps to run (comma-separated, e.g., '1,2,3', '5p', or 'all') [1]: "
        ).strip()

        if steps_input.lower() == 'all':
            steps_to_run = {1, 2, 3, 4, 5, 6}  # All implemented steps
        elif not steps_input:
            steps_to_run = {1}  # Default
        else:
            steps_to_run = set()
            for token in steps_input.split(','):
                token = token.strip().lower()
                if token == '5p':
                    steps_to_run.add('5p')
                else:
                    try:
                        steps_to_run.add(int(token))
                    except ValueError:
                        print(f"   ⚠️  Unrecognised step '{token}' — skipping")

            if not steps_to_run:
                print("   No valid steps parsed. Using default: Step 1")
                steps_to_run = {1}

        # Display selected — sort ints first then string tokens
        int_steps  = sorted(s for s in steps_to_run if isinstance(s, int))
        str_steps  = sorted(s for s in steps_to_run if isinstance(s, str))
        display    = [str(s) for s in int_steps] + str_steps
        print(f"✓ Selected steps: {', '.join(display)}")
        print(f"{'='*60}")
        
        # Create base output directory
        base_outdir = Path(config.output.directory)
        base_outdir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"CDM GENERATION ORCHESTRATOR")
        print(f"Domain: {config.cdm.domain}")
        print(f"Steps to run: {', '.join(display)}")
        print(f"Mode: {mode_str}")
        print(f"{'='*60}")
        
        # === STEP 1: RATIONALIZATION ===
        if 1 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 1: INPUT RATIONALIZATION")
            print(f"{'='*60}")
            
            rationalized_outdir = base_outdir / "rationalized"
            rationalized_outdir.mkdir(parents=True, exist_ok=True)
            
            if dry_run:
                prompts_dir = rationalized_outdir / "prompts"
                prompts_dir.mkdir(parents=True, exist_ok=True)
                print(f"\n🔍 DRY RUN MODE - Prompts will be saved to: {prompts_dir}")
            
            # Prompt for what to process
            process_fhir = False
            process_ncpdp = False
            process_guardrails = False
            process_glue = False
            process_edw = False

            if config.has_fhir():
                process_fhir = prompt_user("Process FHIR resources?", default="Y")
            else:
                print("  ℹ️  No FHIR resources configured")
            
            if config.has_ncpdp():
                process_ncpdp = prompt_user("Process NCPDP standards?", default="Y")
            else:
                print("  ℹ️  No NCPDP standards configured")
            
            if config.has_guardrails():
                process_guardrails = prompt_user("Process Guardrails files?", default="Y")
            else:
                print("  ℹ️  No Guardrails files configured")
            
            if config.has_glue():
                process_glue = prompt_user("Process Glue tables?", default="Y")
            else:
                print("  ℹ️  No Glue tables configured")

            if config.has_edw():
                process_edw = prompt_user("Process EDW tables?", default="Y")
            else:
                print("  ℹ️  No EDW tables configured")
            
            process_ancillary = False
            if config.has_ancillary():
                process_ancillary = prompt_user("Process Ancillary sources?", default="Y")
            else:
                print("  ℹ️  No Ancillary sources configured")

            if not any([process_fhir, process_ncpdp, process_guardrails, process_glue, process_edw, process_ancillary]):
                print("  ⚠️  No sources selected for processing")
            
            # Step 1a-f: Run each enabled rationalizer.  When more than
            # one source is selected, rationalizers run concurrently —
            # they're independent (each writes to its own output file)
            # and the LLM calls are I/O-bound so the GIL doesn't block.
            # When only one source is selected, runs inline (no thread
            # overhead).  Internal prints from each rationalizer will
            # interleave under parallel execution; the per-task start
            # and complete markers below provide coarse progress.
            from concurrent.futures import ThreadPoolExecutor, as_completed
            from functools import partial
            from src.rationalizers import (
                run_fhir_rationalization,
                run_ncpdp_rationalization,
                run_guardrails_rationalization,
                run_glue_rationalization,
                run_edw_rationalization,
            )
            from src.rationalizers.rationalize_ancillary import run_ancillary_rationalization

            common_kwargs = dict(
                config=config,
                outdir=rationalized_outdir,
                llm=llm,
                dry_run=dry_run,
                config_path=str(config_file),
            )

            tasks = []  # list of (label, callable)
            if process_fhir:
                tasks.append(("Step 1a: FHIR",       partial(run_fhir_rationalization,       **common_kwargs)))
            if process_ncpdp:
                tasks.append(("Step 1b: NCPDP",      partial(run_ncpdp_rationalization,      **common_kwargs)))
            if process_guardrails:
                tasks.append(("Step 1c: Guardrails", partial(run_guardrails_rationalization, **common_kwargs)))
            if process_glue:
                tasks.append(("Step 1d: Glue",       partial(run_glue_rationalization,       **common_kwargs)))
            if process_edw:
                tasks.append(("Step 1e: EDW",        partial(run_edw_rationalization,        **common_kwargs)))
            if process_ancillary:
                tasks.append(("Step 1f: Ancillary",  partial(run_ancillary_rationalization,  **common_kwargs)))

            if len(tasks) == 1:
                # Single source — run inline, preserve the original output shape.
                label, fn = tasks[0]
                print(f"\n=== {label} Rationalization ===")
                fn()
            elif len(tasks) > 1:
                print(f"\n=== Running {len(tasks)} rationalizers in parallel ===")
                print(f"   Output from individual rationalizers will interleave.")
                print(f"   Each writes its own output file; failures do not cascade.")
                failures = []
                with ThreadPoolExecutor(max_workers=len(tasks)) as ex:
                    futures = {ex.submit(fn): label for label, fn in tasks}
                    for fut in as_completed(futures):
                        label = futures[fut]
                        try:
                            fut.result()
                            print(f"\n=== ✓ {label}: rationalization complete ===")
                        except Exception as e:
                            print(f"\n=== ✗ {label}: FAILED — {e} ===")
                            failures.append((label, e))
                if failures:
                    print(f"\n   ⚠️  {len(failures)} rationalizer(s) failed:")
                    for label, e in failures:
                        print(f"      - {label}: {e}")

            print(f"\n{'='*60}")
            print(f"✓ STEP 1 COMPLETE")
            print(f"  Rationalized files saved to: {rationalized_outdir}")
            print(f"{'='*60}")
        
        # === STEP 2: BUILD FOUNDATIONAL CDM ===
        if 2 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 2: BUILD FOUNDATIONAL CDM")
            print(f"{'='*60}")

            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)
            rationalized_dir = base_outdir / "rationalized"

            if dry_run:
                print(f"\n   DRY RUN MODE - Prompts will be saved to: {cdm_outdir / 'prompts'}")

            print(f"\n=== Step 2: Build Foundational CDM ===")
            from src.cdm_builder.build_foundational_cdm import run_step3a

            cdm = run_step3a(
                config=config,
                outdir=cdm_outdir,
                llm=llm,
                dry_run=dry_run,
                rationalized_dir=rationalized_dir
            )

            print(f"\n{'='*60}")
            print(f"✓ STEP 2 COMPLETE")
            print(f"  CDM saved to: {cdm_outdir}")
            if cdm:
                print(f"  Note: Run Step 6 to generate artifacts")
            print(f"{'='*60}")
        
        # === STEP 3: REFINEMENT - CONSOLIDATION ===
        if 3 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 3: REFINEMENT - CONSOLIDATION")
            print(f"{'='*60}")

            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)

            from src.refinement.refine_consolidation import run_consolidation_refinement

            cdm = run_consolidation_refinement(
                config=config,
                cdm_file=None,
                outdir=cdm_outdir,
                llm=llm,
                dry_run=dry_run
            )

            print(f"\n{'='*60}")
            print(f"✓ STEP 3 COMPLETE")
            print(f"{'='*60}")

        # === STEP 4: REFINEMENT - PK/FK VALIDATION ===
        if 4 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 4: REFINEMENT - PK/FK VALIDATION")
            print(f"{'='*60}")

            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)

            from src.refinement.refine_pk_fk_validation import run_pk_fk_validation

            cdm = run_pk_fk_validation(
                config=config,
                cdm_file=None,
                outdir=cdm_outdir,
                llm=llm,
                dry_run=dry_run
            )

            print(f"\n{'='*60}")
            print(f"✓ STEP 4 COMPLETE")
            print(f"{'='*60}")
        
        # === STEP 5: BUILD FULL CDM ===
        if 5 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 5: BUILD FULL CDM")
            print(f"{'='*60}")
            
            from src.cdm_full.build_full_cdm import (
                run_build_full_cdm,
                get_discovered_sources,
                get_existing_match_files
            )
            
            # Discover sources and existing match files
            discovered = get_discovered_sources(base_outdir, config.cdm.domain)
            existing_matches = get_existing_match_files(base_outdir)
            
            if not discovered:
                print(f"   ❌ No rationalized files found for domain '{config.cdm.domain}'")
            else:
                source_types = sorted(discovered.keys())
                
                # Display discovered sources
                print(f"\n   {len(source_types)} rationalized files identified:")
                for st in source_types:
                    match_status = "✓ match file exists" if st in existing_matches else "○ no match file"
                    print(f"     • {st}: {discovered[st].name} [{match_status}]")
                
                # Prompt: mapping mode
                #   reuse — use ALL existing match files; no per-source prompts
                #   select — pick per source (default; sources WITH existing match
                #            files default to N, sources WITHOUT default to Y)
                #   remap  — force re-run AI on EVERY source (ignore existing
                #            match files)
                skip_mapping = False
                remap_all = False
                if existing_matches:
                    print(f"\n   Mapping mode:")
                    print(f"     [s] Select per source (default)")
                    print(f"     [r] Reuse all existing match files (no AI mapping)")
                    print(f"     [a] Remap ALL sources (re-run AI on every source)")
                    raw = input(f"   Choose [s/r/a, default s]: ").strip().lower() or "s"
                    if raw in ("r", "reuse", "skip"):
                        skip_mapping = True
                    elif raw in ("a", "all", "remap"):
                        remap_all = True

                # Prompt: Per-source mapping (when in select or remap-all mode)
                sources_to_map = []
                if skip_mapping and not dry_run:
                    print(f"   Reusing existing match files for all sources, skipping AI mapping")
                elif remap_all and not dry_run:
                    sources_to_map = list(source_types)
                    print(f"   Remapping ALL {len(sources_to_map)} sources (existing match files will be replaced)")
                elif not dry_run:
                    print(f"\n   Select sources to map (sources with existing matches default to N):")
                    for st in source_types:
                        existing_note = f" [existing: {existing_matches[st].name}]" if st in existing_matches else ""
                        default = "N" if st in existing_matches else "Y"
                        if prompt_user(f"   Map {st}?{existing_note}", default=default):
                            sources_to_map.append(st)

                    if not sources_to_map and not existing_matches:
                        print(f"   ⚠️  No sources selected and no existing match files")
                
                # Prompt: Generate full CDM?
                generate_cdm = prompt_user("\nGenerate Full CDM?", default="Y")

                # Prompt: Run gap analysis?
                run_gap_analysis = False
                if generate_cdm:
                    run_gap_analysis = prompt_user("Run gap analysis?", default="Y")

                # Prompt: parallel match-file workers.  Even in Reuse
                # mode the refiner gate inside Step 5 may trigger
                # ancillary re-mapping, so we ask any time LLM matching
                # is enabled at all (i.e., not skip_mapping and not
                # dry_run).  1 = sequential.  Tier 4 OpenAI accounts
                # handle 8-16 comfortably.
                match_workers = 1
                if not skip_mapping and not dry_run:
                    raw = input(
                        "   Concurrent LLM workers for per-entity matching [1]: "
                    ).strip() or "1"
                    try:
                        match_workers = max(1, int(raw))
                    except ValueError:
                        print(f"   ⚠️  Invalid worker count '{raw}' — falling back to sequential (1)")
                        match_workers = 1

                # Execute
                if generate_cdm or sources_to_map or dry_run:
                    full_cdm = run_build_full_cdm(
                        config=config,
                        cdm_file=None,
                        outdir=base_outdir,
                        llm=llm,
                        dry_run=dry_run,
                        sources_to_map=sources_to_map if sources_to_map else None,
                        skip_mapping=skip_mapping,
                        generate_cdm=generate_cdm,
                        run_gap_analysis=run_gap_analysis,
                        match_workers=match_workers,
                    )
                else:
                    print(f"   ○ Step 6 cancelled by user")
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 5 COMPLETE")
            print(f"{'='*60}")

            # === POST-PROCESSING ===
            existing_full_cdm = find_existing_full_cdm(base_outdir, config.cdm.domain)
            if existing_full_cdm:
                run_postprocess = prompt_user(
                    "\nRun post-processing? (Rematch, Sensitivity, CDE — interactive menu)",
                    default="Y"
                )
                if run_postprocess:
                    from src.cdm_full.run_postprocess import interactive_postprocessing
                    updated_cdm = interactive_postprocessing(
                        config=config,
                        outdir=base_outdir,
                        llm=llm,
                        dry_run=dry_run
                    )
            else:
                print(f"   ⚠️  No Full CDM available - skipping post-processing")

        # === STEP 5P: STANDALONE POST-PROCESSING ===
        if "5p" in {str(s).lower() for s in steps_to_run}:
            print(f"\n{'='*60}")
            print(f"STEP 5P: POST-PROCESSING (standalone)")
            print(f"{'='*60}")

            from src.cdm_full.run_postprocess import interactive_postprocessing

            existing_full_cdm = find_existing_full_cdm(base_outdir, config.cdm.domain)
            if existing_full_cdm:
                updated_cdm = interactive_postprocessing(
                    config=config,
                    outdir=base_outdir,
                    llm=llm,
                    dry_run=dry_run
                )
            else:
                print(f"   ⚠️  No Full CDM found — run Step 6 first")

            print(f"\n{'='*60}")
            print(f"✓ STEP 5P COMPLETE")
            print(f"{'='*60}")

        # === STEP 6: GENERATE ARTIFACTS ===
        if 6 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 6: GENERATE ARTIFACTS")
            print(f"{'='*60}")

            from src.artifacts.run_artifacts import interactive_artifact_generation

            artifacts = interactive_artifact_generation(
                config=config,
                outdir=base_outdir,
                llm=llm,
                dry_run=dry_run,
            )

            print(f"\n{'='*60}")
            print(f"✓ STEP 6 COMPLETE")
            print(f"{'='*60}")
        
        print(f"\n{'='*60}")
        print("ORCHESTRATION COMPLETE")
        print(f"{'='*60}\n")
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()