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


def main():
    ap = argparse.ArgumentParser(
        description="CDM Generation Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cdm_orchestrator.py plan
  python cdm_orchestrator.py formulary
  python cdm_orchestrator.py "plan and benefit"
        """
    )
    
    ap.add_argument("cdm_name", help="CDM name (e.g., 'plan', 'formulary', 'Plan and Benefit')")
    
    args = ap.parse_args()
    cdm_name = args.cdm_name
    
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
            
            # Step 1a: FHIR
            if process_fhir:
                print(f"\n=== Step 1a: FHIR Rationalization ===")
                from src.rationalizers import run_fhir_rationalization
                
                run_fhir_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )
            
            # Step 1b: NCPDP
            if process_ncpdp:
                print(f"\n=== Step 1b: NCPDP Rationalization ===")
                from src.rationalizers import run_ncpdp_rationalization
                
                run_ncpdp_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )
            
            # Step 1c: Guardrails
            if process_guardrails:
                print(f"\n=== Step 1c: Guardrails Rationalization ===")
                from src.rationalizers import run_guardrails_rationalization
                
                run_guardrails_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )
            
            # Step 1d: Glue
            if process_glue:
                print(f"\n=== Step 1d: Glue Rationalization ===")
                from src.rationalizers import run_glue_rationalization
                
                run_glue_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )

            # Step 1e: EDW
            if process_edw:
                print(f"\n=== Step 1e: EDW Rationalization ===")
                from src.rationalizers import run_edw_rationalization

                run_edw_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )

            # Step 1f: Ancillary
            if process_ancillary:
                print(f"\n=== Step 1f: Ancillary Rationalization ===")
                from src.rationalizers.rationalize_ancillary import run_ancillary_rationalization

                run_ancillary_rationalization(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm,
                    dry_run=dry_run,
                    config_path=str(config_file)
                )

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