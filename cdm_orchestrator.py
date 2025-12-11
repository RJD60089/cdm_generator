# cdm_orchestrator.py
"""
Unified CDM Generation Orchestrator

Interactive flow:
  1. Config generation or refresh (Step 0)
  2. Dry run or live mode selection
  3. Model selection (if live)
  4. Step-by-step execution with granular control

Steps:
  0 - Config Generation (FHIR, NCPDP, Glue analysis)
  1 - Input Rationalization (FHIR, NCPDP, Guardrails, Glue)
  2 - Identify Foundational CDM Generation Mode
  3 - Build Foundational CDM (CDM JSON)
  4 - Refinement - Consolidation (merge overlapping entities)
  5 - Refinement - PK/FK Validation (validate keys & relationships)
  6 - Build Full CDM (cross-reference all sources)
      6-POST - Post-Processing (CDE Identification, expandable)
  7 - Generate Artifacts (DDL, LucidChart CSV, Excel, Word)
  8 - Refinement - Naming Standards (not yet implemented)
  9 - Excel Generation (not yet implemented)
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
        print("  1 - Input Rationalization (FHIR, NCPDP, Guardrails, Glue)")
        print("  2 - Identify Foundational CDM Generation Mode")
        print("  3 - Build Foundational CDM (CDM JSON)")
        print("  4 - Refinement - Consolidation (merge overlapping entities)")
        print("  5 - Refinement - PK/FK Validation (validate keys & relationships)")
        print("  6 - Build Full CDM (cross-reference all sources)")
        print("      └─ Post-Processing (CDE Identification)")
        print("  7 - Generate Artifacts (DDL, LucidChart CSV, Excel, Word)")
        print("  8 - Refinement - Naming Standards (not yet implemented)")
        print("  9 - Excel Generation (not yet implemented)")
        
        steps_input = input("\nEnter steps to run (comma-separated, e.g., '1,2,3' or 'all') [1]: ").strip()
        
        if steps_input.lower() == 'all':
            steps_to_run = {1, 2, 3, 4, 5, 6, 7}  # Implemented steps
        elif not steps_input:
            steps_to_run = {1}  # Default
        else:
            try:
                steps_to_run = set(int(s.strip()) for s in steps_input.split(','))
            except ValueError:
                print("Invalid input. Using default: Step 1")
                steps_to_run = {1}
        
        print(f"✓ Selected steps: {sorted(steps_to_run)}")
        print(f"{'='*60}")
        
        # Create base output directory
        base_outdir = Path(config.output.directory)
        base_outdir.mkdir(parents=True, exist_ok=True)
        
        # Track CDM generation mode (set in Step 2, used in Step 3)
        cdm_generation_mode = None
        
        print(f"\n{'='*60}")
        print(f"CDM GENERATION ORCHESTRATOR")
        print(f"Domain: {config.cdm.domain}")
        print(f"Steps to run: {sorted(steps_to_run)}")
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
            
            if not any([process_fhir, process_ncpdp, process_guardrails, process_glue]):
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
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 1 COMPLETE")
            print(f"  Rationalized files saved to: {rationalized_outdir}")
            print(f"{'='*60}")
        
        # === STEP 2: IDENTIFY FOUNDATIONAL CDM MODE ===
        if 2 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 2: IDENTIFY FOUNDATIONAL CDM GENERATION MODE")
            print(f"{'='*60}")
            
            print(f"\n   Select CDM generation mode:")
            print(f"   ─────────────────────────────────────────────────")
            print(f"   [S] Standards  - FHIR/NCPDP structures prioritized")
            print(f"   [H] Hybrid     - Standards + business files equally weighted")
            print(f"   [A] AI Decide  - Let AI analyze sources and recommend")
            print(f"   [C] Cancel     - Skip mode selection")
            
            while True:
                choice = input("\nSelection [S]: ").strip().upper()
                if choice == '' or choice == 'S':
                    cdm_generation_mode = 'standards'
                    print(f"\n   ✓ Mode selected: STANDARDS")
                    print(f"     Standards-first approach - FHIR/NCPDP structures prioritized")
                    break
                elif choice == 'H':
                    cdm_generation_mode = 'hybrid'
                    print(f"\n   ✓ Mode selected: HYBRID")
                    print(f"     Balanced approach - Standards + internal business files equally weighted")
                    break
                elif choice == 'A':
                    print(f"\n   🤖 AI Determination selected...")
                    print(f"   ⚠️  AI mode determination not yet implemented.")
                    print(f"   Defaulting to HYBRID mode.")
                    cdm_generation_mode = 'hybrid'
                    break
                elif choice == 'C':
                    cdm_generation_mode = None
                    print(f"\n   ○ Mode selection cancelled")
                    print(f"     Step 3 will use default HYBRID mode if run")
                    break
                else:
                    print("   Invalid choice. Please enter S, H, A, or C.")
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 2 COMPLETE")
            print(f"  CDM Generation Mode: {cdm_generation_mode or 'not set (will use hybrid)'}")
            print(f"{'='*60}")
        
        # === STEP 3: BUILD FOUNDATIONAL CDM ===
        if 3 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 3: BUILD FOUNDATIONAL CDM")
            print(f"{'='*60}")
            
            # Use generation mode from Step 2, default to hybrid
            mode = cdm_generation_mode or 'hybrid'
            print(f"   Generation Mode: {mode.upper()}")
            
            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)
            
            if dry_run:
                print(f"\n🔍 DRY RUN MODE - Prompts will be saved to: {cdm_outdir / 'prompts'}")
            
            print(f"\n=== Step 3a: Build Foundational CDM ({mode}) ===")
            from src.cdm_builder.build_foundational_cdm import run_step3a
            
            rationalized_dir = base_outdir / "rationalized"
            
            cdm = run_step3a(
                config=config,
                outdir=cdm_outdir,
                llm=llm,
                dry_run=dry_run,
                rationalized_dir=rationalized_dir
            )
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 3 COMPLETE")
            print(f"  CDM saved to: {cdm_outdir}")
            if cdm:
                print(f"  Note: Run Step 7 to generate DDL and LucidChart artifacts")
            print(f"{'='*60}")
        
        # === STEP 4: REFINEMENT - CONSOLIDATION ===
        if 4 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 4: REFINEMENT - CONSOLIDATION")
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
            print(f"✓ STEP 4 COMPLETE")
            print(f"{'='*60}")
        
        # === STEP 5: REFINEMENT - PK/FK VALIDATION ===
        if 5 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 5: REFINEMENT - PK/FK VALIDATION")
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
            print(f"✓ STEP 5 COMPLETE")
            print(f"{'='*60}")
        
        # === STEP 6: BUILD FULL CDM ===
        if 6 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 6: BUILD FULL CDM")
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
                
                # Prompt: Skip mapping?
                skip_mapping = False
                if existing_matches:
                    skip_mapping = prompt_user(
                        f"\nSkip mapping and use existing match files ({len(existing_matches)} available)?",
                        default="N"
                    )
                
                # Prompt: Per-source mapping (if not skipping)
                sources_to_map = []
                if not skip_mapping and not dry_run:
                    print(f"\n   Select sources to map:")
                    for st in source_types:
                        existing_note = f" [existing: {existing_matches[st].name}]" if st in existing_matches else ""
                        default = "N" if st in existing_matches else "Y"
                        if prompt_user(f"   Map {st}?{existing_note}", default=default):
                            sources_to_map.append(st)
                    
                    if not sources_to_map and not existing_matches:
                        print(f"   ⚠️  No sources selected and no existing match files")
                elif not dry_run:
                    print(f"   Using existing match files, skipping AI mapping")
                
                # Prompt: Generate full CDM?
                generate_cdm = prompt_user("\nGenerate Full CDM?", default="Y")
                
                # Prompt: Run gap analysis?
                run_gap_analysis = False
                if generate_cdm:
                    run_gap_analysis = prompt_user("Run gap analysis?", default="Y")
                
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
                        run_gap_analysis=run_gap_analysis
                    )
                else:
                    print(f"   ○ Step 6 cancelled by user")
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 6 COMPLETE")
            print(f"{'='*60}")
            
            # === POST-PROCESSING ===
            existing_full_cdm = find_existing_full_cdm(base_outdir, config.cdm.domain)
            if existing_full_cdm:
                run_postprocess = prompt_user("\nRun post-processing (CDE Identification)?", default="Y")
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
        
        # === STEP 7: GENERATE ARTIFACTS ===
        if 7 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 7: GENERATE ARTIFACTS")
            print(f"{'='*60}")
            
            from src.artifacts.run_artifacts import interactive_artifact_generation
            
            artifacts = interactive_artifact_generation(
                config=config,
                outdir=base_outdir
            )
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 7 COMPLETE")
            print(f"{'='*60}")
        
        # === STEPS 8-9: FUTURE ===
        if any(s in steps_to_run for s in [8, 9]):
            future_steps = [s for s in [8, 9] if s in steps_to_run]
            print(f"\nSteps {future_steps} not yet implemented.")
        
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