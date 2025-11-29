# cdm_orchestrator.py
"""
Unified CDM Generation Orchestrator

Interactive flow:
  1. Dry run or live mode selection
  2. Model selection (if live)
  3. Step-by-step execution with granular control

Runs all steps from rationalization through Excel generation:
  Step 1: Input Rationalization (1a: FHIR, 1b: Guardrails, 1c: Glue)
  Step 2: CDM Generation (2a: FHIR Foundation, 2b-2e: Refinements)
  Step 3: Relationships & Constraints (future)
  Step 4: DDL Generation (future)
  Step 5: Excel Generation (future)
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from dotenv import load_dotenv

from src.config import load_config
from src.core.llm_client import LLMClient
from src.core.model_selector import MODEL_OPTIONS, select_model, prompt_user
from src.steps.step1a_fhir import run_step1a
from src.steps.step1b_guardrails import run_step1b
from src.steps.step1c_glue import run_step1c

load_dotenv()

def find_latest_config(base_name: str) -> Path:
    """Find latest timestamped config file from base name"""
    config_dir = Path("config")
    
    # Extract base name without extension
    base = Path(base_name).stem  # e.g., "config_plan"
    
    # Find all matching timestamped configs
    pattern = f"{base}_*.json"
    matches = sorted(config_dir.glob(pattern), reverse=True)
    
    if matches:
        return matches[0]
    
    # Fallback to exact match
    exact = config_dir / base_name
    if exact.exists():
        return exact
    
    raise FileNotFoundError(f"No config file found matching: {base_name}")

def main():
    ap = argparse.ArgumentParser(
        description="CDM Generation Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python cdm_orchestrator.py config_plan.json
        """
    )
    
    ap.add_argument("config", help="Config file base name (e.g., config_plan.json)")
    
    args = ap.parse_args()
    
    try:
        # Find latest timestamped config
        config_file = find_latest_config(args.config)
        print(f"Using configuration: {config_file}")
        
        # Load configuration
        config = load_config(str(config_file))
        print(f"✓ Configuration loaded")
        print(f"  Domain: {config.cdm.domain}")
        print(f"  Description: {config.cdm.description}")
        
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
        
        # === 3. STEP SELECTION ===
        print(f"\n{'='*60}")
        print("Available steps:")
        print("  1 - Input Rationalization (FHIR, Guardrails, Glue)")
        print("  2 - CDM Generation (FHIR Foundation + Refinements)")
        print("  3 - Relationships & Model Construction (not yet implemented)")
        print("  4 - DDL Generation (not yet implemented)")
        print("  5 - Excel Generation (not yet implemented)")
        
        steps_input = input("\nEnter steps to run (comma-separated, e.g., '1,2' or 'all') [1]: ").strip()
        
        if steps_input.lower() == 'all':
            steps_to_run = {1, 2}  # Only implemented steps
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
            process_guardrails = False
            process_glue = False
            
            if config.inputs.fhir:
                print(f"\nFound {len(config.inputs.fhir)} FHIR file(s)")
                process_fhir = prompt_user("Process FHIR files?", default="Y")
            
            if config.inputs.guardrails:
                print(f"\nFound {len(config.inputs.guardrails)} Guardrails file(s)")
                process_guardrails = prompt_user("Process Guardrails files?", default="Y")
            
            if config.inputs.glue:
                print(f"\nFound {len(config.inputs.glue)} Glue schema file(s)")
                process_glue = prompt_user("Process Glue schemas?", default="Y")
            
            # Step 1a: FHIR
            if process_fhir:
                print(f"\n=== Step 1a: FHIR Rationalization ===")
                run_step1a(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
            
            # Step 1b: Guardrails
            if process_guardrails:
                print(f"\n=== Step 1b: Guardrails Rationalization ===")
                run_step1b(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
            
            # Step 1c: Glue
            if process_glue:
                print(f"\n=== Step 1c: Glue Schema Rationalization ===")
                run_step1c(
                    config=config,
                    outdir=rationalized_outdir,
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
            
            # Step 1d: NCPDP Rationalization
            ncpdp_general = Path("input/strd_ncpdp/ncpdp_general_standards.json")
            ncpdp_script = Path("input/strd_ncpdp/ncpdp_script_standards.json")
            
            if ncpdp_general.exists() or ncpdp_script.exists():
                print(f"\nFound NCPDP standards files")
                process_ncpdp = prompt_user("Process NCPDP standards?", default="Y")
                
                if process_ncpdp:
                    print(f"\n=== Step 1d: NCPDP Rationalization ===")
                    from src.rationalizers.rationalize_ncpdp import NCPDPRationalizer
                    
                    rationalizer = NCPDPRationalizer(str(config_file))
                    rationalizer.run(str(ncpdp_general), str(ncpdp_script), str(rationalized_outdir))
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 1 COMPLETE")
            print(f"  Rationalized files saved to: {rationalized_outdir}")
            print(f"{'='*60}")
        
        # === STEP 2: CDM GENERATION ===
        if 2 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 2: CDM GENERATION")
            print(f"{'='*60}")
            
            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)
            
            if dry_run:
                prompts_dir = cdm_outdir / "prompts"
                prompts_dir.mkdir(parents=True, exist_ok=True)
                print(f"\n🔍 DRY RUN MODE - Prompts will be saved to: {prompts_dir}")
            
            # Prompt for which Step 2 substeps to run
            print("\nStep 2 consists of multiple substeps:")
            print("  2a: FHIR Foundation - Create canonical model from FHIR")
            print("  2b: NCPDP Refinement - Add NCPDP crosswalk & gap analysis")
            print("  2c: Guardrails Refinement - Add Guardrails crosswalk & gap analysis")
            print("  2d: Glue Refinement - Add Glue crosswalk & gap analysis")
            print("  2e: Other Files & Final Refinement - Naming standards & validation")
            
            run_2a = prompt_user("\nRun Step 2a (FHIR Foundation)?", default="Y")
            run_2b = prompt_user("Run Step 2b (NCPDP Refinement)?", default="N")
            run_2c = prompt_user("Run Step 2c (Guardrails Refinement)?", default="N")
            run_2d = prompt_user("Run Step 2d (Glue Refinement)?", default="N")
            run_2e = prompt_user("Run Step 2e (Other Files & Final)?", default="N")
            
            if not any([run_2a, run_2b, run_2c, run_2d, run_2e]):
                print("  ⚠️  No Step 2 substeps selected, skipping Step 2")
            else:
                # Find rationalized FHIR from Step 1 (needed for 2a)
                if run_2a:
                    rationalized_outdir = base_outdir / "rationalized"
                    if not rationalized_outdir.exists():
                        print("  ❌ ERROR: Step 1 output not found. Run Step 1a first.")
                        sys.exit(1)
                    
                    fhir_files = sorted(rationalized_outdir.glob("rationalized_fhir_*.json"))
                    if not fhir_files:
                        print("  ❌ ERROR: No rationalized FHIR file found. Run Step 1a first.")
                        sys.exit(1)
                    
                    latest_fhir = fhir_files[-1]
                    print(f"  📁 Using rationalized FHIR: {latest_fhir.name}")
                
                # Step 2a: FHIR Foundation
                if run_2a:
                    print(f"\n=== Step 2a: FHIR Foundation CDM ===")
                    from src.steps.step2a_fhir_foundation import run_step2a
                    
                    run_step2a(
                        config=config,
                        rationalized_fhir_file=latest_fhir,
                        outdir=cdm_outdir,
                        llm=llm if not dry_run else None,
                        dry_run=dry_run
                    )
                
                # Step 2b: NCPDP Refinement
                if run_2b:
                    print(f"\n=== Step 2b: NCPDP Refinement ===")
                    
                    # Find most recent foundation CDM from Step 2a
                    foundation_files = sorted(cdm_outdir.glob("foundation_cdm_*.json"))
                    if not foundation_files:
                        print("  ❌ ERROR: No foundation CDM found. Run Step 2a first.")
                        sys.exit(1)
                    
                    latest_foundation = foundation_files[-1]
                    print(f"  📁 Using foundation CDM: {latest_foundation.name}")
                    
                    from src.steps.step2b_ncpdp_refinement import run_step2b
                    
                    run_step2b(
                        config=config,
                        foundation_cdm_file=latest_foundation,
                        outdir=cdm_outdir,
                        llm=llm if not dry_run else None,
                        dry_run=dry_run
                    )
                
                # Step 2c: Guardrails Refinement
                if run_2c:
                    print(f"\n=== Step 2c: Guardrails Refinement ===")
                    
                    # Find most recent enhanced CDM from Step 2b (or foundation from 2a if 2b not run)
                    enhanced_files = sorted(cdm_outdir.glob("enhanced_cdm_ncpdp_*.json"))
                    if enhanced_files:
                        latest_enhanced = enhanced_files[-1]
                        print(f"  📁 Using enhanced CDM from Step 2b: {latest_enhanced.name}")
                    else:
                        # Fall back to foundation CDM if 2b wasn't run
                        foundation_files = sorted(cdm_outdir.glob("foundation_cdm_*.json"))
                        if not foundation_files:
                            print("  ❌ ERROR: No foundation or enhanced CDM found. Run Step 2a or 2b first.")
                            sys.exit(1)
                        latest_enhanced = foundation_files[-1]
                        print(f"  📁 Using foundation CDM (2b not run): {latest_enhanced.name}")
                    
                    from src.steps.step2c_guardrails_refinement import run_step2c
                    
                    run_step2c(
                        config=config,
                        enhanced_cdm_file=latest_enhanced,
                        outdir=cdm_outdir,
                        llm=llm if not dry_run else None,
                        dry_run=dry_run
                    )
                
                # Step 2d: Glue Refinement
                if run_2d:
                    print(f"\n=== Step 2d: Glue Refinement ===")
                    print("Step 2d not yet implemented")
                
                # Step 2e: Final Refinement
                if run_2e:
                    print(f"\n=== Step 2e: Final Refinement ===")
                    print("Step 2e not yet implemented")
                
                print(f"\n{'='*60}")
                print(f"✓ STEP 2 COMPLETE")
                print(f"  CDM files saved to: {cdm_outdir}")
                print(f"{'='*60}")
        
        # === STEP 3-5: FUTURE ===
        if any(s in steps_to_run for s in [3, 4, 5]):
            print(f"\nSteps 3-5 not yet implemented.")
        
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