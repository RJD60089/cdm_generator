# cdm_orchestrator.py
"""
Unified CDM Generation Orchestrator

Interactive flow:
  1. Dry run or live mode selection
  2. Model selection (if live)
  3. Step-by-step execution with granular control

Runs all steps from rationalization through Excel generation:
  Step 1: Input Rationalization (1a: FHIR, 1b: NCPDP, 1c: Guardrails, 1d: Glue)
  Step 2: CDM Generation (2a: FHIR Foundation, 2b-2e: Refinements)
  Step 3: Build CDM Artifacts (3a: CDM JSON, 3b: DDL, 3c: LucidChart)
  Step 4: Relationships & Model Construction (future)
  Step 5: Excel Generation (future)
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

print("[DEBUG] Standard imports done...")  # TEMP DEBUG

try:
    from src.config import load_config
    print("[DEBUG] load_config imported")
except Exception as e:
    print(f"[DEBUG] Failed to import load_config: {e}")
    sys.exit(1)

try:
    from src.core.llm_client import LLMClient
    print("[DEBUG] LLMClient imported")
except Exception as e:
    print(f"[DEBUG] Failed to import LLMClient: {e}")
    sys.exit(1)

try:
    from src.core.model_selector import MODEL_OPTIONS, select_model, prompt_user
    print("[DEBUG] model_selector imported")
except Exception as e:
    print(f"[DEBUG] Failed to import model_selector: {e}")
    sys.exit(1)

print("[DEBUG] All imports complete")  # TEMP DEBUG

load_dotenv()

print("[DEBUG] dotenv loaded, entering main guard...")  # TEMP DEBUG


def find_latest_config(base_name: str) -> Path:
    """Find latest timestamped config file from base name"""
    # Extract base name without extension
    base = Path(base_name).stem  # e.g., "config_plan" or just "plan"
    
    # If just domain name provided, prefix with config_
    if not base.startswith("config_"):
        domain = base  # "plan"
        base = f"config_{base}"  # "config_plan"
    else:
        # Extract domain from config_plan -> plan
        domain = base.replace("config_", "")
    
    # Only one config location per CDM
    config_dir = Path("input/business") / f"cdm_{domain}" / "config"
    
    if not config_dir.exists():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")
    
    # Find all matching timestamped configs
    pattern = f"{base}_*.json"
    matches = sorted(config_dir.glob(pattern), reverse=True)
    
    if matches:
        return matches[0]
    
    # Try exact match
    exact = config_dir / f"{base}.json"
    if exact.exists():
        return exact
    
    raise FileNotFoundError(f"No config file found matching: {base_name} in {config_dir}")


def main():
    ap = argparse.ArgumentParser(
        description="CDM Generation Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python cdm_orchestrator.py plan
  python cdm_orchestrator.py config_plan.json
        """
    )
    
    ap.add_argument("config", help="Config file base name or domain (e.g., 'plan' or 'config_plan.json')")
    
    args = ap.parse_args()
    
    try:
        # Find latest timestamped config
        config_file = find_latest_config(args.config)
        print(f"Using configuration: {config_file}")
        
        # Load configuration
        config = load_config(str(config_file))
        print(f"✓ Configuration loaded")
        print(f"  Domain: {config.cdm.domain}")
        print(f"  Type: {config.cdm.type}")
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
        print("  1 - Input Rationalization (FHIR, NCPDP, Guardrails, Glue)")
        print("  2 - CDM Generation (FHIR Foundation + Refinements)")
        print("  3 - Build CDM Artifacts (CDM JSON, DDL, LucidChart)")
        print("  4 - Relationships & Model Construction (not yet implemented)")
        print("  5 - Excel Generation (not yet implemented)")
        
        steps_input = input("\nEnter steps to run (comma-separated, e.g., '1,2' or 'all') [1]: ").strip()
        
        if steps_input.lower() == 'all':
            steps_to_run = {1, 2, 3}  # Implemented steps
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
            process_ncpdp = False
            process_guardrails = False
            process_glue = False
            
            if config.has_fhir():
                print(f"\nFound {len(config.fhir_igs)} FHIR resource(s)")
                print(f"  StructureDefinitions: {len(config.get_structure_definitions())}")
                print(f"  ValueSets: {len(config.get_value_sets())}")
                print(f"  CodeSystems: {len(config.get_code_systems())}")
                process_fhir = prompt_user("Process FHIR files?", default="Y")
            
            if config.has_ncpdp():
                general_count = len(config.ncpdp_general_standards)
                script_count = len(config.ncpdp_script_standards)
                print(f"\nFound NCPDP standards (General: {general_count}, SCRIPT: {script_count})")
                process_ncpdp = prompt_user("Process NCPDP standards?", default="Y")
            
            if config.has_guardrails():
                print(f"\nFound {len(config.guardrails)} Guardrails file(s)")
                process_guardrails = prompt_user("Process Guardrails files?", default="Y")
            
            if config.has_glue():
                print(f"\nFound {len(config.glue)} Glue schema file(s)")
                process_glue = prompt_user("Process Glue schemas?", default="Y")
            
            # Step 1a: FHIR Rationalization
            if process_fhir:
                print(f"\n=== Step 1a: FHIR Rationalization ===")
                from src.rationalizers.rationalize_fhir import FHIRRationalizer
                
                rationalizer = FHIRRationalizer(
                    config_path=str(config_file),
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
                
                rationalizer.run(str(rationalized_outdir))
            
            # Step 1b: NCPDP Rationalization
            if process_ncpdp:
                print(f"\n=== Step 1b: NCPDP Rationalization ===")
                from src.rationalizers.rationalize_ncpdp import NCPDPRationalizer
                
                ncpdp_general = Path("input/strd_ncpdp/ncpdp_general_standards.json")
                ncpdp_script = Path("input/strd_ncpdp/ncpdp_script_standards.json")
                
                rationalizer = NCPDPRationalizer(
                    config_path=str(config_file),
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
                rationalizer.run(str(ncpdp_general), str(ncpdp_script), str(rationalized_outdir))
            
            # Step 1c: Guardrails Rationalization
            if process_guardrails:
                print(f"\n=== Step 1c: Guardrails Rationalization ===")
                from src.rationalizers.rationalize_guardrails import GuardrailsRationalizer
                
                rationalizer = GuardrailsRationalizer(
                    config_path=str(config_file),
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
                rationalizer.run(str(rationalized_outdir))
            
            # Step 1d: Glue Rationalization
            if process_glue:
                print(f"\n=== Step 1d: Glue Schema Rationalization ===")
                from src.rationalizers.rationalize_glue import GlueRationalizer
                
                rationalizer = GlueRationalizer(
                    config_path=str(config_file),
                    llm=llm if not dry_run else None,
                    dry_run=dry_run
                )
                rationalizer.run(str(rationalized_outdir))
            
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
                rationalized_outdir = base_outdir / "rationalized"
                
                # Find rationalized FHIR from Step 1 (needed for 2a)
                if run_2a:
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
        
        # === STEP 3: BUILD CDM ARTIFACTS ===
        if 3 in steps_to_run:
            print(f"\n{'='*60}")
            print(f"STEP 3: BUILD CDM ARTIFACTS")
            print(f"{'='*60}")
            
            cdm_outdir = base_outdir / "cdm"
            cdm_outdir.mkdir(parents=True, exist_ok=True)
            
            if dry_run:
                print(f"\n🔍 DRY RUN MODE - Prompts will be saved to: {cdm_outdir / 'prompts'}")
            
            # Step 3a: Build Foundational CDM
            print(f"\n=== Step 3a: Build Foundational CDM ===")
            from src.cdm_builder.build_foundational_cdm import run_step3a
            
            rationalized_dir = base_outdir / "rationalized"
            
            cdm = run_step3a(
                config=config,
                outdir=cdm_outdir,
                llm=llm,
                dry_run=dry_run,
                rationalized_dir=rationalized_dir
            )
            
            # Steps 3b/3c only run if CDM was generated (not dry run)
            if cdm:
                # Step 3b: Generate DDL
                print(f"\n=== Step 3b: Generate SQL DDL ===")
                from src.cdm_builder.generate_ddl import DDLGenerator
                
                generator = DDLGenerator(dialect="sqlserver", schema="dbo", catalog="CDM")
                ddl = generator.generate(cdm)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                domain_safe = config.cdm.domain.lower().replace(' ', '_')
                ddl_file = cdm_outdir / f"ddl_{domain_safe}_{timestamp}.sql"
                
                with open(ddl_file, 'w', encoding='utf-8') as f:
                    f.write(ddl)
                
                entity_count = len(cdm.get("entities", []))
                print(f"   ✅ DDL generated: {entity_count} tables")
                print(f"   📄 Saved to: {ddl_file}")
                
                # Step 3c: Generate LucidChart CSV
                print(f"\n=== Step 3c: Generate LucidChart CSV ===")
                from src.cdm_builder.ddl_to_lucidchart import ddl_to_lucidchart
                
                lucid_file = cdm_outdir / f"lucidchart_{domain_safe}_{timestamp}.csv"
                
                rows = ddl_to_lucidchart(
                    ddl_file=ddl_file,
                    output_file=lucid_file,
                    dialect="sqlserver",
                    schema="dbo",
                    catalog="CDM"
                )
            
            print(f"\n{'='*60}")
            print(f"✓ STEP 3 COMPLETE")
            print(f"  CDM artifacts saved to: {cdm_outdir}")
            print(f"{'='*60}")
        
        # === STEP 4-5: FUTURE ===
        if any(s in steps_to_run for s in [4, 5]):
            print(f"\nSteps 4-5 not yet implemented.")
        
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