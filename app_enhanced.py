"""
CDM Generation Application - Enhanced with config file support
"""
import argparse
import sys
from pathlib import Path

from src.config import load_config, create_default_output_filename
from src.converters import (
    convert_fhir_to_json,
    convert_guardrails_to_json,
    convert_ddl_to_json,
    convert_naming_standard_to_json
)
from src.core.llm_client import LLMClient
from src.steps.step0_consolidation import run_step0
from src.steps.step1_requirements import run_step1
from dotenv import load_dotenv
load_dotenv()

def main():
    """Main entry point for CDM generation"""
    ap = argparse.ArgumentParser(
        description="Generate CDM (Canonical Data Model) for pharmacy benefit management domains",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using config file (recommended)
  python app.py --config config/plan_and_benefit.json
  
  # Quick command-line (for testing)
  python app.py --domain "Prescriber" --fhir inputs/fhir/practitioner.json
        """
    )
    
    # Config file or command-line arguments
    ap.add_argument("--config", help="Path to JSON config file (recommended)")
    
    # Command-line arguments (alternative to config file)
    ap.add_argument("--domain", help="CDM domain name")
    ap.add_argument("--fhir", nargs="+", help="Path(s) to FHIR profile JSON file(s)")
    ap.add_argument("--guardrails", nargs="+", help="Path(s) to Guardrails Excel file(s)")
    ap.add_argument("--ddl", nargs="+", help="Path(s) to DDL file(s) (JSON or SQL)")
    ap.add_argument("--naming-standard", nargs="+", help="Path(s) to naming standard Excel file(s)")
    ap.add_argument("--outdir", default="output", help="Output directory")
    
    args = ap.parse_args()
    
    try:
        # Load configuration
        if args.config:
            # Config file mode
            print(f"Loading configuration from: {args.config}")
            config = load_config(args.config)
            print(f"✓ Configuration loaded for domain: {config.cdm.domain}")
        else:
            # Command-line mode
            if not args.domain:
                print("ERROR: Either --config or --domain is required", file=sys.stderr)
                ap.print_help()
                sys.exit(1)
            
            # Build config from command-line args
            from src.config import AppConfig, CDMConfig, InputsConfig, OutputConfig
            config = AppConfig(
                cdm=CDMConfig(domain=args.domain),
                inputs=InputsConfig(
                    fhir=args.fhir,
                    guardrails=args.guardrails,
                    ddl=args.ddl,
                    naming_standard=args.naming_standard
                ),
                output=OutputConfig(directory=args.outdir)
            )
            
            # Validate
            errors = config.validate()
            if errors:
                print("ERROR: Configuration validation failed:", file=sys.stderr)
                for error in errors:
                    print(f"  - {error}", file=sys.stderr)
                sys.exit(1)
        
        # Create output directory
        Path(config.output.directory).mkdir(parents=True, exist_ok=True)
        
        # Convert input files to JSON strings
        print("\n=== Converting Input Files ===")
        inputs_json = {
            'fhir': [],
            'guardrails': [],
            'ddl': [],
            'naming_standard': []
        }
        
        # FHIR files (multiple)
        if config.inputs.fhir:
            print(f"Converting {len(config.inputs.fhir)} FHIR file(s)...")
            for fhir_file in config.inputs.fhir:
                print(f"  Converting: {fhir_file}")
                inputs_json['fhir'].append({
                    'filename': Path(fhir_file).name,
                    'content': convert_fhir_to_json(fhir_file)
                })
            print(f"  ✓ {len(config.inputs.fhir)} FHIR file(s) converted")
        
        # Guardrails files (multiple)
        if config.inputs.guardrails:
            print(f"Converting {len(config.inputs.guardrails)} Guardrails file(s)...")
            for gr_file in config.inputs.guardrails:
                print(f"  Converting: {gr_file}")
                inputs_json['guardrails'].append({
                    'filename': Path(gr_file).name,
                    'content': convert_guardrails_to_json(gr_file)
                })
            print(f"  ✓ {len(config.inputs.guardrails)} Guardrails file(s) converted")
        
        # DDL files (multiple)
        if config.inputs.ddl:
            print(f"Converting {len(config.inputs.ddl)} DDL file(s)...")
            for ddl_file in config.inputs.ddl:
                print(f"  Converting: {ddl_file}")
                inputs_json['ddl'].append({
                    'filename': Path(ddl_file).name,
                    'content': convert_ddl_to_json(ddl_file)
                })
            print(f"  ✓ {len(config.inputs.ddl)} DDL file(s) converted")
        
        # Naming standard files (multiple)
        if config.inputs.naming_standard:
            print(f"Converting {len(config.inputs.naming_standard)} naming standard file(s)...")
            for ns_file in config.inputs.naming_standard:
                print(f"  Converting: {ns_file}")
                inputs_json['naming_standard'].append({
                    'filename': Path(ns_file).name,
                    'content': convert_naming_standard_to_json(ns_file)
                })
            print(f"  ✓ {len(config.inputs.naming_standard)} naming standard file(s) converted")
        
        # Remove empty lists
        inputs_json = {k: v for k, v in inputs_json.items() if v}
        
        if not inputs_json:
            print("WARNING: No input files provided. CDM will be generated from domain name only.")
        
        # Initialize LLM client from environment
        print(f"\n=== Initializing LLM ===")
        llm = LLMClient.from_env()
        print(f"✓ LLM client initialized: {llm.model}")
        
        # Run Step 0: Consolidation (creates two separate files)
        fhir_state = None
        guardrails_state = None
        
        if inputs_json.get('fhir') or inputs_json.get('guardrails'):
            fhir_state, guardrails_state = run_step0(
                domain=config.cdm.domain,
                inputs_json=inputs_json,
                llm=llm,
                outdir=config.output.directory
            )
            
            print("\n✓ Step 0 Consolidation Complete!")
            if fhir_state:
                print(f"  FHIR: {fhir_state.metadata['resources_count']} resources consolidated")
                print(f"        {fhir_state.output_file}")
            if guardrails_state:
                print(f"  Guardrails: {guardrails_state.metadata['entities_count']} entities consolidated")
                print(f"             {guardrails_state.output_file}")
            
            # Add consolidated files to inputs for Step 1
            if fhir_state:
                with open(fhir_state.output_file, 'r') as f:
                    inputs_json['consolidated_fhir'] = [{
                        'filename': 'consolidated_fhir.json',
                        'content': f.read()
                    }]
            
            if guardrails_state:
                with open(guardrails_state.output_file, 'r') as f:
                    inputs_json['consolidated_guardrails'] = [{
                        'filename': 'consolidated_guardrails.json',
                        'content': f.read()
                    }]
        
        # Run Step 1: Requirements Gathering (will reconcile the two consolidated files)
        print(f"\n=== Step 1: Requirements Gathering & Reconciliation for {config.cdm.domain} ===")
        state = run_step1(
            domain=config.cdm.domain,
            inputs_json=inputs_json,
            llm=llm,
            outdir=config.output.directory
        )
        
        print(f"\n✓ Step 1 complete!")
        print(f"  Output: {state.output_file}")
        
        # Future: Run additional steps (2-5)
        print("\n=== Next Steps ===")
        print("Step 2-5 will be implemented in future phases")
        print("Current output contains requirements document for CDM generation")
        
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