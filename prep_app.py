"""
Prep App - Input File Rationalization
Rationalizes entities and attributes WITHIN each file type for CDM generation.
Run this once when inputs change, then use app_cdm_gen.py for CDM generation.

Supports multiple models: GPT-5, GPT-4.1, and local models (70B, 33B, 8B).
"""
import argparse
import sys
import json
import os
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from src.config import load_config
from src.converters import (
    convert_fhir_to_json,
    convert_guardrails_to_json,
    convert_ddl_to_json,
    convert_naming_standard_to_json
)
from src.core.llm_client import LLMClient
load_dotenv()

# Model configuration from .env
MODEL_OPTIONS = {
    "gpt-5": {
        "name": "gpt-5 (OpenAI - best reasoning)",
        "provider": "openai",
        "model": os.getenv("OPENAI_MODEL_5", "gpt-5"),
        "api_key": lambda: os.getenv("OPENAI_API_KEY"),
        "base_url": lambda: os.getenv("OPENAI_BASE_URL")
    },
    "gpt-4.1": {
        "name": "gpt-4.1 (OpenAI - large context)",
        "provider": "openai",
        "model": os.getenv("OPENAI_MODEL_4.1", "gpt-4.1"),
        "api_key": lambda: os.getenv("OPENAI_API_KEY"),
        "base_url": lambda: os.getenv("OPENAI_BASE_URL")
    },
    "local-70b": {
        "name": "local-70b (llama.cpp - Llama 3.3 70B)",
        "provider": "llamacpp",
        "model": os.getenv("LLAMACPP_MODEL_70B"),
        "api_key": lambda: os.getenv("LLAMACPP_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("LLAMACPP_BASE_URL")
    },
    "local-32b": {
        "name": "local-32b (unsloth/Qwen3-32B-GGUF Qwen3-32B-Q4_K_M.gguf)",
        "provider": "llamacpp",
        "model": os.getenv("LLAMACPP_MODEL_32B"),
        "api_key": lambda: os.getenv("LLAMACPP_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("LLAMACPP_BASE_URL")
    },
    "local-8b": {
        "name": "local-8b (vLLM - Llama 3.1 8B)",
        "provider": "vllm",
        "model": os.getenv("VLLM_MODEL_8B"),
        "api_key": lambda: os.getenv("VLLM_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("VLLM_BASE_URL")
    }
}


def prompt_user(message: str, default: str = "N") -> bool:
    """
    Prompt user for yes/no input with default.
    
    Args:
        message: Prompt message
        default: Default value ("Y" or "N")
        
    Returns:
        True if yes, False if no
    """
    default_display = "Y/n" if default.upper() == "Y" else "y/N"
    response = input(f"{message} ({default_display}): ").strip().upper()
    
    if not response:
        response = default.upper()
    
    return response == "Y"


def select_model() -> str:
    """
    Prompt user to select a model.
    
    Returns:
        Selected model key
    """
    print("\nSelect model for rationalization:")
    print("  1. gpt-5 (OpenAI - best reasoning) [DEFAULT]")
    print("  2. gpt-4.1 (OpenAI - large context)")
    print("  3. local-70b (llama.cpp - Llama 3.3 70B)")
    print("  4. local-33b (llama.cpp - QWEN3 32B)")
    print("  5. local-8b (vLLM - Llama 3.1 8B)")
    
    choice = input("Choice (1-5) [1]: ").strip()
    
    if not choice:
        choice = "1"
    
    model_map = {
        "1": "gpt-5",
        "2": "gpt-4.1",
        "3": "local-70b",
        "4": "local-32b",
        "5": "local-8b"
    }
    
    return model_map.get(choice, "gpt-5")


def count_tokens(text: str) -> int:
    """Rough token count estimate (4 chars per token)"""
    return len(text) // 4


def save_prompt_to_file(prompt: str, filename: str, prompts_dir: Path) -> dict:
    """Save prompt to file and return stats"""
    output_file = prompts_dir / filename
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    char_count = len(prompt)
    token_count = count_tokens(prompt)
    
    return {
        'file': str(output_file),
        'characters': char_count,
        'tokens_estimate': token_count
    }


def build_fhir_rationalization_prompt(domain: str, fhir_files: list) -> tuple[str, list]:
    """Build FHIR rationalization prompt"""
    # Convert files to JSON
    fhir_json = []
    for fhir_file in fhir_files:
        fhir_json.append({
            'filename': Path(fhir_file).name,
            'content': convert_fhir_to_json(fhir_file)
        })
    
    # Build prompt
    prompt = f"""You are a FHIR expert rationalizing multiple FHIR resource profiles for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(fhir_files)} FHIR profile files provided and rationalize them into a unified set of entities and attributes.

**Rationalization Goals:**
1. Identify all unique entities across all FHIR resources
2. Consolidate duplicate or overlapping attributes
3. Resolve conflicts between different FHIR resources
4. Preserve important FHIR metadata (types, cardinality, descriptions)
5. Create a clean, unified entity/attribute structure

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):
```json
{{
  "domain": "{domain}",
  "source": "fhir",
  "rationalized_entities": [
    {{
      "entity_name": "Coverage",
      "description": "Member insurance coverage information",
      "source_resources": ["Coverage", "InsurancePlan"],
      "attributes": [
        {{
          "attribute_name": "coverage_id",
          "fhir_path": "Coverage.identifier",
          "data_type": "Identifier",
          "cardinality": "1..*",
          "required": true,
          "description": "Unique coverage identifier",
          "source_files": ["coverage.profile.json"]
        }},
        {{
          "attribute_name": "status",
          "fhir_path": "Coverage.status",
          "data_type": "code",
          "cardinality": "1..1",
          "required": true,
          "possible_values": ["active", "cancelled", "draft"],
          "description": "Coverage status"
        }}
      ],
      "relationships": [
        {{
          "related_entity": "Plan",
          "relationship_type": "many-to-one",
          "fhir_reference": "Coverage.insurance.coverage",
          "description": "Coverage belongs to a Plan"
        }}
      ]
    }}
  ]
}}
```

**CRITICAL:** 
- Output ONLY valid JSON
- Rationalize conflicts (don't just list everything)
- Focus on PBM passthrough model needs
- Preserve cardinality and data types

---

## FHIR Profile Files

"""
    
    for i, fhir_data in enumerate(fhir_json, 1):
        prompt += f"### FHIR File {i}: {fhir_data['filename']}\n\n```json\n{fhir_data['content']}\n```\n\n"
    
    return prompt, fhir_json


def build_guardrails_rationalization_prompt(domain: str, gr_files: list) -> tuple[str, list]:
    """Build Guardrails rationalization prompt"""
    # Convert files to JSON
    gr_json = []
    for gr_file in gr_files:
        gr_json.append({
            'filename': Path(gr_file).name,
            'content': convert_guardrails_to_json(gr_file)
        })
    
    # Build prompt
    prompt = f"""You are a business analyst rationalizing multiple API specifications for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(gr_files)} Guardrails specification files and rationalize them into a unified set of business entities and attributes.

**Rationalization Goals:**
1. Identify all unique business entities across all API specifications
2. Consolidate duplicate or overlapping attributes across different APIs
3. Resolve conflicts between API versions and specifications
4. Preserve business rules and validation requirements
5. Create a clean, unified business entity/attribute structure

**This is HEAVY rationalization** - different APIs may have conflicting definitions that need resolution.

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):
```json
{{
  "domain": "{domain}",
  "source": "guardrails",
  "rationalized_entities": [
    {{
      "entity_name": "Plan",
      "description": "Insurance plan product definition",
      "source_apis": ["Hierarchy API v1.5", "Benefit Setup API v1.0"],
      "attributes": [
        {{
          "attribute_name": "plan_id",
          "data_type": "string",
          "max_length": 50,
          "required": true,
          "description": "Unique plan identifier",
          "validation_rules": ["Required", "Unique", "Alphanumeric"],
          "source_files": ["GR_Hierarchy_v1.5.xlsx", "GR_BenefitSetup_v1.0.xlsx"]
        }},
        {{
          "attribute_name": "plan_type",
          "data_type": "code",
          "required": true,
          "allowed_values": ["Medical", "Pharmacy", "Dental", "Vision"],
          "description": "Type of insurance plan"
        }}
      ],
      "business_rules": [
        {{
          "rule": "Plan must have at least one active benefit package",
          "source": "BenefitSetup API"
        }}
      ],
      "relationships": [
        {{
          "related_entity": "BenefitPackage",
          "relationship_type": "one-to-many",
          "foreign_key": "plan_id"
        }}
      ]
    }}
  ]
}}
```

**CRITICAL:**
- Output ONLY valid JSON
- Heavy rationalization needed - resolve conflicts
- Focus on PBM passthrough business model
- Preserve business rules and validation

---

## Guardrails Specification Files

"""
    
    for i, gr_data in enumerate(gr_json, 1):
        prompt += f"### Guardrails File {i}: {gr_data['filename']}\n\n```json\n{gr_data['content']}\n```\n\n"
    
    return prompt, gr_json


def build_ddl_rationalization_prompt(domain: str, ddl_files: list) -> tuple[str, list]:
    """Build DDL rationalization prompt"""
    # Convert files to JSON
    ddl_json = []
    for ddl_file in ddl_files:
        ddl_json.append({
            'filename': Path(ddl_file).name,
            'content': convert_ddl_to_json(ddl_file)
        })
    
    # Build prompt
    prompt = f"""You are a database architect rationalizing DDL schemas from multiple interface definitions for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the DDL files (which may contain multiple interface schemas) and rationalize them into a unified set of current-state entities and attributes.

**Rationalization Goals:**
1. Identify all unique tables/entities across all interface definitions
2. Consolidate duplicate or overlapping columns/attributes
3. Resolve conflicts between different interface schemas
4. Preserve data types, keys, and constraints
5. Create a clean, unified current-state entity/attribute structure

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):
```json
{{
  "domain": "{domain}",
  "source": "ddl",
  "rationalized_entities": [
    {{
      "entity_name": "plan_bpm",
      "description": "Current plan table in BPM system",
      "source_schemas": ["interface_1", "interface_2"],
      "attributes": [
        {{
          "attribute_name": "plan_id",
          "data_type": "varchar(50)",
          "nullable": false,
          "primary_key": true,
          "description": "Unique plan identifier",
          "source_tables": ["interface_1.plan", "interface_2.insurance_plan"]
        }},
        {{
          "attribute_name": "plan_name",
          "data_type": "varchar(200)",
          "nullable": false,
          "description": "Plan name"
        }}
      ],
      "keys": [
        {{
          "key_type": "primary",
          "columns": ["plan_id"]
        }},
        {{
          "key_type": "foreign",
          "columns": ["network_id"],
          "references": "network_bpm(network_id)"
        }}
      ]
    }}
  ]
}}
```

**CRITICAL:**
- Output ONLY valid JSON
- Rationalize across interface definitions
- Preserve keys and constraints
- Note: DDL represents current production state

---

## DDL Files

"""
    
    for i, ddl_data in enumerate(ddl_json, 1):
        prompt += f"### DDL File {i}: {ddl_data['filename']}\n\n```json\n{ddl_data['content']}\n```\n\n"
    
    return prompt, ddl_json


def build_naming_rationalization_prompt(domain: str, naming_files: list) -> tuple[str, list]:
    """Build Naming Standards rationalization prompt"""
    # Convert files to JSON
    naming_json = []
    for naming_file in naming_files:
        naming_json.append({
            'filename': Path(naming_file).name,
            'content': convert_naming_standard_to_json(naming_file)
        })
    
    # Build prompt
    prompt = f"""You are a data governance specialist rationalizing naming standards for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(naming_files)} naming standard files and rationalize them into a unified set of naming conventions and rules.

**Rationalization Goals:**
1. Identify all naming rules and conventions
2. Consolidate duplicate or overlapping rules
3. Resolve conflicts between different standards (note conflicts for user review if needed)
4. Create a clean, unified naming standard

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):
```json
{{
  "domain": "{domain}",
  "source": "naming_standards",
  "rationalized_rules": [
    {{
      "rule_category": "field_naming",
      "rule": "Use snake_case for all field names",
      "examples": ["plan_id", "member_first_name", "effective_date"],
      "source_files": ["Standard_2024.xlsx"]
    }},
    {{
      "rule_category": "data_types",
      "rule": "Use VARCHAR for text fields, specify max length",
      "examples": ["VARCHAR(50)", "VARCHAR(200)"],
      "source_files": ["Standard_2024.xlsx"]
    }},
    {{
      "rule_category": "prefixes",
      "rule": "Use 'is_' prefix for boolean fields",
      "examples": ["is_active", "is_primary", "is_deleted"],
      "source_files": ["Standard_2024.xlsx"]
    }}
  ],
  "rationalized_patterns": [
    {{
      "pattern_name": "identifier_fields",
      "pattern": "{{entity_name}}_id",
      "data_type": "VARCHAR(50)",
      "examples": ["plan_id", "member_id", "claim_id"]
    }},
    {{
      "pattern_name": "date_fields",
      "pattern": "{{context}}_date",
      "data_type": "DATE",
      "examples": ["effective_date", "termination_date", "created_date"]
    }}
  ]
}}
```

**CRITICAL:**
- Output ONLY valid JSON
- Resolve conflicts where possible
- If standards directly conflict, note for user review
- Focus on consistency and clarity

---

## Naming Standard Files

"""
    
    for i, naming_data in enumerate(naming_json, 1):
        prompt += f"### Naming Standard File {i}: {naming_data['filename']}\n\n```json\n{naming_data['content']}\n```\n\n"
    
    return prompt, naming_json


def rationalize_with_llm(prompt: str, llm: LLMClient, output_file: Path) -> dict:
    """Call LLM and save rationalized output"""
    print(f"  Calling {llm.model}...")
    
    start_time = time.time()
    response = llm.call(prompt)
    elapsed_time = time.time() - start_time
    
    print(f"  ⏱️  Response received in {elapsed_time:.1f} seconds")
    # Parse JSON response
    try:
        rationalized_json = json.loads(response)
    except json.JSONDecodeError:
        # Try to extract JSON from response
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            rationalized_json = json.loads(json_match.group())
        else:
            raise ValueError("LLM did not return valid JSON")
    
    # Save output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(rationalized_json, f, indent=2)
    
    return rationalized_json


def main():
    """Main entry point for prep app"""
    load_dotenv()
    
    ap = argparse.ArgumentParser(
        description="Prep App - Rationalize input files for CDM generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python prep_app.py --config config/plan_and_benefit.json
        """
    )
    
    ap.add_argument("--config", required=True, help="Path to JSON config file")
    
    args = ap.parse_args()
    
    try:
        # Load configuration
        print(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        print(f"✓ Configuration loaded for domain: {config.cdm.domain}")
        
        # Create prep output directory
        prep_outdir = Path(config.output.directory) / "prep"
        prep_outdir.mkdir(parents=True, exist_ok=True)
        
        prompts_dir = prep_outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"PREP APP - Input Rationalization")
        print(f"Domain: {config.cdm.domain}")
        print(f"Output: {prep_outdir}")
        print(f"{'='*60}")
        
        # === MODEL SELECTION ===
        selected_model = select_model()
        model_config = MODEL_OPTIONS[selected_model]
        print(f"\n✓ Selected model: {model_config['name']}")
        
        # === DRY RUN PROMPT (default: Y) ===
        dry_run = prompt_user("\nDry run mode (review prompts without calling LLM)?", default="Y")
        
        if dry_run:
            print("\n🔍 DRY RUN MODE - Prompts will be saved for review, no LLM calls made")
        else:
            print("\n🚀 LIVE MODE - LLM will be called for rationalization")
        
        # Collect what to process
        process_fhir = False
        process_guardrails = False
        process_ddl = False
        process_naming = False
        
        if config.inputs.fhir:
            print(f"\nFound {len(config.inputs.fhir)} FHIR file(s)")
            process_fhir = prompt_user("Rationalize FHIR files?", default="N")
        
        if config.inputs.guardrails:
            print(f"\nFound {len(config.inputs.guardrails)} Guardrails file(s)")
            process_guardrails = prompt_user("Rationalize Guardrails files?", default="N")
        
        if config.inputs.ddl:
            print(f"\nFound {len(config.inputs.ddl)} DDL file(s)")
            process_ddl = prompt_user("Rationalize DDL files?", default="N")
        
        if config.inputs.naming_standard:
            print(f"\nFound {len(config.inputs.naming_standard)} Naming Standard file(s)")
            process_naming = prompt_user("Rationalize Naming Standard files?", default="N")
        
        # === DRY RUN MODE ===
        if dry_run:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"\n{'='*60}")
            print("DRY RUN - Generating prompts...")
            print(f"{'='*60}")
            
            dry_run_results = {
                'domain': config.cdm.domain,
                'mode': 'dry_run',
                'selected_model': selected_model,
                'prompts': {}
            }
            
            # FHIR
            if process_fhir:
                print(f"\n📝 Generating FHIR rationalization prompt...")
                prompt, _ = build_fhir_rationalization_prompt(config.cdm.domain, config.inputs.fhir)
                stats = save_prompt_to_file(prompt, f"fhir_rationalization_prompt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['fhir'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Guardrails
            if process_guardrails:
                print(f"\n📝 Generating Guardrails rationalization prompt...")
                prompt, _ = build_guardrails_rationalization_prompt(config.cdm.domain, config.inputs.guardrails)
                stats = save_prompt_to_file(prompt, f"guardrails_rationalization_prompt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['guardrails'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # DDL
            if process_ddl:
                print(f"\n📝 Generating DDL rationalization prompt...")
                prompt, _ = build_ddl_rationalization_prompt(config.cdm.domain, config.inputs.ddl)
                stats = save_prompt_to_file(prompt, f"ddl_rationalization_prompt.txt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['ddl'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Naming
            if process_naming:
                print(f"\n📝 Generating Naming Standards rationalization prompt...")
                prompt, _ = build_naming_rationalization_prompt(config.cdm.domain, config.inputs.naming_standard)
                stats = save_prompt_to_file(prompt, f"naming_rationalization_prompt.txt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['naming'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Save dry run manifest
            manifest_file = prompts_dir / f"dry_run_manifest_{timestamp}.json"  # dry run
            with open(manifest_file, 'w', encoding='utf-8') as f:
                json.dump(dry_run_results, f, indent=2)
            
            print(f"\n{'='*60}")
            print(f"✓ DRY RUN COMPLETE")
            print(f"  Prompts saved to: {prompts_dir}")
            print(f"  Manifest: {manifest_file}")
            print(f"\nReview the prompts, then run again without dry run to execute.")
            print(f"{'='*60}")
            
            return
        
        # === LIVE MODE ===
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"\n{'='*60}")
        print(f"LIVE MODE - Calling {model_config['name']}...")
        print(f"{'='*60}")
        
        results = {
            'domain': config.cdm.domain,
            'mode': 'live',
            'selected_model': selected_model,
            'rationalized_files': {}
        }
        
        # Initialize LLM
        print(f"\n=== Initializing LLM ===")
        print(f"DEBUG: model_config = {model_config}")
        print(f"DEBUG: model = {model_config['model']}")
        print(f"DEBUG: base_url = {model_config['base_url']()}")

        llm = LLMClient(
            model=model_config['model'],
            base_url=model_config['base_url'](),
            temperature=float(os.getenv("TEMP_DEFAULT", "0.2")),
            timeout=1800
        )
        print(f"✓ LLM initialized: {llm.model}")
        
        # FHIR
        if process_fhir:
            print(f"\n=== Rationalizing FHIR files ===")
            prompt, _ = build_fhir_rationalization_prompt(config.cdm.domain, config.inputs.fhir)
            output_file = prep_outdir / f"rationalized_fhir_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['fhir'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # Guardrails
        if process_guardrails:
            print(f"\n=== Rationalizing Guardrails files ===")
            prompt, _ = build_guardrails_rationalization_prompt(config.cdm.domain, config.inputs.guardrails)
            output_file = prep_outdir / f"rationalized_guardrails_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['guardrails'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # DDL
        if process_ddl:
            print(f"\n=== Rationalizing DDL files ===")
            prompt, _ = build_ddl_rationalization_prompt(config.cdm.domain, config.inputs.ddl)
            output_file = prep_outdir / f"rationalized_ddl_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['ddl'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # Naming
        if process_naming:
            print(f"\n=== Rationalizing Naming Standards ===")
            prompt, _ = build_naming_rationalization_prompt(config.cdm.domain, config.inputs.naming_standard)
            output_file = prep_outdir / f"rationalized_naming_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['naming'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Rules: {len(rationalized.get('rationalized_rules', []))}")
        
        # Save manifest
        manifest_file = prep_outdir / f"prep_manifest_{timestamp}.json"   
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n{'='*60}")
        print(f"✓ PREP COMPLETE")
        print(f"  Manifest: {manifest_file}")
        print(f"  Rationalized files saved to: {prep_outdir}")
        print(f"\nNext step: Run app_cdm_gen.py to generate CDM")
        print(f"{'='*60}")
        
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