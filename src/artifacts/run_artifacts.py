# src/artifacts/run_artifacts.py
"""
Artifact Generation Orchestrator (Step 7)

Generates deliverable artifacts from Full CDM.
Requires Step 6 (Build Full CDM) to be completed first.

Artifacts available:
- Excel CDM (13 tabs - primary deliverable with all content)
- DDL Word Document (SQL DDL script only)
- DDL SQL File (raw .sql file)
- LucidChart CSV (for ERD import)

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select Step 7
"""

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from src.config.config_parser import AppConfig


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
    """Find latest gaps file."""
    domain_safe = domain.lower().replace(' ', '_')
    full_cdm_dir = outdir / "full_cdm"
    
    if not full_cdm_dir.exists():
        return None
    
    pattern = f"gaps_{domain_safe}_*.json"
    matches = list(full_cdm_dir.glob(pattern))
    
    if not matches:
        return None
    
    matches.sort(reverse=True)
    return matches[0]


def find_consolidation_file(outdir: Path, domain: str) -> Optional[Path]:
    """Find latest consolidation recommendations file."""
    domain_safe = domain.lower().replace(' ', '_')
    cdm_dir = outdir / "cdm"
    
    if not cdm_dir.exists():
        return None
    
    pattern = f"consolidation_recommendations_{domain_safe}_*.json"
    matches = list(cdm_dir.glob(pattern))
    
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


def prompt_dialect() -> str:
    """Prompt user for SQL dialect."""
    print("\n   Select SQL dialect for DDL:")
    print("   [1] SQL Server (default)")
    print("   [2] PostgreSQL")
    print("   [3] MySQL")
    
    choice = input("   Dialect [1]: ").strip() or "1"
    
    dialect_map = {"1": "sqlserver", "2": "postgresql", "3": "mysql"}
    return dialect_map.get(choice, "sqlserver")


def prompt_schema(dialect: str) -> str:
    """Prompt user for schema name."""
    default = "dbo" if dialect == "sqlserver" else "public"
    schema = input(f"   Schema name [{default}]: ").strip() or default
    return schema


# =============================================================================
# MAIN GENERATOR
# =============================================================================

def run_artifact_generation(
    config: AppConfig,
    outdir: Path,
    cdm_file: Path,
    generate_excel_flag: bool = False,
    generate_ddl_word_flag: bool = False,
    generate_ddl_sql_flag: bool = False,
    generate_lucidchart_flag: bool = False,
    dialect: str = "sqlserver",
    schema: str = "dbo"
) -> Dict[str, Path]:
    """
    Run artifact generation from Full CDM.
    
    Args:
        config: App configuration
        outdir: Base output directory (e.g., output/plan)
        cdm_file: Path to Full CDM file
        generate_excel_flag: Generate Excel CDM
        generate_ddl_word_flag: Generate Word DDL Document
        generate_ddl_sql_flag: Generate DDL SQL file
        generate_lucidchart_flag: Generate LucidChart CSV
        dialect: SQL dialect for DDL
        schema: Database schema name
    
    Returns:
        Dict of artifact_type -> output file path
    """
    
    artifacts_dir = outdir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Load CDM for entity count display
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    entity_count = len(cdm.get("entities", []))
    print(f"   Entities: {entity_count}")
    
    outputs = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = config.cdm.domain.lower().replace(' ', '_')
    
    # Find supporting files
    gaps_file = find_gaps_file(outdir, config.cdm.domain)
    consolidation_file = find_consolidation_file(outdir, config.cdm.domain)
    
    if gaps_file:
        print(f"   Gaps file: {gaps_file.name}")
    if consolidation_file:
        print(f"   Consolidation file: {consolidation_file.name}")
    
    # Generate Excel CDM
    if generate_excel_flag:
        print(f"\n   Generating Excel CDM...")
        
        from src.artifacts.excel.generate_excel_cdm import generate_excel_cdm
        
        excel_file = artifacts_dir / f"{domain_safe}_CDM_{timestamp}.xlsx"
        
        generate_excel_cdm(
            config=config,
            cdm_path=cdm_file,
            output_path=excel_file,
            gaps_path=gaps_file,
            consolidation_path=consolidation_file,
            erd_url=None
        )
        
        outputs["excel"] = excel_file
    
    # Generate DDL SQL file
    ddl_path = None
    if generate_ddl_sql_flag or generate_lucidchart_flag:
        print(f"\n   Generating DDL SQL file ({dialect})...")
        
        from src.artifacts.word.generate_ddl import generate_ddl_file
        from src.artifacts.common.cdm_extractor import CDMExtractor
        
        extractor = CDMExtractor(cdm_path=cdm_file)
        
        ddl_path = generate_ddl_file(
            extractor=extractor,
            outdir=outdir,
            domain=config.cdm.domain,
            dialect=dialect,
            schema=schema
        )
        
        outputs["ddl_sql"] = ddl_path
        print(f"   ✓ DDL SQL: {ddl_path.name}")
    
    # Generate LucidChart CSV (requires DDL)
    if generate_lucidchart_flag and ddl_path:
        print(f"\n   Generating LucidChart CSV...")
        
        from src.artifacts.word.generate_lucidchart_csv import generate_lucidchart_files
        
        csv_files = generate_lucidchart_files(
            ddl_path=ddl_path,
            outdir=outdir,
            domain=config.cdm.domain,
            dialect=dialect,
            schema=schema
        )
        
        for name, path in csv_files.items():
            outputs[f"csv_{name}"] = path
            print(f"   ✓ {name}: {path.name}")
    
    # Generate Word DDL Document
    if generate_ddl_word_flag:
        print(f"\n   Generating Word DDL Document...")
        
        from src.artifacts.word.generate_word_cdm import generate_word_ddl
        
        word_file = generate_word_ddl(
            config=config,
            cdm_path=cdm_file,
            outdir=outdir,
            dialect=dialect,
            schema=schema
        )
        
        outputs["word_ddl"] = word_file
    
    return outputs


# =============================================================================
# INTERACTIVE ENTRY POINT
# =============================================================================

def interactive_artifact_generation(config: AppConfig, outdir: Path) -> Dict[str, Path]:
    """
    Interactive artifact generation with Y/N prompts.
    Called from orchestrator.
    
    Args:
        config: App configuration
        outdir: Base output directory
    
    Returns:
        Dict of artifact_type -> output file path
    """
    
    # Find Full CDM (required)
    cdm_file = find_full_cdm(outdir, config.cdm.domain)
    
    if not cdm_file:
        print(f"\n   ❌ No Full CDM found. Run Step 6 first.")
        print(f"      Expected location: {outdir / 'full_cdm'}")
        return {}
    
    print(f"\n   Source: {cdm_file.name}")
    
    # Y/N prompts for each artifact
    print(f"\n   {'─'*50}")
    print(f"   SELECT ARTIFACTS TO GENERATE")
    print(f"   {'─'*50}")
    
    generate_excel_flag = prompt_yes_no("Generate Excel CDM (13 tabs)?", default="Y")
    generate_ddl_word_flag = prompt_yes_no("Generate Word DDL Script?", default="N")
    generate_ddl_sql_flag = prompt_yes_no("Generate DDL SQL file?", default="Y")
    generate_lucidchart_flag = prompt_yes_no("Generate LucidChart CSV?", default="Y")
    
    if not any([generate_excel_flag, generate_ddl_word_flag, generate_ddl_sql_flag, generate_lucidchart_flag]):
        print("\n   No artifacts selected.")
        return {}
    
    # Get dialect and schema (needed for DDL-related artifacts)
    dialect = "sqlserver"
    schema = "dbo"
    
    if any([generate_ddl_word_flag, generate_ddl_sql_flag, generate_lucidchart_flag]):
        dialect = prompt_dialect()
        schema = prompt_schema(dialect)
    
    # Generate
    print(f"\n   {'─'*50}")
    print(f"   GENERATING ARTIFACTS")
    print(f"   {'─'*50}")
    
    outputs = run_artifact_generation(
        config=config,
        outdir=outdir,
        cdm_file=cdm_file,
        generate_excel_flag=generate_excel_flag,
        generate_ddl_word_flag=generate_ddl_word_flag,
        generate_ddl_sql_flag=generate_ddl_sql_flag,
        generate_lucidchart_flag=generate_lucidchart_flag,
        dialect=dialect,
        schema=schema
    )
    
    # Summary
    if outputs:
        print(f"\n   {'─'*50}")
        print(f"   COMPLETE - {len(outputs)} artifact(s) generated")
        print(f"   Output: {outdir / 'artifacts'}")
        print(f"   {'─'*50}")
    
    return outputs


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    import sys
    print("This module should be run via cdm_orchestrator.py")
    print("Usage: python cdm_orchestrator.py plan  # then select Step 7")
    sys.exit(1)