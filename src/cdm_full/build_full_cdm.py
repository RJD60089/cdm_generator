# src/cdm_full/build_full_cdm.py
"""
Build Full CDM (Step 6) - Main Orchestration Module

5-Step Resumable Architecture:
  1. discover_sources()      - Find rationalized files
  2. initialize_full_cdm()   - Create from foundational CDM
  3. generate_match_files()  - Per-source AI mapping (interactive per-source)
  4. apply_match_files()     - Merge all matches into full CDM
  5. generate_gap_report()   - Post-process unmapped fields

Output Location: output/{cdm_name}/full_cdm/

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select Step 6

Orchestrator controls interactive flow - prompts user for each source.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient

# Import from submodules
from src.cdm_full.discover import (
    discover_sources,
    get_discovered_sources,
    get_existing_match_files,
    find_existing_match_file
)
from src.cdm_full.initialize import (
    find_latest_foundational_cdm,
    initialize_full_cdm
)
from src.cdm_full.match_generator import (
    generate_match_file
)
from src.cdm_full.match_applier import (
    apply_match_files
)
from src.cdm_full.gap_report import (
    generate_gap_report,
    generate_summary
)


# Re-export for orchestrator convenience
__all__ = [
    'run_build_full_cdm',
    'get_discovered_sources',
    'get_existing_match_files'
]


def run_build_full_cdm(
    config: AppConfig,
    cdm_file: Optional[Path],
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False,
    sources_to_map: Optional[List[str]] = None,
    skip_mapping: bool = False,
    generate_cdm: bool = True,
    run_gap_analysis: bool = True
) -> Optional[Dict]:
    """
    Main entry point for Full CDM generation (Step 6).
    Orchestrator controls flow via parameters.
    
    Args:
        config: App configuration
        cdm_file: Path to foundational CDM (None to auto-find latest)
        outdir: Base output directory (e.g., output/plan)
        llm: LLM client
        dry_run: If True, save prompts only
        sources_to_map: List of source types to map (None = use existing match files only)
        skip_mapping: If True, skip all mapping and use existing match files
        generate_cdm: If True, generate full CDM from match files
        run_gap_analysis: If True, generate gap report
        
    Returns:
        Full CDM dict (None if dry_run or generate_cdm=False)
    """
    
    print(f"\n{'='*60}")
    print(f"STEP 6: BUILD FULL CDM")
    print(f"{'='*60}")
    print(f"   Domain: {config.cdm.domain}")
    
    # Setup directories
    cdm_dir = outdir / "cdm"
    rationalized_dir = outdir / "rationalized"
    full_cdm_dir = outdir / "full_cdm"
    full_cdm_dir.mkdir(parents=True, exist_ok=True)
    
    # Get domain description
    domain_description = getattr(config.cdm, 'description', '') or \
        "Pharmacy Benefits Management (PBM) with pass-through pricing model"
    
    # ─────────────────────────────────────────────────────────────
    # STEP 1: Discover Sources
    # ─────────────────────────────────────────────────────────────
    print(f"\n   [Step 1/5] Discovering sources...")
    
    if not rationalized_dir.exists():
        print(f"   ❌ ERROR: Rationalized directory not found: {rationalized_dir}")
        return None
    
    discovered_sources = discover_sources(rationalized_dir, config.cdm.domain)
    
    if not discovered_sources:
        print(f"   ❌ ERROR: No rationalized files found for domain '{config.cdm.domain}'")
        return None
    
    source_types = sorted(discovered_sources.keys())
    print(f"   Found {len(source_types)} sources: {', '.join(source_types)}")
    
    # ─────────────────────────────────────────────────────────────
    # STEP 2: Initialize Full CDM
    # ─────────────────────────────────────────────────────────────
    print(f"\n   [Step 2/5] Initializing Full CDM...")
    
    if cdm_file is None:
        cdm_file = find_latest_foundational_cdm(cdm_dir, config.cdm.domain)
        if cdm_file is None:
            print(f"   ❌ ERROR: No foundational CDM found in {cdm_dir}")
            return None
    
    print(f"   Source CDM: {cdm_file.name}")
    
    with open(cdm_file, 'r', encoding='utf-8') as f:
        foundational_cdm = json.load(f)
    
    entity_count = len(foundational_cdm.get("entities", []))
    print(f"   Entities: {entity_count}")
    
    full_cdm = initialize_full_cdm(foundational_cdm, source_types, domain_description)
    
    # Save initialized full CDM
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = config.cdm.domain.lower().replace(' ', '_')
    init_file = full_cdm_dir / f"full_cdm_initialized_{domain_safe}_{timestamp}.json"
    with open(init_file, 'w', encoding='utf-8') as f:
        json.dump(full_cdm, f, indent=2)
    print(f"   Saved: {init_file.name}")
    
    # ─────────────────────────────────────────────────────────────
    # STEP 3: Generate Match Files (per-source, orchestrator-controlled)
    # ─────────────────────────────────────────────────────────────
    print(f"\n   [Step 3/5] Processing match files...")
    
    # Load source entities for later use in apply
    source_entities_lookup = {}
    for source_type, source_file in discovered_sources.items():
        with open(source_file, 'r', encoding='utf-8') as f:
            rationalized = json.load(f)
        source_entities_lookup[source_type] = {
            e.get("entity_name"): e for e in rationalized.get("entities", [])
        }
    
    match_files = {}
    
    # Determine which sources to process
    if skip_mapping:
        sources_to_process = []
        print(f"   Skipping mapping - will use existing match files")
    elif sources_to_map is not None:
        sources_to_process = [s for s in sources_to_map if s in source_types]
    else:
        sources_to_process = source_types
    
    # Generate match files for selected sources
    for source_type in source_types:
        existing = find_existing_match_file(full_cdm_dir, source_type)
        
        if source_type in sources_to_process:
            # Generate new match file
            match_file = generate_match_file(
                config=config,
                source_type=source_type,
                rationalized_file=discovered_sources[source_type],
                full_cdm=full_cdm,
                llm=llm,
                full_cdm_dir=full_cdm_dir,
                domain_description=domain_description,
                dry_run=dry_run
            )
            
            if match_file:
                match_files[source_type] = match_file
            elif existing:
                # Dry run or failure - use existing if available
                match_files[source_type] = existing
        elif existing:
            # Use existing match file
            print(f"   {source_type.upper()}: Using existing {existing.name}")
            match_files[source_type] = existing
        else:
            print(f"   ⚠️  {source_type.upper()}: No match file found")
    
    if dry_run:
        print(f"\n   🔍 DRY RUN complete. Review prompts in {full_cdm_dir / 'prompts'}")
        return None
    
    if not generate_cdm:
        print(f"\n   ○ CDM generation skipped by user")
        return None
    
    if not match_files:
        print(f"   ❌ ERROR: No match files available to apply")
        return None
    
    # ─────────────────────────────────────────────────────────────
    # STEP 4: Apply Match Files
    # ─────────────────────────────────────────────────────────────
    print(f"\n   [Step 4/5] Applying match files...")
    
    full_cdm, application_report = apply_match_files(
        full_cdm, match_files, source_entities_lookup
    )
    
    # ─────────────────────────────────────────────────────────────
    # STEP 5: Generate Gap Report
    # ─────────────────────────────────────────────────────────────
    gap_file = None
    if run_gap_analysis:
        print(f"\n   [Step 5/5] Generating gap report...")
        gap_file = generate_gap_report(application_report, full_cdm_dir, config.cdm.domain)
    else:
        print(f"\n   [Step 5/5] Gap analysis skipped by user")
    
    # ─────────────────────────────────────────────────────────────
    # Save Final Full CDM
    # ─────────────────────────────────────────────────────────────
    
    # Add summary
    full_cdm["summary"] = generate_summary(full_cdm, source_types)
    
    # Remove normalized fields (internal use only)
    for entity in full_cdm.get("entities", []):
        entity.pop("entity_name_normalized", None)
        for attr in entity.get("attributes", []):
            attr.pop("attribute_name_normalized", None)
    
    # Save full CDM
    final_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_cdm_file = full_cdm_dir / f"cdm_{domain_safe}_full_{final_timestamp}.json"
    with open(full_cdm_file, 'w', encoding='utf-8') as f:
        json.dump(full_cdm, f, indent=2)
    
    print(f"\n   ✅ Full CDM saved: {full_cdm_file.name}")
    
    # Save application report
    report_file = full_cdm_dir / f"disposition_{domain_safe}_{final_timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(application_report, f, indent=2)
    print(f"   📋 Disposition: {report_file.name}")
    
    # Print summary
    summary = full_cdm.get("summary", {})
    print(f"\n   {'─'*50}")
    print(f"   FULL CDM SUMMARY")
    print(f"   {'─'*50}")
    print(f"   Entities: {summary.get('total_entities', 0)}")
    print(f"   Attributes: {summary.get('total_attributes', 0)}")
    print(f"   Relationships: {summary.get('total_relationships', 0)}")
    print(f"   Attribute coverage:")
    for source, count in summary.get("attribute_coverage_by_source", {}).items():
        print(f"     - {source}: {count} attributes mapped")
    print(f"   Total mapped: {application_report.get('total_mapped', 0)}")
    print(f"   Total unmapped: {application_report.get('total_unmapped', 0)}")
    print(f"   Total requires review: {application_report.get('total_requires_review', 0)}")
    
    if application_report.get("application_errors"):
        print(f"   ⚠️  Application errors: {len(application_report['application_errors'])}")
    
    print(f"\n{'='*60}")
    print(f"FULL CDM BUILD COMPLETE")
    print(f"{'='*60}")
    
    return full_cdm


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    import sys
    print("This module should be run via cdm_orchestrator.py")
    print("Usage: python cdm_orchestrator.py plan  # then select Step 6")
    sys.exit(1)