# src/cdm_full/build_full_cdm.py
"""
Build Full CDM (Step 5) - Main Orchestration Module

5-Step Resumable Architecture:
  1. discover_sources()      - Find rationalized files
  2. initialize_full_cdm()   - Create from foundational CDM
  3. generate_match_files()  - Per-source AI mapping (interactive per-source)
  4. apply_match_files()     - Merge all matches into full CDM
  5. generate_gap_report()   - Post-process unmapped fields

Output Location: output/{cdm_name}/full_cdm/

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select Step 5

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
    run_gap_analysis: bool = True,
    match_workers: int = 1,
) -> Optional[Dict]:
    """
    Main entry point for Full CDM generation (Step 5).
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
        match_workers: Per-entity concurrency in generate_match_file
            (default 1 = sequential).  Tier 4: 8-16 reasonable.

    Returns:
        Full CDM dict (None if dry_run or generate_cdm=False)
    """
    
    print(f"\n{'='*60}")
    print(f"STEP 5: BUILD FULL CDM")
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
                dry_run=dry_run,
                max_workers=match_workers,
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
    # REFINER GATE: Ancillary gap-driven CDM refinement
    # ─────────────────────────────────────────────────────────────
    # Triggered when:
    #   1. Config has ancillary sources with mode=refiner
    #   2. Gap report has unmapped ancillary fields
    # Maps first, then refines based on actual mapping failures.

    # Find ancillary source types in the pipeline (any source starting with "ancillary")
    ancillary_source_types = [st for st in source_types if st.startswith("ancillary")]
    refiner_source_ids = (
        [a.get("source_id") for a in config.get_ancillary_by_mode("refiner")]
        if hasattr(config, "get_ancillary_by_mode") else []
    )
    refiner_sources_in_pipeline = [
        st for st in ancillary_source_types if st in refiner_source_ids
    ]

    # Skip refiner gate if refiner data was already included in the foundational CDM
    refiner_in_foundational = foundational_cdm.get("source_files", {}).get("ancillary_refiner") is not None
    if refiner_in_foundational and refiner_sources_in_pipeline:
        print(f"\n   Refiner ancillary data was included in foundational CDM — skipping refiner gate.")
        refiner_sources_in_pipeline = []

    if refiner_sources_in_pipeline and gap_file:

        # Load gap report to check for ancillary unmapped fields
        with open(gap_file, 'r', encoding='utf-8') as f:
            gap_data = json.load(f)

        # Restrict the gate to REFINER-mode unmapped fields only.  Mapper-
        # mode ancillaries are explicitly source-to-target mapping only —
        # they must NOT drive CDM modifications.  Pre-filter both the
        # trigger count and the data passed to refinement.
        refiner_set = set(refiner_sources_in_pipeline)
        unmapped = gap_data.get("unmapped_fields", [])
        ancillary_unmapped = [
            u for u in unmapped
            if (u.get("source_type") or "") in refiner_set
        ]

        if ancillary_unmapped:
            print(f"\n   {'─'*50}")
            print(f"   ANCILLARY REFINER GATE")
            print(f"   {'─'*50}")
            print(f"   Refiner-source unmapped fields: {len(ancillary_unmapped)}")
            print(f"   Refiner sources: {', '.join(refiner_sources_in_pipeline)}")

            # Refinement runs unconditionally when the data-level conditions
            # above are satisfied.  The config's refiner-mode declaration is
            # the user's intent statement that this source should refine the
            # CDM; re-prompting "Run refinement?" at this point asks the user
            # to re-confirm a decision they already made at config time.

            # Merge rationalized data ONLY from refiner-mode ancillary
            # sources — mapper sources are excluded so their entities
            # cannot influence CDM refinement.
            merged_ancillary_data = {"entities": []}
            for anc_src in refiner_sources_in_pipeline:
                anc_file = discovered_sources.get(anc_src)
                if anc_file:
                    with open(anc_file, 'r', encoding='utf-8') as f:
                        anc_data = json.load(f)
                    merged_ancillary_data["entities"].extend(
                        anc_data.get("entities", [])
                    )

            from src.cdm_full.refine_from_gaps import run_ancillary_gap_refinement

            refined_cdm, was_modified = run_ancillary_gap_refinement(
                cdm=full_cdm,
                gap_report=gap_data,
                ancillary_data=merged_ancillary_data,
                config=config,
                llm=llm,
                outdir=full_cdm_dir,
                domain=config.cdm.domain,
                dry_run=dry_run,
            )

            if was_modified:
                full_cdm = refined_cdm

                # Re-map all ancillary sources against modified CDM
                print(f"\n   Re-mapping ancillary sources against modified CDM...")
                re_init_cdm = initialize_full_cdm(
                    full_cdm, source_types, domain_description
                )

                for anc_source in ancillary_source_types:
                    anc_match = generate_match_file(
                        config=config,
                        source_type=anc_source,
                        rationalized_file=discovered_sources[anc_source],
                        full_cdm=re_init_cdm,
                        llm=llm,
                        full_cdm_dir=full_cdm_dir,
                        domain_description=domain_description,
                        dry_run=dry_run,
                        max_workers=match_workers,
                    )
                    if anc_match:
                        match_files[anc_source] = anc_match

                # Re-apply ALL match files
                full_cdm, application_report = apply_match_files(
                    re_init_cdm, match_files, source_entities_lookup
                )

                # Re-generate gap report
                gap_file = generate_gap_report(
                    application_report, full_cdm_dir, config.cdm.domain
                )

                # Report improvement
                with open(gap_file, 'r', encoding='utf-8') as f:
                    new_gaps = json.load(f)
                new_unmapped = len([
                    u for u in new_gaps.get("unmapped_fields", [])
                    if u.get("source_type", "").lower().startswith("ancillary")
                ])
                print(f"\n   Ancillary unmapped: {len(ancillary_unmapped)} -> {new_unmapped}")
        else:
            print(f"\n   No unmapped ancillary fields — refiner gate not triggered.")

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