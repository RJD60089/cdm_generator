# src/cdm_full/discover.py
"""
Source discovery for Full CDM generation.

Functions:
  - discover_sources(): Find rationalized files for a domain
  - get_discovered_sources(): Helper for orchestrator
  - get_existing_match_files(): Find existing match files
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict


def discover_sources(rationalized_dir: Path, domain: str) -> Dict[str, Path]:
    """
    Discover available source types from rationalized directory.
    Looks for files matching: rationalized_{source_type}_{domain}_{timestamp}.json
    
    Args:
        rationalized_dir: Path to rationalized files directory
        domain: CDM domain name (e.g., "Plan", "Utilization Management")
    
    Returns:
        Dict mapping source_type -> latest file path
    """
    discovered = {}
    # Normalize domain: "Utilization Management" -> "utilization_management"
    domain_normalized = domain.lower().replace(' ', '_')
    
    pattern = "rationalized_*.json"
    for filepath in rationalized_dir.glob(pattern):
        filename = filepath.stem  # rationalized_fhir_Utilization_Management_20251201_134436
        parts = filename.split('_')
        
        # Expected: rationalized_{source}_{domain...}_{date}_{time}
        # Domain may have multiple underscored parts (e.g., Utilization_Management)
        # Last two parts are always date (YYYYMMDD) and time (HHMMSS)
        if len(parts) >= 5 and parts[0] == 'rationalized':
            source_type = parts[1].lower()
            
            # Reconstruct domain from parts between source_type and timestamp
            # parts[2:-2] = domain parts, parts[-2:] = date, time
            file_domain = '_'.join(parts[2:-2]).lower()
            
            # Only include files for this domain
            if file_domain != domain_normalized:
                continue
            
            # Track latest file per source type (by timestamp in filename)
            if source_type not in discovered:
                discovered[source_type] = filepath
            else:
                # Compare timestamps (last two parts: date_time)
                current_ts = '_'.join(parts[-2:])
                existing_parts = discovered[source_type].stem.split('_')
                existing_ts = '_'.join(existing_parts[-2:])
                if current_ts > existing_ts:
                    discovered[source_type] = filepath
    
    return discovered


def get_discovered_sources(outdir: Path, domain: str) -> Dict[str, Path]:
    """
    Helper for orchestrator to get discovered sources before prompting user.
    
    Args:
        outdir: Base output directory (e.g., output/plan)
        domain: CDM domain name
    
    Returns:
        Dict of source_type -> file path
    """
    rationalized_dir = outdir / "rationalized"
    if not rationalized_dir.exists():
        return {}
    return discover_sources(rationalized_dir, domain)


def get_existing_match_files(outdir: Path) -> Dict[str, Path]:
    """
    Helper for orchestrator to check for existing match files.
    
    Args:
        outdir: Base output directory (e.g., output/plan)
    
    Returns:
        Dict of source_type -> match file path (latest per source)
    """
    full_cdm_dir = outdir / "full_cdm"
    if not full_cdm_dir.exists():
        return {}
    
    existing = {}
    for match_file in full_cdm_dir.glob("match_*.json"):
        # match_{source_type}_{timestamp}.json
        parts = match_file.stem.split('_')
        if len(parts) >= 2:
            source_type = parts[1]
            if source_type not in existing:
                existing[source_type] = match_file
            elif match_file.name > existing[source_type].name:
                existing[source_type] = match_file
    
    return existing


def find_existing_match_file(full_cdm_dir: Path, source_type: str) -> Path | None:
    """
    Find existing match file for a specific source type.
    
    Args:
        full_cdm_dir: Path to full_cdm output directory
        source_type: Source type (e.g., "guardrails")
    
    Returns:
        Path to latest match file, or None if not found
    """
    pattern = f"match_{source_type}_*.json"
    matches = list(full_cdm_dir.glob(pattern))
    if not matches:
        return None
    
    # Return latest by filename
    matches.sort(reverse=True)
    return matches[0]