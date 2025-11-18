# src/steps/step1d_ncpdp.py
"""
Step 1d: NCPDP Rationalization

Filters full NCPDP standards to domain-relevant fields using rule-based filtering.
Reduces prompt size for Step 2b by filtering 1,140+ fields to ~300 relevant fields.

Input: Full NCPDP standards + domain filter config
Output: Rationalized NCPDP JSON with only relevant fields
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from src.config.config_parser import AppConfig


def filter_by_standards(field: dict, allowed_standards: List[str]) -> bool:
    """
    Check if NCPDP field belongs to allowed standards.
    
    Args:
        field: NCPDP field dict with 's' property (e.g., "T,A,F")
        allowed_standards: List of standard codes to include (e.g., ["T", "F", "H"])
    
    Returns:
        True if field belongs to any allowed standard
    """
    field_standards = field.get('s', '').split(',')
    field_standards = [s.strip() for s in field_standards if s.strip()]
    
    return any(std in allowed_standards for std in field_standards)


def rationalize_ncpdp(
    ncpdp_data: dict,
    domain: str,
    filter_config: Optional[Dict[str, Any]] = None
) -> dict:
    """
    Filter NCPDP fields based on standards only.
    
    Args:
        ncpdp_data: Full NCPDP standards dict with 'fields' array
        domain: CDM domain name (e.g., "Plan and Benefit")
        filter_config: Optional filter configuration with 'standards'
    
    Returns:
        Filtered NCPDP dict with metadata
    """
    original_count = len(ncpdp_data.get('fields', []))
    
    if not filter_config:
        # No filtering - return original with metadata
        return {
            **ncpdp_data,
            "_filter_metadata": {
                "original_field_count": original_count,
                "filtered_field_count": original_count,
                "domain": domain,
                "filter_date": datetime.now().isoformat(),
                "filter_approach": "none",
                "note": "No filter configuration provided - all fields included"
            }
        }
    
    allowed_standards = filter_config.get('standards', [])
    
    # If no standards specified, return all fields
    if not allowed_standards:
        return {
            **ncpdp_data,
            "_filter_metadata": {
                "original_field_count": original_count,
                "filtered_field_count": original_count,
                "domain": domain,
                "filter_date": datetime.now().isoformat(),
                "filter_approach": "none",
                "note": "Empty filter configuration - all fields included"
            }
        }
    
    filtered_fields = []
    removed_by_standard = 0
    
    for field in ncpdp_data.get('fields', []):
        # Check standards filter
        if not filter_by_standards(field, allowed_standards):
            removed_by_standard += 1
            continue
        
        # Passed standards filter
        filtered_fields.append(field)
    
    # Build filtered NCPDP with metadata
    filtered_count = len(filtered_fields)
    
    return {
        "_columns": ncpdp_data.get('_columns', {}),
        "_standards": ncpdp_data.get('_standards', {}),
        "_filter_metadata": {
            "original_field_count": original_count,
            "filtered_field_count": filtered_count,
            "standards_included": allowed_standards,
            "domain": domain,
            "filter_date": datetime.now().isoformat(),
            "filter_approach": "standards_only",
            "filtering_stats": {
                "removed_by_standard": removed_by_standard,
                "kept": filtered_count,
                "reduction_percentage": round((1 - filtered_count / original_count) * 100, 1) if original_count > 0 else 0
            }
        },
        "fields": filtered_fields
    }


def run_step1d(
    config: AppConfig,
    outdir: Path,
    dry_run: bool = False
) -> Optional[dict]:
    """
    Step 1d: Filter NCPDP standards to domain-relevant fields
    
    Args:
        config: Configuration with CDM domain and NCPDP paths
        outdir: Output directory for rationalized NCPDP
        dry_run: If True, show what would be done without creating output
    
    Returns:
        Dict with rationalized NCPDP data for each standard type (None in dry run)
    """
    
    print(f"  📖 Loading NCPDP standards for filtering...")
    
    # Get filter configuration
    filter_config = None
    if hasattr(config.inputs, 'ncpdp_filter') and config.inputs.ncpdp_filter:
        filter_config = config.inputs.ncpdp_filter
        print(f"  📋 Filter configuration found:")
        if 'standards' in filter_config:
            print(f"     Standards: {filter_config['standards']}")
    else:
        print(f"  ⚠️  No filter configuration found - will include all fields")
    
    results = {}
    
    # Process general NCPDP standards
    if hasattr(config.inputs, 'ncpdp') and config.inputs.ncpdp and 'general' in config.inputs.ncpdp:
        general_file = Path(config.inputs.ncpdp['general'])
        if general_file.exists():
            print(f"  📖 Loading general NCPDP: {general_file.name}")
            
            with open(general_file, 'r', encoding='utf-8') as f:
                ncpdp_general = json.load(f)
            
            original_count = len(ncpdp_general.get('fields', []))
            print(f"     Original fields: {original_count:,}")
            
            if dry_run:
                # Dry run - just show what would happen
                if filter_config:
                    # Simulate filtering to show stats
                    filtered = rationalize_ncpdp(ncpdp_general, config.cdm.domain, filter_config)
                    filtered_count = filtered['_filter_metadata']['filtered_field_count']
                    reduction = filtered['_filter_metadata']['filtering_stats']['reduction_percentage']
                    print(f"     Would filter to: {filtered_count:,} fields ({reduction}% reduction)")
                else:
                    print(f"     Would keep all: {original_count:,} fields (no filter)")
            else:
                # Live mode - perform filtering
                filtered = rationalize_ncpdp(ncpdp_general, config.cdm.domain, filter_config)
                filtered_count = filtered['_filter_metadata']['filtered_field_count']
                
                # Save rationalized version
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                domain_safe = config.cdm.domain.replace(' ', '_')
                output_file = outdir / f"rationalized_ncpdp_general_{domain_safe}_{timestamp}.json"
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, indent=2)
                
                print(f"     ✓ Filtered to: {filtered_count:,} fields")
                print(f"     ✓ Saved: {output_file.name}")
                
                results['general'] = filtered
    
    # Process SCRIPT NCPDP standards
    if hasattr(config.inputs, 'ncpdp') and config.inputs.ncpdp and 'script' in config.inputs.ncpdp:
        script_file = Path(config.inputs.ncpdp['script'])
        if script_file.exists():
            print(f"  📖 Loading SCRIPT NCPDP: {script_file.name}")
            
            with open(script_file, 'r', encoding='utf-8') as f:
                ncpdp_script = json.load(f)
            
            original_count = len(ncpdp_script.get('fields', []))
            print(f"     Original fields: {original_count:,}")
            
            if dry_run:
                # Dry run - just show what would happen
                if filter_config:
                    filtered = rationalize_ncpdp(ncpdp_script, config.cdm.domain, filter_config)
                    filtered_count = filtered['_filter_metadata']['filtered_field_count']
                    reduction = filtered['_filter_metadata']['filtering_stats']['reduction_percentage']
                    print(f"     Would filter to: {filtered_count:,} fields ({reduction}% reduction)")
                else:
                    print(f"     Would keep all: {original_count:,} fields (no filter)")
            else:
                # Live mode - perform filtering
                filtered = rationalize_ncpdp(ncpdp_script, config.cdm.domain, filter_config)
                filtered_count = filtered['_filter_metadata']['filtered_field_count']
                
                # Save rationalized version
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                domain_safe = config.cdm.domain.replace(' ', '_')
                output_file = outdir / f"rationalized_ncpdp_script_{domain_safe}_{timestamp}.json"
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(filtered, f, indent=2)
                
                print(f"     ✓ Filtered to: {filtered_count:,} fields")
                print(f"     ✓ Saved: {output_file.name}")
                
                results['script'] = filtered
    
    if not results and not dry_run:
        print(f"  ⚠️  No NCPDP standards processed")
        return None
    
    if dry_run:
        print(f"  📝 DRY RUN - No files created")
        return None
    
    print(f"  ✓ Step 1d complete")
    return results