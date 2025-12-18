# src/cdm_full/postprocess_terminology.py
"""
Post-processing: Terminology Enrichment

Enriches CDM attributes with ValueSet/CodeSystem terminology data by:
1. Walking attributes with binding URLs in source_lineage
2. Matching binding URLs to VS/CS files via config canonical_url lookup
3. Loading raw FHIR VS/CS files on-demand
4. Adding terminology metadata to attributes

Run after Full CDM is built in Step 6 (alongside sensitivity and CDE analysis).

This approach:
- Avoids processing VS/CS during rationalization (no AI needed for VS/CS)
- Loads raw FHIR files preserving all data (compose, expansion, nested concepts)
- Links terminology to attributes via standard FHIR binding mechanism
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set

from src.config.config_parser import AppConfig


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_terminology_lookup(config_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Build lookup from canonical URL to file path and metadata.
    
    Args:
        config_path: Path to CDM config JSON file
        
    Returns:
        Dict mapping canonical_url -> {file_path, file_type, resource_name, ig_source}
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    lookup = {}
    
    for ig in config.get('input_files', {}).get('fhir_igs', []):
        file_type = ig.get('file_type', '')
        canonical_url = ig.get('canonical_url')
        
        if file_type in ('ValueSet', 'CodeSystem') and canonical_url:
            lookup[canonical_url] = {
                "file_path": ig.get('file', ''),
                "file_type": file_type,
                "resource_name": ig.get('resource_name', ''),
                "ig_source": ig.get('ig_source', '')
            }
    
    return lookup


def load_valueset(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load and parse a FHIR ValueSet file.
    
    Args:
        file_path: Path to ValueSet JSON file
        
    Returns:
        Parsed terminology data, or None if failed
    """
    try:
        path = Path(file_path)
        if not path.exists():
            return None
        
        with open(path, 'r', encoding='utf-8') as f:
            vs_data = json.load(f)
        
        return {
            "type": "ValueSet",
            "url": vs_data.get('url', ''),
            "name": vs_data.get('name', ''),
            "title": vs_data.get('title', ''),
            "status": vs_data.get('status', ''),
            "description": vs_data.get('description', ''),
            "compose": vs_data.get('compose'),
            "expansion": vs_data.get('expansion')
        }
    except Exception as e:
        print(f"      Warning: Failed to load ValueSet {file_path}: {e}")
        return None


def load_codesystem(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load and parse a FHIR CodeSystem file.
    
    Args:
        file_path: Path to CodeSystem JSON file
        
    Returns:
        Parsed terminology data, or None if failed
    """
    try:
        path = Path(file_path)
        if not path.exists():
            return None
        
        with open(path, 'r', encoding='utf-8') as f:
            cs_data = json.load(f)
        
        # Extract concepts (may be nested)
        concepts = cs_data.get('concept', [])
        
        return {
            "type": "CodeSystem",
            "url": cs_data.get('url', ''),
            "name": cs_data.get('name', ''),
            "title": cs_data.get('title', ''),
            "status": cs_data.get('status', ''),
            "description": cs_data.get('description', ''),
            "content": cs_data.get('content', ''),
            "concept_count": len(concepts),
            "concepts": concepts  # Full concept hierarchy
        }
    except Exception as e:
        print(f"      Warning: Failed to load CodeSystem {file_path}: {e}")
        return None


def extract_binding_urls(cdm: Dict[str, Any]) -> Set[str]:
    """
    Extract all unique binding URLs from CDM attributes.
    
    Args:
        cdm: Full CDM dictionary
        
    Returns:
        Set of unique binding URLs
    """
    urls = set()
    
    for entity in cdm.get("entities", []):
        for attr in entity.get("attributes", []):
            # Check source_lineage for bindings
            for source_type, lineage_list in attr.get("source_lineage", {}).items():
                if not isinstance(lineage_list, list):
                    continue
                for lineage in lineage_list:
                    binding = lineage.get("binding", {})
                    if isinstance(binding, dict):
                        value_set = binding.get("value_set")
                        if value_set:
                            urls.add(value_set)
    
    return urls


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def enrich_terminology_bindings(
    cdm: Dict[str, Any],
    config_path: str,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Enrich CDM attributes with terminology data from VS/CS files.
    
    Args:
        cdm: Full CDM dictionary (will be modified)
        config_path: Path to CDM config JSON file
        verbose: If True, print progress details
        
    Returns:
        Updated CDM with terminology field on attributes
    """
    
    # Build lookup from canonical URL to file info
    terminology_lookup = build_terminology_lookup(config_path)
    
    if not terminology_lookup:
        if verbose:
            print(f"      No VS/CS entries with canonical_url in config - skipping")
        return cdm
    
    if verbose:
        print(f"      Terminology lookup: {len(terminology_lookup)} VS/CS entries")
    
    # Extract binding URLs from CDM
    binding_urls = extract_binding_urls(cdm)
    
    if not binding_urls:
        if verbose:
            print(f"      No binding URLs found in CDM attributes - skipping")
        return cdm
    
    if verbose:
        print(f"      Found {len(binding_urls)} unique binding URLs in CDM")
    
    # Track stats
    stats = {
        "binding_urls_found": len(binding_urls),
        "urls_matched": 0,
        "urls_unmatched": 0,
        "attributes_enriched": 0,
        "files_loaded": 0
    }
    
    # Cache loaded terminology to avoid reloading
    terminology_cache: Dict[str, Dict[str, Any]] = {}
    
    # Walk CDM and enrich attributes
    for entity in cdm.get("entities", []):
        entity_name = entity.get("entity_name", "")
        
        for attr in entity.get("attributes", []):
            attr_name = attr.get("attribute_name", "")
            attr_enriched = False
            
            # Check all source lineage entries for bindings
            for source_type, lineage_list in attr.get("source_lineage", {}).items():
                if not isinstance(lineage_list, list):
                    continue
                    
                for lineage in lineage_list:
                    binding = lineage.get("binding", {})
                    if not isinstance(binding, dict):
                        continue
                    
                    binding_url = binding.get("value_set")
                    if not binding_url:
                        continue
                    
                    # Check if we have this URL in our lookup
                    if binding_url not in terminology_lookup:
                        stats["urls_unmatched"] += 1
                        continue
                    
                    stats["urls_matched"] += 1
                    
                    # Load terminology if not cached
                    if binding_url not in terminology_cache:
                        term_info = terminology_lookup[binding_url]
                        file_path = term_info["file_path"]
                        file_type = term_info["file_type"]
                        
                        if file_type == "ValueSet":
                            term_data = load_valueset(file_path)
                        elif file_type == "CodeSystem":
                            term_data = load_codesystem(file_path)
                        else:
                            term_data = None
                        
                        if term_data:
                            terminology_cache[binding_url] = term_data
                            stats["files_loaded"] += 1
                    
                    # Add terminology to attribute
                    if binding_url in terminology_cache:
                        term_data = terminology_cache[binding_url]
                        
                        # Initialize terminology list if needed
                        if "terminology" not in attr:
                            attr["terminology"] = []
                        
                        # Add terminology entry
                        term_entry = {
                            "binding_url": binding_url,
                            "binding_strength": binding.get("strength", ""),
                            "source": source_type,
                            **term_data
                        }
                        
                        # Avoid duplicates
                        existing_urls = [t.get("binding_url") for t in attr["terminology"]]
                        if binding_url not in existing_urls:
                            attr["terminology"].append(term_entry)
                            attr_enriched = True
            
            if attr_enriched:
                stats["attributes_enriched"] += 1
    
    # Add enrichment metadata to CDM
    cdm["terminology_enrichment"] = {
        "processed_date": datetime.now().isoformat(),
        "stats": stats
    }
    
    return cdm


def run_terminology_postprocess(
    cdm: Dict[str, Any],
    config_path: str,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Run terminology enrichment post-processing.
    
    Args:
        cdm: Full CDM dictionary (will be modified)
        config_path: Path to CDM config JSON file
        dry_run: If True, show info but do not process
    
    Returns:
        Updated CDM with terminology enrichment
    """
    
    print(f"\n   POST-PROCESSING: Terminology Enrichment")
    print(f"   {'-'*40}")
    
    if dry_run:
        print(f"   (Dry run - skipping terminology enrichment)")
        return cdm
    
    if not config_path:
        print(f"   ⚠️  No config_path provided - skipping terminology enrichment")
        return cdm
    
    # Run enrichment
    cdm = enrich_terminology_bindings(cdm, config_path, verbose=True)
    
    # Print summary
    stats = cdm.get("terminology_enrichment", {}).get("stats", {})
    
    print(f"\n   Terminology enrichment complete:")
    print(f"      Binding URLs in CDM: {stats.get('binding_urls_found', 0)}")
    print(f"      URLs matched to VS/CS: {stats.get('urls_matched', 0)}")
    print(f"      URLs unmatched: {stats.get('urls_unmatched', 0)}")
    print(f"      VS/CS files loaded: {stats.get('files_loaded', 0)}")
    print(f"      Attributes enriched: {stats.get('attributes_enriched', 0)}")
    
    return cdm


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python postprocess_terminology.py <full_cdm.json> <config.json>")
        print("  Enriches CDM attributes with ValueSet/CodeSystem terminology data")
        sys.exit(1)
    
    cdm_path = sys.argv[1]
    config_path = sys.argv[2]
    
    print(f"Loading CDM: {cdm_path}")
    with open(cdm_path, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    print(f"Config: {config_path}")
    
    cdm = run_terminology_postprocess(cdm, config_path)
    
    # Save enriched CDM
    output_path = cdm_path.replace('.json', '_terminology.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cdm, f, indent=2)
    
    print(f"\n✓ Saved enriched CDM: {output_path}")