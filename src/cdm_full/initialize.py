# src/cdm_full/initialize.py
"""
Full CDM initialization from Foundational CDM.

Functions:
  - find_latest_foundational_cdm(): Find latest CDM file from refinement pipeline
  - initialize_full_cdm(): Transform foundational to full CDM structure
"""
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def find_latest_foundational_cdm(cdm_dir: Path, domain: str) -> Optional[Path]:
    """
    Find latest foundational/refined CDM file.
    
    Naming: cdm_{domain}_{module}_{YYYYMMDD}_{HHMMSS}.json
    Excludes: disposition, recommendations, approved, findings, gaps, full
    
    Args:
        cdm_dir: Path to CDM output directory (e.g., output/plan/cdm)
        domain: CDM domain name
    
    Returns:
        Path to latest CDM file, or None if not found
    """
    domain_safe = domain.lower().replace(' ', '_')
    pattern = f"cdm_{domain_safe}_*.json"
    matches = list(cdm_dir.glob(pattern))
    
    # Filter out reports and full CDM outputs
    exclude_patterns = ['disposition', 'recommendations', 'approved', 'findings', 'gaps', 'full']
    cdm_files = [f for f in matches 
                 if not any(p in f.name for p in exclude_patterns)]
    
    if not cdm_files:
        return None
    
    def extract_timestamp(filepath: Path) -> str:
        parts = filepath.stem.split('_')
        if len(parts) >= 2:
            return f"{parts[-2]}_{parts[-1]}"
        return "0"
    
    cdm_files.sort(key=extract_timestamp, reverse=True)
    return cdm_files[0]


def initialize_full_cdm(
    foundational_cdm: Dict, 
    source_types: List[str], 
    domain_description: str
) -> Dict:
    """
    Transform Foundational CDM structure to Full CDM structure.
    Adds source_lineage scaffolding to each entity and attribute.
    
    Args:
        foundational_cdm: The foundational CDM dict
        source_types: List of source types to create scaffolding for
        domain_description: Description of the CDM domain
    
    Returns:
        Full CDM dict with source_lineage scaffolding
    """
    
    def make_source_lineage():
        return {st: [] for st in source_types}
    
    full_cdm = {
        "domain": foundational_cdm.get("domain"),
        "domain_description": domain_description,
        "cdm_version": foundational_cdm.get("cdm_version", "1.0"),
        "generated_date": datetime.now().isoformat(),
        "source_files": {st: None for st in source_types},
        "entities": []
    }
    
    for entity in foundational_cdm.get("entities", []):
        full_entity = {
            "entity_name": entity.get("entity_name"),
            "entity_name_normalized": entity.get("entity_name", "").lower(),
            "description": entity.get("description"),
            "classification": entity.get("classification"),
            "source_lineage": make_source_lineage(),
            "attributes": [],
            "relationships": entity.get("relationships", [])
        }
        
        for attr in entity.get("attributes", []):
            source_lineage = make_source_lineage()
            
            full_attr = {
                "attribute_name": attr.get("name"),
                "attribute_name_normalized": attr.get("name", "").lower(),
                "data_type": _extract_base_type(attr.get("type", "VARCHAR")),
                "max_length": _extract_length(attr.get("type")),
                "precision": None,
                "scale": None,
                "cardinality": "1..1" if attr.get("required") else "0..1",
                "required": attr.get("required", False),
                "nullable": not attr.get("required", False),
                "pk": attr.get("pk", False),
                "description": attr.get("description"),
                "business_rules": [],
                "validation_rules": [],
                "possible_values": None,
                "example_values": [],
                "default_value": None,
                "classification": "Operational",
                "is_pii": False,
                "is_phi": False,
                "source_lineage": source_lineage
            }
            full_entity["attributes"].append(full_attr)
        
        full_cdm["entities"].append(full_entity)
    
    return full_cdm


def _extract_base_type(type_str: str) -> str:
    """Extract base type from VARCHAR(50) -> VARCHAR"""
    if '(' in type_str:
        return type_str.split('(')[0]
    return type_str


def _extract_length(type_str: str) -> Optional[int]:
    """Extract length from VARCHAR(50) -> 50"""
    if '(' in type_str and ')' in type_str:
        try:
            return int(type_str.split('(')[1].split(')')[0].split(',')[0])
        except:
            pass
    return None
