# src/cdm_full/match_applier.py
"""
Match file application for Full CDM.

Applies match files to Full CDM with case-insensitive entity/attribute matching.

Work Item 3: Added binding passthrough to source_lineage for terminology enrichment.

Functions:
  - apply_match_files(): Merge all match files into Full CDM
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Tuple


def apply_match_files(
    full_cdm: Dict,
    match_files: Dict[str, Path],
    source_entities_lookup: Dict[str, Dict]
) -> Tuple[Dict, Dict]:
    """
    Apply all match files to full CDM.
    Uses case-insensitive matching for entities and attributes.
    
    Args:
        full_cdm: Full CDM dict to update
        match_files: Dict of source_type -> match file path
        source_entities_lookup: Dict of source_type -> {entity_name: entity_dict}
    
    Returns:
        Tuple of (updated full_cdm, application_report)
    """
    
    print(f"\n   {'─'*50}")
    print(f"   Applying match files to Full CDM")
    print(f"   {'─'*50}")
    
    # Build case-insensitive lookup for CDM entities
    entity_lookup = {}
    for entity in full_cdm.get("entities", []):
        entity_name = entity.get("entity_name")
        normalized = entity_name.lower()
        entity_lookup[normalized] = entity
        entity["_attr_lookup"] = {
            a.get("attribute_name").lower(): a 
            for a in entity.get("attributes", [])
        }
    
    application_report = {
        "sources_applied": [],
        "total_mapped": 0,
        "total_unmapped": 0,
        "total_requires_review": 0,
        "unmapped_fields": [],
        "requires_review_fields": [],
        "application_errors": []
    }
    
    for source_type, match_file_path in match_files.items():
        print(f"   Applying: {source_type.upper()} ({match_file_path.name})")
        
        with open(match_file_path, 'r', encoding='utf-8') as f:
            match_data = json.load(f)
        
        # Get source entities for this source type
        source_entities = source_entities_lookup.get(source_type, {})
        
        # Update source_files in CDM
        full_cdm["source_files"][source_type] = match_data.get("source_file")
        
        source_mapped = 0
        source_unmapped = 0
        source_requires_review = 0
        
        for mapping_result in match_data.get("entity_mappings", []):
            source_entity_name = mapping_result.get("source_entity")
            source_entity = source_entities.get(source_entity_name, {})
            source_attrs = {
                a.get("attribute_name", "").lower(): a 
                for a in source_entity.get("attributes", [])
            }
            
            entity_eval = mapping_result.get("entity_evaluation", {})
            cdm_entity_name = entity_eval.get("maps_to_cdm_entity", "")
            cdm_entity_normalized = cdm_entity_name.lower()
            
            # Update entity source_lineage (case-insensitive)
            if cdm_entity_normalized in entity_lookup:
                cdm_entity = entity_lookup[cdm_entity_normalized]
                
                source_info = source_entity.get("source_info", {})
                lineage_entry = {
                    "source_entity": source_entity_name,
                    "source_files": source_info.get("files", []),
                    "api": source_info.get("api"),
                    "schema": source_info.get("schema"),
                    "table": source_info.get("table")
                }
                cdm_entity["source_lineage"][source_type].append(lineage_entry)
            
            # Apply attribute mappings
            for attr_mapping in mapping_result.get("attribute_mappings", []):
                disposition = attr_mapping.get("disposition")
                source_attr_name = attr_mapping.get("source_attribute", "")
                
                if disposition == "mapped":
                    cdm_ent_name = attr_mapping.get("cdm_entity") or ""
                    cdm_attr_name = attr_mapping.get("cdm_attribute") or ""
                    
                    # Skip if missing required fields
                    if not cdm_ent_name or not cdm_attr_name:
                        application_report["application_errors"].append({
                            "source_type": source_type,
                            "source_entity": source_entity_name,
                            "source_attribute": source_attr_name,
                            "error": f"Mapped entry missing cdm_entity or cdm_attribute: entity={cdm_ent_name}, attr={cdm_attr_name}"
                        })
                        source_unmapped += 1
                        continue
                    
                    cdm_ent_normalized = cdm_ent_name.lower()
                    cdm_attr_normalized = cdm_attr_name.lower()
                    
                    if cdm_ent_normalized not in entity_lookup:
                        application_report["application_errors"].append({
                            "source_type": source_type,
                            "source_entity": source_entity_name,
                            "source_attribute": source_attr_name,
                            "error": f"CDM entity not found: {cdm_ent_name}"
                        })
                        continue
                    
                    cdm_entity = entity_lookup[cdm_ent_normalized]
                    cdm_attr = cdm_entity["_attr_lookup"].get(cdm_attr_normalized)
                    
                    if not cdm_attr:
                        application_report["application_errors"].append({
                            "source_type": source_type,
                            "source_entity": source_entity_name,
                            "source_attribute": source_attr_name,
                            "error": f"CDM attribute not found: {cdm_ent_name}.{cdm_attr_name}"
                        })
                        continue
                    
                    # Get source attribute details
                    source_attr = source_attrs.get(source_attr_name.lower(), {})
                    
                    # Work Item 3: Extract binding from source_metadata for terminology enrichment
                    source_metadata = source_attr.get("source_metadata", {})
                    binding = source_metadata.get("binding")
                    
                    # Add to attribute source_lineage
                    attr_lineage = {
                        "source_entity": source_entity_name,
                        "source_attribute": source_attr_name,
                        "source_files": source_attr.get("source_files_element", []),
                        "mapping_type": attr_mapping.get("mapping_type", "direct"),
                        "confidence": attr_mapping.get("confidence", "medium"),
                        "data_type": source_attr.get("data_type"),
                        "required": source_attr.get("required"),
                        "description": source_attr.get("description")
                    }
                    
                    # Work Item 3: Add binding if present (for post-process terminology enrichment)
                    if binding:
                        attr_lineage["binding"] = binding
                    
                    cdm_attr["source_lineage"][source_type].append(attr_lineage)
                    
                    # Merge validation rules
                    for rule in attr_mapping.get("validation_rules_extracted", []):
                        existing = [r for r in cdm_attr["validation_rules"] if r.get("rule") == rule]
                        if existing:
                            if source_type not in existing[0].get("sources", []):
                                existing[0]["sources"].append(source_type)
                        else:
                            cdm_attr["validation_rules"].append({
                                "rule": rule,
                                "sources": [source_type]
                            })
                    
                    # Merge business rules
                    for rule in attr_mapping.get("business_rules_extracted", []):
                        existing = [r for r in cdm_attr["business_rules"] if r.get("rule") == rule]
                        if existing:
                            if source_type not in existing[0].get("sources", []):
                                existing[0]["sources"].append(source_type)
                        else:
                            cdm_attr["business_rules"].append({
                                "rule": rule,
                                "sources": [source_type]
                            })
                    
                    source_mapped += 1
                    
                    # Track requires_review items
                    if attr_mapping.get("requires_review", False):
                        source_requires_review += 1
                        application_report["requires_review_fields"].append({
                            "source_type": source_type,
                            "source_entity": source_entity_name,
                            "source_attribute": source_attr_name,
                            "cdm_entity": cdm_ent_name,
                            "cdm_attribute": cdm_attr_name,
                            "mapping_type": attr_mapping.get("mapping_type"),
                            "confidence": attr_mapping.get("confidence"),
                            "review_reason": attr_mapping.get("review_reason", "Low confidence mapping")
                        })
                
                else:  # unmapped
                    source_unmapped += 1
                    application_report["unmapped_fields"].append({
                        "source_type": source_type,
                        "source_entity": source_entity_name,
                        "source_attribute": source_attr_name,
                        "reason": attr_mapping.get("reason", ""),
                        "suggested_cdm_entity": attr_mapping.get("suggested_cdm_entity"),
                        "suggested_attribute_name": attr_mapping.get("suggested_attribute_name")
                    })
        
        application_report["sources_applied"].append({
            "source_type": source_type,
            "mapped": source_mapped,
            "unmapped": source_unmapped,
            "requires_review": source_requires_review
        })
        application_report["total_mapped"] += source_mapped
        application_report["total_unmapped"] += source_unmapped
        application_report["total_requires_review"] += source_requires_review
        
        print(f"     Mapped: {source_mapped}, Unmapped: {source_unmapped}, Requires Review: {source_requires_review}")
    
    # Cleanup lookup helpers
    for entity in full_cdm.get("entities", []):
        if "_attr_lookup" in entity:
            del entity["_attr_lookup"]
    
    return full_cdm, application_report