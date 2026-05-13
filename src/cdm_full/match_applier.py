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
from typing import Dict, List, Optional, Tuple


def apply_match_files(
    full_cdm: Dict,
    match_files: Dict[str, Path],
    source_entities_lookup: Dict[str, Dict],
    source_mode_map: Optional[Dict[str, str]] = None,
) -> Tuple[Dict, Dict]:
    """
    Apply all match files to full CDM.
    Uses case-insensitive matching for entities and attributes.

    Args:
        full_cdm: Full CDM dict to update
        match_files: Dict of source_type -> match file path
        source_entities_lookup: Dict of source_type -> {entity_name: entity_dict}
        source_mode_map: Optional dict of source_type -> processing_mode.
            When provided, every unmapped_fields and requires_review_fields
            row is stamped with the source's mode so downstream consumers
            (rematch, Excel tabs, extension reviews) can filter without
            re-deriving mode from config.

    Returns:
        Tuple of (updated full_cdm, application_report)
    """
    mode_map = {k.lower(): v for k, v in (source_mode_map or {}).items()}

    def _stamp_mode(source_type: str) -> str:
        return mode_map.get((source_type or "").lower(), "refiner")
    
    print(f"\n   {'─'*50}")
    print(f"   Applying match files to Full CDM")
    print(f"   {'─'*50}")
    
    # Build case-insensitive lookup for CDM entities
    entity_lookup = {}
    for entity in full_cdm.get("entities", []):
        entity_name = entity.get("entity_name")
        if not entity_name:
            continue
        normalized = entity_name.lower()
        entity_lookup[normalized] = entity
        entity["_attr_lookup"] = {
            (a.get("attribute_name") or a.get("name") or "").lower(): a
            for a in entity.get("attributes", [])
            if a.get("attribute_name") or a.get("name")
        }
    
    application_report = {
        "sources_applied": [],
        "total_mapped": 0,
        "total_unmapped": 0,
        "total_requires_review": 0,
        "unmapped_fields": [],
        "requires_review_fields": [],
        "application_errors": [],
        "extension_candidates": [],   # add_entity proposals surfaced from anchored mode
        "additions_applied": [],      # add_attribute proposals applied to the CDM
    }

    # Anchored mode means the user provided the foundational CDM.  Only in
    # anchored mode are refiner additions actually applied to the CDM.  In
    # synthesized mode, Step 2's foundational build already incorporated
    # refiner data, so match-time additions would duplicate that work and
    # are silently dropped.
    anchored = bool(full_cdm.get("anchored"))

    for source_type, match_file_path in match_files.items():
        print(f"   Applying: {source_type.upper()} ({match_file_path.name})")

        with open(match_file_path, 'r', encoding='utf-8') as f:
            match_data = json.load(f)

        # Get source entities for this source type
        source_entities = source_entities_lookup.get(source_type, {})

        # Update source_files in CDM
        full_cdm["source_files"][source_type] = match_data.get("source_file")

        # ── Apply additions FIRST, before lineage mapping ────────────
        # Match-time additions (anchored-mode gap-fills) must land in
        # the CDM before attribute_mappings reference them — otherwise
        # the mappings will fail attribute-lookup and get logged as
        # application_errors.
        source_mode = _stamp_mode(source_type)
        for addition in match_data.get("additions", []) or []:
            action = (addition.get("action") or "").lower()
            target_entity = addition.get("target_entity") or ""
            target_normalized = target_entity.lower()

            if action == "add_entity":
                # add_entity is never applied automatically.  Anchored mode
                # surfaces the proposal for human review; synthesized mode
                # drops it (Step 2 owned structural decisions).
                if anchored:
                    application_report["extension_candidates"].append({
                        "source_type": source_type,
                        "processing_mode": source_mode,
                        **addition,
                    })
                continue

            if action != "add_attribute":
                continue
            if not anchored:
                continue
            if source_mode != "refiner":
                # mapper-mode sources can never add to the CDM; foundational
                # sources don't run match.
                continue
            if target_normalized not in entity_lookup:
                # The LLM proposed an attribute on an entity that doesn't
                # exist.  Treat as an extension candidate so it surfaces
                # for review rather than silently inflating the gap report.
                application_report["extension_candidates"].append({
                    "source_type": source_type,
                    "processing_mode": source_mode,
                    "rejected_reason": f"target_entity {target_entity!r} not found",
                    **addition,
                })
                continue

            cdm_entity = entity_lookup[target_normalized]
            attr_name = addition.get("attribute_name")
            if not attr_name:
                continue
            if attr_name.lower() in cdm_entity["_attr_lookup"]:
                # Already exists — nothing to add.
                continue

            new_attr = {
                "attribute_name": attr_name,
                "data_type": addition.get("data_type") or "VARCHAR(255)",
                "required": bool(addition.get("required", False)),
                "pk": False,
                "description": addition.get("description") or "",
                "source_lineage": {st: [] for st in match_files.keys()},
                "validation_rules": [],
                "business_rules": [],
                "_added_by": {"source_type": source_type, "reasoning": addition.get("reasoning", "")},
            }
            cdm_entity.setdefault("attributes", []).append(new_attr)
            cdm_entity["_attr_lookup"][attr_name.lower()] = new_attr
            application_report["additions_applied"].append({
                "source_type": source_type,
                "processing_mode": source_mode,
                "target_entity": target_entity,
                "attribute_name": attr_name,
            })
        
        source_mapped = 0
        source_unmapped = 0
        source_requires_review = 0
        
        for mapping_result in match_data.get("entity_mappings", []):
            source_entity_name = mapping_result.get("source_entity")
            source_entity = source_entities.get(source_entity_name, {})
            # `or ""` guards against rationalized files where attribute_name
            # is explicitly null — without it, .lower() crashes the same way
            # maps_to_cdm_entity did.
            source_attrs = {
                (a.get("attribute_name") or "").lower(): a
                for a in source_entity.get("attributes", [])
            }
            
            entity_eval = mapping_result.get("entity_evaluation", {}) or {}
            # `or ""` guards against null in JSON (which dict.get's default
            # does NOT cover — default only fires when the key is absent).
            cdm_entity_name = entity_eval.get("maps_to_cdm_entity") or ""
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
                # `or ""` to coerce explicit null values in match files
                # (default in .get() only fires for missing keys).
                source_attr_name = attr_mapping.get("source_attribute") or ""
                
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
                            "processing_mode": _stamp_mode(source_type),
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
                        "processing_mode": _stamp_mode(source_type),
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