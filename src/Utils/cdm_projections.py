# src/utils/cdm_projections.py
"""
CDM Projection Utilities

Creates compact views of CDM for efficient LLM processing.
Removes noise (origin, source_mappings, etc.) while keeping semantic information.
"""
from typing import Dict, List, Any


def build_compact_catalog(enhanced_cdm: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build compact CDM attribute catalog for mapping tasks.
    
    Keeps only:
    - Entity name
    - Attribute name, data type
    - Glossary term, business context
    
    Drops:
    - origin, source_mappings (not needed for semantic matching)
    - size, precision, scale, classification
    - PK/FK metadata
    
    Args:
        enhanced_cdm: Full enhanced CDM with all metadata
    
    Returns:
        Compact catalog with just semantic information
    """
    catalog = {
        "domain": enhanced_cdm.get("cdm_metadata", {}).get("domain", "Unknown"),
        "entities": []
    }
    
    for entity in enhanced_cdm.get("entities", []):
        compact_entity = {
            "entity_name": entity.get("entity_name"),
            "business_definition": entity.get("business_definition", ""),
            "attributes": []
        }
        
        for attr in entity.get("attributes", []):
            # Coarsen data type for simpler matching
            data_type = attr.get("data_type", "").upper()
            if data_type in ["VARCHAR", "CHAR", "TEXT", "STRING"]:
                coarse_type = "string"
            elif data_type in ["INT", "INTEGER", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]:
                coarse_type = "number"
            elif data_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                coarse_type = "date"
            elif data_type in ["BOOLEAN", "BOOL"]:
                coarse_type = "boolean"
            else:
                coarse_type = "string"  # Default
            
            compact_attr = {
                "name": attr.get("canonical_column"),
                "data_type": coarse_type,
                "glossary": attr.get("glossary_term", "")[:300],  # Truncate if very long
                "business_context": attr.get("business_context", "")[:300]  # Truncate if very long
            }
            
            compact_entity["attributes"].append(compact_attr)
        
        catalog["entities"].append(compact_entity)
    
    return catalog


def merge_guardrails_mappings(
    enhanced_cdm: Dict[str, Any],
    entity_mappings: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Merge Guardrails mapping results back into full enhanced CDM.
    
    Args:
        enhanced_cdm: Full enhanced CDM to update
        entity_mappings: List of mapping results from per-entity LLM calls
            Each contains: {
                "guardrails_entity": "...",
                "mappings": [
                    {
                        "guardrails_attribute": "...",
                        "cdm_entity": "...",
                        "cdm_attribute": "...",
                        "disposition": "mapped|extension_attribute|unmapped",
                        ...
                    }
                ]
            }
    
    Returns:
        Updated enhanced CDM with Guardrails mappings applied
    """
    # Create lookup for fast access
    entity_lookup = {}
    for entity in enhanced_cdm.get("entities", []):
        entity_name = entity.get("entity_name")
        entity_lookup[entity_name] = entity
        
        # Create attribute lookup within entity
        attr_lookup = {}
        for attr in entity.get("attributes", []):
            attr_name = attr.get("canonical_column")
            attr_lookup[attr_name] = attr
        entity["_attr_lookup"] = attr_lookup
    
    # Apply mappings
    for entity_mapping in entity_mappings:
        for mapping in entity_mapping.get("mappings", []):
            disposition = mapping.get("disposition")
            
            if disposition == "mapped":
                # Add Guardrails mapping to existing attribute
                cdm_entity = mapping.get("cdm_entity")
                cdm_attribute = mapping.get("cdm_attribute")
                
                if cdm_entity in entity_lookup:
                    entity = entity_lookup[cdm_entity]
                    attr_lookup = entity.get("_attr_lookup", {})
                    
                    if cdm_attribute in attr_lookup:
                        attr = attr_lookup[cdm_attribute]
                        
                        # Initialize source_mappings if needed
                        if "source_mappings" not in attr:
                            attr["source_mappings"] = {}
                        
                        # Add Guardrails mapping
                        attr["source_mappings"]["guardrails"] = {
                            "disposition": "mapped",
                            "guardrails_entity": mapping.get("guardrails_entity"),
                            "guardrails_attribute": mapping.get("guardrails_attribute"),
                            "mapping_type": mapping.get("mapping_type", "direct"),
                            "added_in_step": "2c",
                            "api_source_files": mapping.get("api_source_files", [])
                        }
            
            elif disposition == "extension_attribute":
                # Add new attribute to existing entity
                cdm_entity = mapping.get("cdm_entity")
                
                if cdm_entity in entity_lookup:
                    entity = entity_lookup[cdm_entity]
                    
                    # Create new attribute
                    new_attr = {
                        "canonical_column": mapping.get("new_attribute_name"),
                        "source_column": mapping.get("new_attribute_name", "").upper(),
                        "data_type": mapping.get("data_type", "VARCHAR"),
                        "size": mapping.get("size"),
                        "nullable": mapping.get("nullable", True),
                        "glossary_term": mapping.get("glossary", ""),
                        "business_context": mapping.get("business_context", ""),
                        "classification": "Extension",
                        "origin": {
                            "standard": "guardrails",
                            "created_in_step": "2c",
                            "source_path": f"{mapping.get('guardrails_entity')}.{mapping.get('guardrails_attribute')}",
                            "source_file": mapping.get("api_source_files", [""])[0] if mapping.get("api_source_files") else "",
                            "justification": mapping.get("justification", "")
                        },
                        "source_mappings": {
                            "fhir": None,
                            "ncpdp": None,
                            "guardrails": {
                                "disposition": "extension_attribute",
                                "guardrails_entity": mapping.get("guardrails_entity"),
                                "guardrails_attribute": mapping.get("guardrails_attribute"),
                                "mapping_type": "direct",
                                "added_in_step": "2c",
                                "api_source_files": mapping.get("api_source_files", [])
                            },
                            "glue": None
                        }
                    }
                    
                    entity["attributes"].append(new_attr)
    
    # Clean up temporary lookups
    for entity in enhanced_cdm.get("entities", []):
        if "_attr_lookup" in entity:
            del entity["_attr_lookup"]
    
    return enhanced_cdm