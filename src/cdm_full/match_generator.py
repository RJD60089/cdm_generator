# src/cdm_full/match_generator.py
"""
Match file generation for Full CDM.

Handles AI-driven mapping of source attributes to CDM attributes.

Functions:
  - build_compact_catalog(): Create token-efficient CDM representation
  - build_source_entity_prompt(): Build AI prompt for entity mapping
  - generate_match_file(): Generate match file for a source
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_compact_catalog(full_cdm: Dict) -> Dict:
    """
    Build compact CDM catalog for AI context (minimizes tokens).
    
    Args:
        full_cdm: Full CDM dict
    
    Returns:
        Compact catalog with essential info for matching
    """
    
    catalog = {
        "domain": full_cdm.get("domain"),
        "entities": []
    }
    
    for entity in full_cdm.get("entities", []):
        compact_entity = {
            "entity_name": entity.get("entity_name"),
            "description": (entity.get("description") or "")[:200],
            "classification": entity.get("classification"),
            "attributes": []
        }
        
        for attr in entity.get("attributes", []):
            data_type = (attr.get("data_type") or "").upper()
            if data_type in ["VARCHAR", "CHAR", "TEXT", "STRING"]:
                coarse_type = "string"
            elif data_type in ["INT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]:
                coarse_type = "number"
            elif data_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                coarse_type = "date"
            elif data_type in ["BOOLEAN", "BOOL"]:
                coarse_type = "boolean"
            else:
                coarse_type = "string"
            
            compact_attr = {
                "name": attr.get("attribute_name"),
                "type": coarse_type,
                "pk": attr.get("pk", False),
                "desc": (attr.get("description") or "")[:150]
            }
            compact_entity["attributes"].append(compact_attr)
        
        catalog["entities"].append(compact_entity)
    
    return catalog


def build_source_entity_prompt(
    config: AppConfig,
    source_type: str,
    compact_catalog: Dict,
    source_entity: Dict,
    domain_description: str
) -> str:
    """
    Build prompt to map a single source entity to CDM.
    
    Args:
        config: App configuration
        source_type: Source type (e.g., "guardrails")
        compact_catalog: Compact CDM catalog
        source_entity: Source entity to map
        domain_description: Domain context description
    
    Returns:
        Prompt string for AI
    """
    
    entity_name = source_entity.get("entity_name")
    attributes = source_entity.get("attributes", [])
    
    prompt = f"""Map {source_type.upper()} entity attributes to the CDM. Every source attribute MUST be accounted for.

DOMAIN: {config.cdm.domain}
DOMAIN CONTEXT: {domain_description}

SOURCE TYPE: {source_type.upper()}
SOURCE ENTITY: {entity_name}
Description: {source_entity.get("description", "N/A")}
Business Context: {source_entity.get("business_context", "N/A")}
Attributes to map: {len(attributes)}

TASK:
1. For each source attribute, find the best matching CDM entity.attribute
2. Extract validation_rules and business_rules from source metadata
3. High Quality mapping is REQUIRED - review EACH AND EVERY ATTRIBUTE in SOURCE {entity_name} for a proper match in CDM, use all available information to make best match.
4. There should be few unmapped attributes. If one occurs, mark as gap (potential CDM addition needed).
5. If confidence is low for an attribute mapping, set requires_review=true and include review_reason.

CRITICAL: Every source attribute MUST appear in attribute_mappings with disposition "mapped" or "unmapped".

CDM CATALOG:
{json.dumps(compact_catalog, indent=2)}

SOURCE ATTRIBUTES:
{json.dumps(attributes, indent=2)}

OUTPUT (JSON only, no markdown):
{{
  "source_type": "{source_type}",
  "source_entity": "{entity_name}",
  "entity_evaluation": {{
    "maps_to_cdm_entity": "Carrier",
    "confidence": "high",
    "reasoning": "..."
  }},
  "attribute_mappings": [
    {{
      "source_attribute": "carrier_code",
      "disposition": "mapped",
      "cdm_entity": "Carrier",
      "cdm_attribute": "carrier_code",
      "mapping_type": "direct",
      "confidence": "high",
      "requires_review": false,
      "validation_rules_extracted": ["Required", "Max length 10"],
      "business_rules_extracted": ["Must be unique within organization"]
    }},
    {{
      "source_attribute": "effective_date",
      "disposition": "mapped",
      "cdm_entity": "Carrier",
      "cdm_attribute": "effective_start_date",
      "mapping_type": "semantic_alias",
      "confidence": "low",
      "requires_review": true,
      "review_reason": "Semantic match uncertain - source is 'effective_date', CDM has 'effective_start_date' and 'effective_end_date'",
      "validation_rules_extracted": [],
      "business_rules_extracted": []
    }},
    {{
      "source_attribute": "unknown_field",
      "disposition": "unmapped",
      "reason": "No semantic match in CDM - potential gap",
      "suggested_cdm_entity": "Carrier",
      "suggested_attribute_name": "unknown_field",
      "validation_rules_extracted": [],
      "business_rules_extracted": []
    }}
  ],
  "summary": {{
    "total_attributes": {len(attributes)},
    "mapped": 0,
    "unmapped": 0,
    "requires_review": 0
  }}
}}

MAPPING TYPES:
- direct: Exact semantic match
- semantic_alias: Same concept, different name
- transformed: Requires data transformation
- conditional: Maps under certain conditions

CONFIDENCE LEVELS:
- high: Certain match based on name, type, and description
- medium: Reasonable match but some ambiguity
- low: Uncertain match - requires SME review

RULES:
- Match on semantic meaning, not just name similarity
- Use case-insensitive matching for entity/attribute names
- Extract validation_rules from source (Required, Max length, Format, etc.)
- Extract business_rules from source (Must be unique, Derived from X, etc.)
- Low confidence mappings: set requires_review=true with review_reason
- Unmapped = CDM gap requiring review
- Output ONLY valid JSON
"""
    return prompt


def generate_match_file(
    config: AppConfig,
    source_type: str,
    rationalized_file: Path,
    full_cdm: Dict,
    llm: LLMClient,
    full_cdm_dir: Path,
    domain_description: str,
    dry_run: bool = False
) -> Optional[Path]:
    """
    Generate match file for a single source.
    
    Args:
        config: App configuration
        source_type: Source type (e.g., "guardrails")
        rationalized_file: Path to rationalized source file
        full_cdm: Full CDM dict
        llm: LLM client
        full_cdm_dir: Output directory for match files
        domain_description: Domain context description
        dry_run: If True, save prompts only
    
    Returns:
        Path to match file (None if dry_run)
    """
    
    print(f"\n   {'─'*50}")
    print(f"   Generating match file: {source_type.upper()}")
    print(f"   {'─'*50}")
    
    # Load rationalized source
    with open(rationalized_file, 'r', encoding='utf-8') as f:
        rationalized = json.load(f)
    
    source_entities = rationalized.get("entities", [])
    total_attrs = sum(len(e.get("attributes", [])) for e in source_entities)
    print(f"   Source: {rationalized_file.name}")
    print(f"   Entities: {len(source_entities)}, Attributes: {total_attrs}")
    
    # Build compact catalog
    compact_catalog = build_compact_catalog(full_cdm)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if dry_run:
        # Save example prompt
        if source_entities:
            example_prompt = build_source_entity_prompt(
                config, source_type, compact_catalog, source_entities[0], domain_description
            )
            prompts_dir = full_cdm_dir / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = prompts_dir / f"match_{source_type}_example_{timestamp}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(example_prompt)
            print(f"   ✓ Example prompt saved: {output_file.name}")
        return None
    
    # Process each source entity
    entity_mappings = []
    ai_failures = []
    
    for idx, source_entity in enumerate(source_entities, 1):
        entity_name = source_entity.get("entity_name")
        attr_count = len(source_entity.get("attributes", []))
        print(f"   [{idx}/{len(source_entities)}] {entity_name} ({attr_count} attrs)...", end=" ")
        
        prompt = build_source_entity_prompt(
            config, source_type, compact_catalog, source_entity, domain_description
        )
        
        messages = [
            {"role": "system", "content": "You are an expert healthcare data engineer and data analyst experienced mapping source to target data. Return ONLY valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response, _ = llm.chat(messages)
            
            # Parse response
            response_clean = response.strip()
            if response_clean.startswith("```"):
                lines = response_clean.split("\n")
                response_clean = "\n".join(lines[1:-1])
            
            result = json.loads(response_clean)
            entity_mappings.append(result)
            
            summary = result.get("summary", {})
            print(f"mapped: {summary.get('mapped', 0)}, unmapped: {summary.get('unmapped', 0)}, review: {summary.get('requires_review', 0)}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"FAILED: {error_msg}")
            ai_failures.append({
                "source_entity": entity_name,
                "attribute_count": attr_count,
                "error": error_msg,
                "timestamp": datetime.now().isoformat()
            })
    
    # Build match file
    match_file_data = {
        "source_type": source_type,
        "source_file": rationalized_file.name,
        "generated_timestamp": datetime.now().isoformat(),
        "source_entity_count": len(source_entities),
        "source_attribute_count": total_attrs,
        "ai_failures": ai_failures,
        "entity_mappings": entity_mappings
    }
    
    # Save match file
    match_file_path = full_cdm_dir / f"match_{source_type}_{timestamp}.json"
    with open(match_file_path, 'w', encoding='utf-8') as f:
        json.dump(match_file_data, f, indent=2)
    
    success_count = len(entity_mappings)
    fail_count = len(ai_failures)
    print(f"   ✓ Match file saved: {match_file_path.name}")
    print(f"     Processed: {success_count} success, {fail_count} failures")
    
    if ai_failures:
        print(f"   ⚠️  AI failures logged - review match file for details")
    
    return match_file_path