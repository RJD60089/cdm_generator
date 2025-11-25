# src/steps/step2c_guardrails_refinement.py
"""
Step 2c: Guardrails Refinement & Gap Analysis (Per-Entity Processing)

Enhances CDM from Step 2b by mapping Guardrails fields.
Uses compact catalog + per-entity processing to avoid timeouts.

Input: Enhanced CDM from Step 2b + Rationalized Guardrails JSON
Output: Enhanced CDM with Guardrails mappings + unmapped fields + disposition report
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_compact_catalog(enhanced_cdm: Dict[str, Any]) -> Dict[str, Any]:
    """Build compact CDM attribute catalog (inline for now)"""
    catalog = {"domain": enhanced_cdm.get("cdm_metadata", {}).get("domain", "Unknown"), "entities": []}
    
    for entity in enhanced_cdm.get("entities", []):
        compact_entity = {
            "entity_name": entity.get("entity_name"),
            "business_definition": entity.get("business_definition", "")[:200],
            "attributes": []
        }
        
        for attr in entity.get("attributes", []):
            data_type = attr.get("data_type", "").upper()
            if data_type in ["VARCHAR", "CHAR", "TEXT", "STRING"]:
                coarse_type = "string"
            elif data_type in ["INT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT"]:
                coarse_type = "number"
            elif data_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                coarse_type = "date"
            elif data_type in ["BOOLEAN", "BOOL"]:
                coarse_type = "boolean"
            else:
                coarse_type = "string"
            
            compact_attr = {
                "name": attr.get("canonical_column"),
                "data_type": coarse_type,
                "glossary": attr.get("glossary_term", "")[:250],
                "business_context": attr.get("business_context", "")[:250]
            }
            compact_entity["attributes"].append(compact_attr)
        
        catalog["entities"].append(compact_entity)
    
    return catalog


def build_entity_prompt(
    config: AppConfig,
    compact_catalog: Dict[str, Any],
    guardrails_entity: Dict[str, Any]
) -> str:
    """Build prompt for single Guardrails entity mapping"""
    
    entity_name = guardrails_entity.get("entity_name")
    attributes = guardrails_entity.get("attributes", [])
    
    prompt = f"""Map Guardrails entity attributes to CDM.

DOMAIN: {config.cdm.domain}

GUARDRAILS ENTITY: {entity_name}
Description: {guardrails_entity.get("description", "N/A")}
Business Purpose: {guardrails_entity.get("business_purpose", "N/A")}
Attributes to map: {len(attributes)}

TASK:
1. Evaluate if Guardrails entity is business entity or interface artifact
2. For each attribute, find best CDM match or mark unmapped
3. Map = CDM gap if no match found

CDM ATTRIBUTE CATALOG:
{json.dumps(compact_catalog, indent=2)}

GUARDRAILS ATTRIBUTES:
{json.dumps(attributes, indent=2)}

OUTPUT (JSON only, no markdown):
{{
  "guardrails_entity": "{entity_name}",
  "entity_evaluation": {{
    "type": "business_entity | interface_artifact",
    "reasoning": "..."
  }},
  "mappings": [
    {{
      "guardrails_attribute": "carrier_code",
      "disposition": "mapped",
      "cdm_entity": "Organization",
      "cdm_attribute": "identifier_value",
      "mapping_type": "direct",
      "confidence": "high"
    }},
    {{
      "guardrails_attribute": "unknown_field",
      "disposition": "unmapped",
      "reason": "No semantic match in CDM, may indicate gap"
    }}
  ],
  "summary": {{
    "total_attributes": {len(attributes)},
    "mapped": 0,
    "extension_attribute": 0,
    "unmapped": 0
  }}
}}

RULES:
- Unmapped = potential CDM gap (serious)
- Semantic fit required (not forced)
- Output ONLY valid JSON
"""
    return prompt


def merge_mappings(enhanced_cdm: Dict[str, Any], entity_mappings: List[Dict]) -> Dict[str, Any]:
    """Merge entity mapping results into enhanced CDM"""
    
    # Build entity/attr lookup
    entity_lookup = {}
    for entity in enhanced_cdm.get("entities", []):
        entity_name = entity.get("entity_name")
        entity_lookup[entity_name] = entity
        entity["_attrs"] = {a.get("canonical_column"): a for a in entity.get("attributes", [])}
    
    # Apply mappings
    for entity_result in entity_mappings:
        for mapping in entity_result.get("mappings", []):
            disp = mapping.get("disposition")
            
            if disp == "mapped":
                cdm_ent = mapping.get("cdm_entity")
                cdm_attr = mapping.get("cdm_attribute")
                
                if cdm_ent in entity_lookup:
                    attr = entity_lookup[cdm_ent]["_attrs"].get(cdm_attr)
                    if attr:
                        if "source_mappings" not in attr:
                            attr["source_mappings"] = {}
                        attr["source_mappings"]["guardrails"] = {
                            "disposition": "mapped",
                            "guardrails_entity": entity_result.get("guardrails_entity"),
                            "guardrails_attribute": mapping.get("guardrails_attribute"),
                            "mapping_type": mapping.get("mapping_type", "direct"),
                            "added_in_step": "2c"
                        }
            
            elif disp == "extension_attribute":
                cdm_ent = mapping.get("cdm_entity")
                if cdm_ent in entity_lookup:
                    new_attr = {
                        "canonical_column": mapping.get("new_attribute_name"),
                        "source_column": mapping.get("new_attribute_name", "").upper(),
                        "data_type": mapping.get("data_type", "VARCHAR"),
                        "nullable": True,
                        "glossary_term": mapping.get("glossary", ""),
                        "business_context": mapping.get("business_context", ""),
                        "classification": "Extension",
                        "origin": {
                            "standard": "guardrails",
                            "created_in_step": "2c",
                            "justification": mapping.get("justification", "")
                        },
                        "source_mappings": {
                            "fhir": None,
                            "ncpdp": None,
                            "guardrails": {
                                "disposition": "extension_attribute",
                                "guardrails_entity": entity_result.get("guardrails_entity"),
                                "guardrails_attribute": mapping.get("guardrails_attribute"),
                                "added_in_step": "2c"
                            },
                            "glue": None
                        }
                    }
                    entity_lookup[cdm_ent]["attributes"].append(new_attr)
    
    # Cleanup
    for entity in enhanced_cdm.get("entities", []):
        if "_attrs" in entity:
            del entity["_attrs"]
    
    return enhanced_cdm


def build_disposition_report(entity_mappings: List[Dict], guardrails_entities: List[Dict]) -> Dict[str, Any]:
    """Build aggregated disposition report from all entity mappings"""
    
    total_gr_attrs = sum(len(e.get("attributes", [])) for e in guardrails_entities)
    
    # Count dispositions across all entities
    mapped_count = 0
    extension_count = 0
    unmapped_count = 0
    business_entities = 0
    interface_artifacts = 0
    
    entity_evaluations = []
    details = []
    
    for entity_result in entity_mappings:
        # Entity evaluation
        entity_eval = entity_result.get("entity_evaluation", {})
        eval_type = entity_eval.get("type", "unknown")
        
        if eval_type == "business_entity":
            business_entities += 1
        elif eval_type == "interface_artifact":
            interface_artifacts += 1
        
        entity_evaluations.append({
            "guardrails_entity": entity_result.get("guardrails_entity"),
            "evaluation": eval_type,
            "reasoning": entity_eval.get("reasoning", "")
        })
        
        # Mapping dispositions
        for mapping in entity_result.get("mappings", []):
            disp = mapping.get("disposition")
            
            if disp == "mapped":
                mapped_count += 1
                details.append({
                    "guardrails_entity": entity_result.get("guardrails_entity"),
                    "guardrails_attribute": mapping.get("guardrails_attribute"),
                    "disposition": "mapped",
                    "cdm_target": f"{mapping.get('cdm_entity')}.{mapping.get('cdm_attribute')}",
                    "mapping_type": mapping.get("mapping_type", "direct")
                })
            elif disp == "extension_attribute":
                extension_count += 1
                details.append({
                    "guardrails_entity": entity_result.get("guardrails_entity"),
                    "guardrails_attribute": mapping.get("guardrails_attribute"),
                    "disposition": "extension_attribute",
                    "cdm_target": f"{mapping.get('cdm_entity')}.{mapping.get('new_attribute_name')}",
                    "justification": mapping.get("justification", "")
                })
            elif disp == "unmapped":
                unmapped_count += 1
                details.append({
                    "guardrails_entity": entity_result.get("guardrails_entity"),
                    "guardrails_attribute": mapping.get("guardrails_attribute"),
                    "disposition": "unmapped",
                    "reason": mapping.get("reason", "")
                })
    
    return {
        "summary": {
            "total_guardrails_entities_evaluated": len(guardrails_entities),
            "business_entities_identified": business_entities,
            "interface_artifacts_identified": interface_artifacts,
            "total_attributes_evaluated": total_gr_attrs,
            "mapped_to_existing_cdm": mapped_count,
            "extension_attributes_added": extension_count,
            "unmapped_for_review": unmapped_count
        },
        "field_accounting": {
            "total_input_attributes": total_gr_attrs,
            "detailed_disposition_count": mapped_count + extension_count + unmapped_count,
            "total_accounted_for": mapped_count + extension_count + unmapped_count,
            "accounting_complete": (mapped_count + extension_count + unmapped_count) == total_gr_attrs
        },
        "entity_evaluations": entity_evaluations,
        "details": details
    }


def run_step2c(
    config: AppConfig,
    enhanced_cdm_file: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[dict]:
    """Step 2c: Per-entity Guardrails mapping"""
    
    print(f"  📖 Loading enhanced CDM from: {enhanced_cdm_file}")
    with open(enhanced_cdm_file, 'r', encoding='utf-8') as f:
        enhanced_cdm = json.load(f)
    
    entity_count = len(enhanced_cdm.get('entities', []))
    print(f"  📊 Enhanced CDM: {entity_count} entities")
    
    # Load Guardrails
    print(f"  📖 Loading Guardrails...")
    prep_outdir = outdir.parent / "prep"
    guardrails_files = sorted(prep_outdir.glob("rationalized_guardrails_*.json"))
    if not guardrails_files:
        print(f"  ❌ ERROR: No rationalized Guardrails found")
        return None
    
    with open(guardrails_files[-1], 'r', encoding='utf-8') as f:
        guardrails = json.load(f)
    
    gr_entities = guardrails.get('rationalized_entities', [])
    gr_attrs = sum(len(e.get('attributes', [])) for e in gr_entities)
    print(f"  📊 Guardrails: {len(gr_entities)} entities, {gr_attrs} attributes")
    
    # Build compact catalog
    print(f"  🔄 Building compact CDM catalog...")
    compact_catalog = build_compact_catalog(enhanced_cdm)
    catalog_size = sum(len(e["attributes"]) for e in compact_catalog["entities"])
    print(f"  📊 Catalog: {len(compact_catalog['entities'])} entities, {catalog_size} attributes")
    
    if dry_run:
        # Save example prompt
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        if gr_entities:
            example_prompt = build_entity_prompt(config, compact_catalog, gr_entities[0])
            output_file = prompts_dir / f"step2c_example_entity_{timestamp}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(example_prompt)
            print(f"  ✓ Example prompt saved: {output_file}")
            print(f"    Entities to process: {len(gr_entities)}")
            print(f"    Estimated calls: {len(gr_entities)}")
        return None
    
    # Process each Guardrails entity
    print(f"\n  🤖 Processing {len(gr_entities)} Guardrails entities...")
    entity_mappings = []
    unmapped_fields = []
    all_feedback = []  # Collect feedback from each entity
    
    for idx, gr_entity in enumerate(gr_entities, 1):
        entity_name = gr_entity.get("entity_name")
        attr_count = len(gr_entity.get("attributes", []))
        print(f"  [{idx}/{len(gr_entities)}] Processing {entity_name} ({attr_count} attrs)...")
        
        prompt = build_entity_prompt(config, compact_catalog, gr_entity)
        
        messages = [
            {"role": "system", "content": "You are a healthcare data architect. Return ONLY valid JSON."},
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
            
            # Collect feedback if present
            if "feedback" in result:
                all_feedback.append({
                    "guardrails_entity": entity_name,
                    "feedback": result["feedback"]
                })
            
            # Collect unmapped
            for mapping in result.get("mappings", []):
                if mapping.get("disposition") == "unmapped":
                    unmapped_fields.append({
                        "guardrails_entity": entity_name,
                        "guardrails_attribute": mapping.get("guardrails_attribute"),
                        "reason": mapping.get("reason", "")
                    })
            
            summary = result.get("summary", {})
            print(f"      Mapped: {summary.get('mapped', 0)}, Unmapped: {summary.get('unmapped', 0)}")
            
        except Exception as e:
            print(f"      ❌ ERROR processing {entity_name}: {e}")
            continue
    
    # Merge results
    print(f"\n  🔄 Merging {len(entity_mappings)} entity results...")
    enhanced_cdm = merge_mappings(enhanced_cdm, entity_mappings)
    
    # Build aggregated disposition report
    print(f"  📋 Building disposition report...")
    disposition_report = build_disposition_report(entity_mappings, gr_entities)
    
    # Save outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = config.cdm.domain.replace(' ', '_')
    
    # Save enhanced CDM
    cdm_file = outdir / f"enhanced_cdm_guardrails_{domain_safe}_{timestamp}.json"
    with open(cdm_file, 'w', encoding='utf-8') as f:
        json.dump(enhanced_cdm, f, indent=2)
    print(f"  ✓ Enhanced CDM saved: {cdm_file}")
    
    # Save disposition report
    disp_file = outdir / f"guardrails_disposition_report_{domain_safe}_{timestamp}.json"
    with open(disp_file, 'w', encoding='utf-8') as f:
        json.dump({
            "domain": config.cdm.domain,
            "step": "2c",
            "timestamp": timestamp,
            "disposition_report": disposition_report
        }, f, indent=2)
    print(f"  📋 Disposition report saved: {disp_file}")
    
    # Save aggregated feedback
    if all_feedback:
        feedback_file = outdir / f"step2c_feedback_{domain_safe}_{timestamp}.txt"
        with open(feedback_file, 'w', encoding='utf-8') as f:
            f.write(f"STEP 2C PROCESSING FEEDBACK (AGGREGATED)\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Domain: {config.cdm.domain}\n")
            f.write(f"Entities processed: {len(entity_mappings)}\n")
            f.write(f"=" * 80 + "\n\n")
            
            for entity_feedback in all_feedback:
                f.write(f"ENTITY: {entity_feedback['guardrails_entity']}\n")
                f.write(f"-" * 80 + "\n")
                feedback = entity_feedback['feedback']
                for key, value in feedback.items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
        
        print(f"  📝 Feedback saved: {feedback_file}")
    
    # Save unmapped fields
    if unmapped_fields:
        unmapped_file = outdir / f"unmapped_guardrails_{domain_safe}_{timestamp}.json"
        with open(unmapped_file, 'w', encoding='utf-8') as f:
            json.dump({
                "domain": config.cdm.domain,
                "step": "2c",
                "timestamp": timestamp,
                "total_unmapped": len(unmapped_fields),
                "unmapped_fields": unmapped_fields
            }, f, indent=2)
        print(f"  ⚠️  Unmapped fields: {unmapped_file} ({len(unmapped_fields)} fields)")
    
    # Summary
    total_mapped = sum(m.get("summary", {}).get("mapped", 0) for m in entity_mappings)
    total_unmapped = len(unmapped_fields)
    print(f"\n  📋 Summary:")
    print(f"     Entities processed: {len(entity_mappings)}")
    print(f"     Total mapped: {total_mapped}")
    print(f"     Total unmapped: {total_unmapped}")
    
    return enhanced_cdm