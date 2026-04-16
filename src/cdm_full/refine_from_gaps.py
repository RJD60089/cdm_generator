# src/cdm_full/refine_from_gaps.py
"""
Ancillary Gap-Driven CDM Refinement

Called from within build_full_cdm.py after initial mapping + gap report
when ancillary sources are configured in Refiner mode.

Uses concrete gap data (actual unmapped fields) rather than speculative
comparison to recommend CDM modifications:
  1. ANALYZE: AI reviews gap report + rationalized ancillary → recommendations
  2. REVIEW: Interactive user review (Accept/Reject/Modify/Skip/Quit)
  3. APPLY: AI modifies CDM structure with approved changes

Follows the refine_consolidation.py 3-phase pattern.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# =============================================================================
# PHASE 1: ANALYZE GAPS
# =============================================================================

def build_analysis_prompt(
    cdm: Dict,
    gap_report: Dict,
    ancillary_data: Dict,
    config: AppConfig,
) -> str:
    """Build prompt for AI to analyze gaps and recommend CDM modifications.

    Args:
        cdm: Current full CDM
        gap_report: Gap report with unmapped ancillary fields
        ancillary_data: Rationalized ancillary source data
        config: AppConfig for domain context

    Returns:
        Analysis prompt string
    """
    # Extract unmapped ancillary fields from gap report
    unmapped = gap_report.get("unmapped_fields", [])
    ancillary_unmapped = [
        f for f in unmapped
        if f.get("source_type", "").lower().startswith("ancillary")
    ]

    # Build compact CDM summary
    cdm_summary = []
    for entity in cdm.get("entities", []):
        attrs = [a.get("attribute_name", "") for a in entity.get("attributes", [])]
        cdm_summary.append({
            "entity_name": entity.get("entity_name"),
            "classification": entity.get("classification"),
            "attribute_count": len(attrs),
            "attributes": attrs[:20],  # First 20 for context
            "has_more": len(attrs) > 20,
        })

    prompt = f"""You are a senior data architect analyzing mapping gaps between an ancillary
source system and an existing CDM. Your task is to recommend CDM modifications
to address unmapped fields.

=============================================================================
DOMAIN CONTEXT
=============================================================================

DOMAIN: {config.cdm.domain}
DESCRIPTION: {config.cdm.description}

=============================================================================
CURRENT CDM STRUCTURE ({len(cdm_summary)} entities)
=============================================================================

{json.dumps(cdm_summary, indent=2)}

=============================================================================
UNMAPPED ANCILLARY FIELDS ({len(ancillary_unmapped)} fields)
=============================================================================

These fields from the ancillary source could not be mapped to any existing
CDM entity or attribute:

{json.dumps(ancillary_unmapped, indent=2)}

=============================================================================
ANCILLARY SOURCE CONTEXT
=============================================================================

Full rationalized ancillary entities for reference:

{json.dumps(ancillary_data.get("entities", []), indent=2)}

=============================================================================
YOUR TASK
=============================================================================

Analyze the unmapped fields and recommend CDM modifications. For each
recommendation, specify one of these actions:

1. **add_entity**: Create a new entity in the CDM (when unmapped fields
   represent a business concept not currently modeled)
2. **add_attribute**: Add an attribute to an existing entity (when an
   unmapped field belongs to a concept already modeled)
3. **modify_attribute**: Change an existing attribute's type/description
   (when a mapping failed due to type mismatch)
4. **add_relationship**: Add a relationship between entities (when
   unmapped FK fields reveal missing connections)

For each recommendation:
- Provide clear justification grounded in the gap data
- Assign a confidence score (0.0-1.0)
- Only recommend changes supported by actual unmapped fields
- Do NOT recommend changes for audit/technical columns unless they
  carry business meaning
- Group related unmapped fields into single recommendations where possible

=============================================================================
OUTPUT FORMAT
=============================================================================

Return ONLY valid JSON:

{{
  "analysis_summary": {{
    "total_unmapped": {len(ancillary_unmapped)},
    "recommendations_count": 0,
    "coverage_improvement_estimate": "X%"
  }},
  "recommendations": [
    {{
      "id": "REC-001",
      "action": "add_entity | add_attribute | modify_attribute | add_relationship",
      "target_entity": "ExistingOrNewEntityName",
      "details": {{
        "entity_name": "NewEntity",
        "classification": "Core | Reference | Junction",
        "description": "...",
        "attributes": [
          {{"name": "attr_name", "type": "VARCHAR(50)", "description": "..."}},
        ],
        "relationships": []
      }},
      "unmapped_fields_addressed": ["field1", "field2"],
      "justification": "...",
      "confidence": 0.85
    }}
  ]
}}

Return ONLY the JSON. No explanation, no markdown code blocks."""

    return prompt


def analyze_gaps(
    cdm: Dict,
    gap_report: Dict,
    ancillary_data: Dict,
    config: AppConfig,
    llm: LLMClient,
    outdir: Path,
    dry_run: bool = False,
) -> Optional[Dict]:
    """Phase 1: Analyze gaps and generate recommendations.

    Returns:
        Analysis result dict with recommendations, or None
    """
    print(f"\n   {'='*50}")
    print(f"   PHASE 1: ANALYZE ANCILLARY GAPS")
    print(f"   {'='*50}")

    prompt = build_analysis_prompt(cdm, gap_report, ancillary_data, config)

    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        output_file = prompts_dir / f"ancillary_gap_analysis_{timestamp}.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(prompt)
        print(f"   Prompt saved: {output_file.name}")
        print(f"     Characters: {len(prompt):,}")
        return None

    print(f"   Calling LLM for gap analysis...")
    print(f"      Prompt size: {len(prompt):,} chars (~{len(prompt) // 4:,} tokens)")

    messages = [
        {"role": "system", "content": "You are a senior data architect. Return ONLY valid JSON."},
        {"role": "user", "content": prompt},
    ]

    response, _ = llm.chat(messages)

    text = response.strip()
    if text.startswith("```"):
        lines = text.split("```")
        if len(lines) >= 2:
            text = lines[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

    analysis = json.loads(text)

    rec_count = len(analysis.get("recommendations", []))
    print(f"   Analysis complete: {rec_count} recommendations")

    return analysis


# =============================================================================
# PHASE 2: INTERACTIVE REVIEW
# =============================================================================

def review_recommendations(analysis: Dict) -> Dict:
    """Phase 2: Interactive user review of recommendations.

    Follows refine_consolidation.review_recommendations() pattern.

    Returns:
        Dict with approved_changes and rejected_changes
    """
    recs = analysis.get("recommendations", [])

    if not recs:
        print("\n   No recommendations to review.")
        return {"approved_changes": [], "rejected_changes": []}

    print(f"\n   {'='*50}")
    print(f"   PHASE 2: REVIEW RECOMMENDATIONS ({len(recs)} items)")
    print(f"   {'='*50}")

    approved: List[Dict] = []
    rejected: List[Dict] = []
    choice = ""

    for i, rec in enumerate(recs, 1):
        rec_id = rec.get("id", f"REC-{i:03d}")
        action = rec.get("action", "unknown")
        target = rec.get("target_entity", "")
        confidence = rec.get("confidence", 0)
        justification = rec.get("justification", "")
        fields = rec.get("unmapped_fields_addressed", [])

        print(f"\n   --- {rec_id} ({i}/{len(recs)}) ---")
        print(f"   Action: {action}")
        print(f"   Target: {target}")
        print(f"   Confidence: {confidence:.0%}")
        print(f"   Justification: {justification}")
        if fields:
            print(f"   Addresses: {', '.join(fields[:5])}")
            if len(fields) > 5:
                print(f"              ... and {len(fields) - 5} more")

        # Show details based on action type
        details = rec.get("details", {})
        if action == "add_entity":
            attrs = details.get("attributes", [])
            print(f"   New entity: {details.get('entity_name')} ({details.get('classification')})")
            print(f"   Attributes: {len(attrs)}")
        elif action == "add_attribute":
            attrs = details.get("attributes", [])
            for a in attrs[:3]:
                print(f"     + {a.get('name')} ({a.get('type', 'VARCHAR')})")

        while True:
            choice = input(f"\n   [A]pprove / [R]eject / [S]kip / [Q]uit? ").strip().upper()

            if choice == "A":
                approved.append(rec)
                print(f"   Approved")
                break
            elif choice == "R":
                rejected.append(rec)
                print(f"   Rejected")
                break
            elif choice == "S":
                rejected.append(rec)
                print(f"   Skipped (treated as reject)")
                break
            elif choice == "Q":
                print(f"\n   Stopping review. {len(approved)} approved, {len(recs) - i} not reviewed.")
                rejected.extend(recs[i:])
                break
            else:
                print("   Invalid choice. Please enter A, R, S, or Q.")

        if choice == "Q":
            break

    result = {
        "approved_changes": approved,
        "rejected_changes": rejected,
        "review_date": datetime.now().isoformat(),
        "total_approved": len(approved),
        "total_rejected": len(rejected),
    }

    print(f"\n   {'='*50}")
    print(f"   REVIEW COMPLETE")
    print(f"   {'='*50}")
    print(f"   Approved: {len(approved)}")
    print(f"   Rejected: {len(rejected)}")

    return result


# =============================================================================
# PHASE 3: APPLY MODIFICATIONS
# =============================================================================

def _build_compact_catalog(cdm: Dict) -> List[Dict]:
    """Build compact CDM catalog for relationship context (entity + attribute names only)."""
    catalog = []
    for entity in cdm.get("entities", []):
        attr_names = []
        for a in entity.get("attributes", []):
            if isinstance(a, dict):
                attr_names.append(a.get("attribute_name") or a.get("name") or "")
            elif isinstance(a, str):
                attr_names.append(a)
        catalog.append({
            "entity_name": entity.get("entity_name"),
            "classification": entity.get("classification"),
            "attributes": attr_names,
        })
    return catalog


def _extract_targeted_entities(cdm: Dict, approved_changes: List[Dict]) -> List[Dict]:
    """Extract only the entities that are targeted by approved changes.

    For add_attribute/modify_attribute/add_relationship: returns the entity
    stripped of all source_lineage (entity-level AND attribute-level) to
    minimize prompt size.
    For add_entity: nothing needed from existing CDM.
    """
    target_names = set()
    for change in approved_changes:
        action = change.get("action", "")
        if action in ("add_attribute", "modify_attribute", "add_relationship"):
            target = change.get("target_entity", "")
            if target:
                target_names.add(target.lower())

    targeted = []
    for entity in cdm.get("entities", []):
        if entity.get("entity_name", "").lower() in target_names:
            # Strip entity-level source_lineage
            slim = {k: v for k, v in entity.items() if k != "source_lineage"}
            # Strip attribute-level source_lineage and other bulk fields
            slim_attrs = []
            for attr in slim.get("attributes", []):
                if isinstance(attr, dict):
                    slim_attr = {k: v for k, v in attr.items()
                                 if k not in ("source_lineage", "business_rules",
                                              "validation_rules", "possible_values",
                                              "example_values")}
                    slim_attrs.append(slim_attr)
            slim["attributes"] = slim_attrs
            targeted.append(slim)

    return targeted


def build_apply_prompt(cdm: Dict, approved_changes: List[Dict]) -> str:
    """Build prompt for AI to apply approved CDM modifications.

    OPTIMIZED: Sends only targeted entities + compact catalog instead of
    the entire full CDM. This reduces prompt size from ~250K tokens to ~10-20K.
    """
    compact_catalog = _build_compact_catalog(cdm)
    targeted_entities = _extract_targeted_entities(cdm, approved_changes)

    prompt = f"""You are a senior data architect. Your task is to generate ONLY the new or
modified entities for approved CDM modifications.

===============================================================================
CDM CATALOG (entity names + attributes for relationship context)
===============================================================================

{json.dumps(compact_catalog, indent=2)}

===============================================================================
TARGETED ENTITIES (entities being modified — full detail, no lineage)
===============================================================================

{json.dumps(targeted_entities, indent=2)}

===============================================================================
APPROVED MODIFICATIONS
===============================================================================

{json.dumps(approved_changes, indent=2)}

===============================================================================
RULES
===============================================================================

1. APPLY ONLY the approved modifications.

2. For add_entity: Create the new entity with the specified attributes,
   classification, and relationships. Include a surrogate PK named
   {{entity_name_snake}}_id (INTEGER, pk=true, required=true).
   Include created_at and updated_at DATETIME attributes.

3. For add_attribute: Return the COMPLETE entity (all existing attributes
   plus the new ones). Use the targeted entity above as the base.

4. For modify_attribute: Return the COMPLETE entity with the attribute
   modified. Use the targeted entity above as the base.

5. For add_relationship: Return the COMPLETE entity with the new
   relationship added. Use the targeted entity above as the base.

6. Return ONLY entities that are new or modified. Do NOT return unchanged
   entities.

===============================================================================
OUTPUT FORMAT
===============================================================================

Return ONLY valid JSON with this structure:

{{
  "entities": [
    {{
      "entity_name": "EntityName",
      "description": "...",
      "classification": "Core|Reference|Junction",
      "attributes": [...],
      "relationships": [...]
    }}
  ],
  "ancillary_refinement_log": [
    {{
      "id": "REC-001",
      "action": "add_entity",
      "summary": "Added PharmacyNetwork entity with 12 attributes"
    }}
  ]
}}

Return ONLY the JSON. No explanation, no markdown."""

    return prompt


def _merge_entities(cdm: Dict, llm_response: Dict) -> Tuple[Dict, List[Dict]]:
    """Programmatically merge LLM-returned entities into the full CDM.

    - Modified entities: replace structure, preserve source_lineage
    - New entities: append with empty source_lineage
    - Unchanged entities: left untouched

    Returns:
        (modified_cdm, refinement_log)
    """
    import copy
    modified_cdm = copy.deepcopy(cdm)

    returned_entities = llm_response.get("entities", [])
    log = llm_response.get("ancillary_refinement_log", [])

    # Normalize attribute keys — LLM may return "name" instead of "attribute_name"
    for entity in returned_entities:
        normalized_attrs = []
        for attr in entity.get("attributes", []):
            if isinstance(attr, str):
                # LLM returned a bare string — wrap it
                attr = {"attribute_name": attr, "type": "VARCHAR"}
            elif isinstance(attr, dict):
                if not attr.get("attribute_name") and attr.get("name"):
                    attr["attribute_name"] = attr.pop("name")
            normalized_attrs.append(attr)
        entity["attributes"] = normalized_attrs

    # Build lookup of existing entities by name (case-insensitive)
    existing_lookup = {}
    for i, entity in enumerate(modified_cdm.get("entities", [])):
        name = entity.get("entity_name", "").lower()
        existing_lookup[name] = i

    for returned_entity in returned_entities:
        name = returned_entity.get("entity_name", "")
        name_lower = name.lower()

        if name_lower in existing_lookup:
            # Modified entity — preserve source_lineage from original
            idx = existing_lookup[name_lower]
            original = modified_cdm["entities"][idx]
            original_lineage = original.get("source_lineage", {})

            # Also preserve per-attribute source_lineage
            original_attr_lineage = {}
            for attr in original.get("attributes", []):
                if not isinstance(attr, dict):
                    continue
                attr_name = (attr.get("attribute_name") or attr.get("name", "")).lower()
                if attr_name:
                    original_attr_lineage[attr_name] = attr.get("source_lineage", {})

            # Replace entity structure with LLM output
            modified_cdm["entities"][idx] = returned_entity

            # Restore entity-level source_lineage
            modified_cdm["entities"][idx]["source_lineage"] = original_lineage

            # Restore per-attribute source_lineage
            for attr in modified_cdm["entities"][idx].get("attributes", []):
                if not isinstance(attr, dict):
                    continue
                attr_name = (attr.get("attribute_name") or attr.get("name", "")).lower()
                if attr_name in original_attr_lineage:
                    attr["source_lineage"] = original_attr_lineage[attr_name]

        else:
            # New entity — initialize empty source_lineage
            returned_entity.setdefault("source_lineage", {})
            for attr in returned_entity.get("attributes", []):
                if isinstance(attr, dict):
                    attr.setdefault("source_lineage", {})
            modified_cdm["entities"].append(returned_entity)

    # Attach log
    modified_cdm["ancillary_refinement_log"] = log

    return modified_cdm, log


def apply_modifications(
    cdm: Dict,
    approved: Dict,
    llm: LLMClient,
    outdir: Path,
    domain: str,
    dry_run: bool = False,
) -> Tuple[Dict, bool]:
    """Phase 3: Apply approved modifications to CDM.

    OPTIMIZED: Sends only targeted entities + compact catalog to LLM,
    gets back only new/modified entities, merges programmatically.
    Preserves all source_lineage from the original CDM.

    Returns:
        (modified_cdm, was_modified)
    """
    print(f"\n   {'='*50}")
    print(f"   PHASE 3: APPLY MODIFICATIONS")
    print(f"   {'='*50}")

    approved_changes = approved.get("approved_changes", [])

    if not approved_changes:
        print("   No approved changes to apply. CDM unchanged.")
        return cdm, False

    print(f"   Applying {len(approved_changes)} approved modifications...")

    prompt = build_apply_prompt(cdm, approved_changes)

    print(f"   Prompt size: {len(prompt):,} chars (~{len(prompt) // 4:,} tokens)")

    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        output_file = prompts_dir / f"ancillary_gap_apply_{timestamp}.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(prompt)
        print(f"   Prompt saved: {output_file.name}")
        return cdm, False

    messages = [
        {"role": "system", "content": "You are a senior data architect. Return ONLY valid JSON."},
        {"role": "user", "content": prompt},
    ]

    response, _ = llm.chat(messages)

    text = response.strip()
    if text.startswith("```"):
        lines = text.split("```")
        if len(lines) >= 2:
            text = lines[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

    llm_response = json.loads(text)

    if "entities" not in llm_response:
        raise ValueError("LLM response missing 'entities' key")

    # Programmatic merge — preserves source_lineage
    modified_cdm, log = _merge_entities(cdm, llm_response)

    # Report results
    orig_count = len(cdm.get("entities", []))
    new_count = len(modified_cdm.get("entities", []))

    print(f"   Entities: {orig_count} -> {new_count}")
    print(f"   Changes applied: {len(log)}")
    for entry in log:
        print(f"     - {entry.get('id', '?')}: {entry.get('summary', '?')}")

    # Save refined CDM
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = domain.lower().replace(" ", "_")
    output_file = outdir / f"cdm_{domain_safe}_ancillary_refined_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(modified_cdm, f, indent=2)
    print(f"   Saved: {output_file.name}")

    return modified_cdm, True


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_ancillary_gap_refinement(
    cdm: Dict,
    gap_report: Dict,
    ancillary_data: Dict,
    config: AppConfig,
    llm: LLMClient,
    outdir: Path,
    domain: str,
    dry_run: bool = False,
) -> Tuple[Dict, bool]:
    """Refine CDM based on actual mapping gaps from ancillary sources.

    Called from within build_full_cdm.py after initial map + gap report.

    Flow:
      1. ANALYZE: AI reviews gap report + ancillary → recommendations
      2. REVIEW: Interactive user review (A/R/S/Q)
      3. APPLY: AI modifies CDM structure with approved changes

    Args:
        cdm: Current full CDM dict
        gap_report: Gap report with unmapped fields
        ancillary_data: Rationalized ancillary source data
        config: AppConfig for domain context
        llm: LLM client
        outdir: Output directory for saving results
        domain: CDM domain name
        dry_run: If True, save prompts only

    Returns:
        (modified_cdm, was_modified) tuple
    """
    print(f"\n   {'='*50}")
    print(f"   ANCILLARY GAP-DRIVEN CDM REFINEMENT")
    print(f"   {'='*50}")

    # Phase 1: Analyze
    analysis = analyze_gaps(cdm, gap_report, ancillary_data, config, llm, outdir, dry_run)

    if analysis is None or dry_run:
        return cdm, False

    if not analysis.get("recommendations"):
        print("\n   No refinement recommendations generated.")
        return cdm, False

    # Phase 2: Review
    review_result = review_recommendations(analysis)

    # Save full review record (approved + rejected + analysis)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = domain.lower().replace(" ", "_")
    review_record = {
        "domain": domain,
        "review_date": review_result.get("review_date"),
        "analysis_summary": analysis.get("analysis_summary", {}),
        "total_recommendations": len(analysis.get("recommendations", [])),
        "total_approved": review_result.get("total_approved", 0),
        "total_rejected": review_result.get("total_rejected", 0),
        "approved_changes": review_result.get("approved_changes", []),
        "rejected_changes": review_result.get("rejected_changes", []),
    }
    review_file = outdir / f"ancillary_refinement_review_{domain_safe}_{timestamp}.json"
    with open(review_file, "w", encoding="utf-8") as f:
        json.dump(review_record, f, indent=2)
    print(f"\n   Review saved: {review_file.name}")

    if not review_result.get("approved_changes"):
        print("\n   No changes approved. CDM unchanged.")
        return cdm, False

    # Phase 3: Apply
    return apply_modifications(cdm, review_result, llm, outdir, domain, dry_run)
