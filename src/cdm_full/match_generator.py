# src/cdm_full/match_generator.py
"""
Match file generation for Full CDM.

Handles AI-driven mapping of source attributes to CDM attributes.

For entities with > BATCH_THRESHOLD attributes (EDW, large Glue tables):
  - Processes in batches of BATCH_SIZE (150)
  - Each batch: full detail for 150 attributes + names-only for remainder
  - Provides full context without overwhelming output generation
  - entity_evaluation captured on first batch, reused for subsequent batches

For small entities (<= BATCH_THRESHOLD):
  - Single call with all attributes (original behaviour)

Functions:
  - build_compact_catalog(): Create token-efficient CDM representation
  - build_batch_prompt(): Build prompt for a batch of attributes (large entities)
  - build_source_entity_prompt(): Build prompt for small entity (single call)
  - generate_match_file(): Generate match file for a source
"""
from __future__ import annotations
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# Entities with more attributes than this threshold are processed in batches
# Entities with attribute counts above this threshold get split into
# sequential batches.  Raised from 150 -> 500 because:
#   1. Modern LLMs (gpt-5.x with 922K context) handle 500-attribute
#      prompts comfortably; the original 150 was tuned for older models.
#   2. Sequential batches inside one entity become the bottleneck under
#      per-entity parallelism — fewer batched entities = better wall-
#      clock when worker count > 1.
#   3. Most CDM entities have <= 50 attributes anyway; only a handful
#      of "kitchen sink" entities (e.g. Claims's ClaimTransaction at
#      181 attrs) ever hit the previous threshold.
BATCH_THRESHOLD = 500
BATCH_SIZE = 500


# =============================================================================
# CDM CATALOG
# =============================================================================

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


# =============================================================================
# PROMPT BUILDERS
# =============================================================================

def build_source_entity_prompt(
    config: AppConfig,
    source_type: str,
    compact_catalog: Dict,
    source_entity: Dict,
    domain_description: str,
) -> str:
    """
    Build prompt for a small entity — all attributes in a single call.
    Used when attribute count <= BATCH_THRESHOLD.
    """
    entity_name = source_entity.get("entity_name")
    attributes = source_entity.get("attributes", [])

    return f"""Map {source_type.upper()} entity attributes to the CDM. Every source attribute MUST be accounted for.

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
3. High Quality mapping is REQUIRED - review EACH AND EVERY ATTRIBUTE for a proper match in CDM.
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
    "maps_to_cdm_entity": "PrimaryEntityName",
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


def build_batch_prompt(
    config: AppConfig,
    source_type: str,
    compact_catalog: Dict,
    source_entity: Dict,
    domain_description: str,
    batch_attrs: List[Dict],
    remaining_names: List[str],
    batch_num: int,
    total_batches: int,
    entity_evaluation: Optional[Dict] = None,
) -> str:
    """
    Build prompt for one batch of a large entity.

    batch_attrs    : Full attribute detail for this batch (up to BATCH_SIZE attrs).
    remaining_names: Names only for all other attributes — context only, do NOT map.
    entity_evaluation: Result from batch 1, passed into batches 2..N for consistency.
    """
    entity_name = source_entity.get("entity_name")
    total_attrs = len(source_entity.get("attributes", []))

    # Batch 1: ask AI to determine entity_evaluation
    # Batch 2+: supply the already-determined entity_evaluation as context
    if batch_num == 1:
        entity_eval_instruction = (
            "- Determine which CDM entity (or entities) this source entity primarily maps to "
            "and include an entity_evaluation block in your output."
        )
        entity_eval_context = ""
        entity_eval_output = """  "entity_evaluation": {
    "maps_to_cdm_entity": "PrimaryEntityName",
    "confidence": "high",
    "reasoning": "Brief reasoning..."
  },"""
    else:
        entity_eval_instruction = (
            "- Entity evaluation has already been determined (see ENTITY EVALUATION below). "
            "Do NOT include entity_evaluation in your output — output attribute_mappings only."
        )
        entity_eval_context = f"""
ENTITY EVALUATION (already determined — for context only):
{json.dumps(entity_evaluation, indent=2)}
"""
        entity_eval_output = ""

    remaining_section = ""
    if remaining_names:
        remaining_section = f"""
REMAINING ATTRIBUTES — names only (context only — do NOT map these):
{json.dumps(remaining_names)}
"""

    output_example = f"""{{
{entity_eval_output}
  "attribute_mappings": [
    {{
      "source_attribute": "example_field",
      "disposition": "mapped",
      "cdm_entity": "SomeEntity",
      "cdm_attribute": "some_attribute",
      "mapping_type": "direct",
      "confidence": "high",
      "requires_review": false,
      "validation_rules_extracted": [],
      "business_rules_extracted": []
    }}
  ],
  "summary": {{
    "batch": {batch_num},
    "batch_attributes": {len(batch_attrs)},
    "mapped": 0,
    "unmapped": 0,
    "requires_review": 0
  }}
}}"""

    return f"""Map {source_type.upper()} attributes to the CDM. Batch {batch_num} of {total_batches}.

DOMAIN: {config.cdm.domain}
DOMAIN CONTEXT: {domain_description}

SOURCE TYPE: {source_type.upper()}
SOURCE ENTITY: {entity_name} ({total_attrs} attributes total)
Description: {source_entity.get("description", "N/A")}
Business Context: {source_entity.get("business_context", "N/A")}
{entity_eval_context}
TASK:
- Map ONLY the {len(batch_attrs)} attributes listed in "ATTRIBUTES TO MAP NOW"
{entity_eval_instruction}
- Extract validation_rules and business_rules from source metadata
- High quality mapping is REQUIRED for every attribute in this batch
- Few unmapped attributes expected — mark genuine gaps with disposition "unmapped"
- Low confidence mappings: set requires_review=true with review_reason

CRITICAL:
- Map ONLY the attributes in "ATTRIBUTES TO MAP NOW" — return EXACTLY {len(batch_attrs)} mappings
- Do NOT map any attributes from the "REMAINING ATTRIBUTES" section
- The remaining attributes are provided for context so you understand the full entity shape

CDM CATALOG:
{json.dumps(compact_catalog, indent=2)}

ATTRIBUTES TO MAP NOW ({len(batch_attrs)} of {total_attrs}):
{json.dumps(batch_attrs, indent=2)}
{remaining_section}
OUTPUT (JSON only, no markdown):
{output_example}

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
- Output ONLY valid JSON
"""


# =============================================================================
# RESPONSE PARSING
# =============================================================================

def _parse_response(response: str) -> Dict:
    """Parse LLM response, stripping markdown fences if present."""
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if len(lines) > 2 else lines[1:]).strip()
    return json.loads(text)


# =============================================================================
# CHUNKED ENTITY MAPPING
# =============================================================================

def _map_entity_chunked(
    config: AppConfig,
    source_type: str,
    compact_catalog: Dict,
    source_entity: Dict,
    domain_description: str,
    llm: LLMClient,
) -> Tuple[Dict, List[Dict]]:
    """
    Map a large entity in batches of BATCH_SIZE.

    Returns:
        (entity_evaluation, all_attribute_mappings)
    """
    all_attrs = source_entity.get("attributes", [])
    total = len(all_attrs)
    batches = [all_attrs[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    n_batches = len(batches)

    all_mappings: List[Dict] = []
    entity_evaluation: Optional[Dict] = None

    for bi, batch in enumerate(batches, 1):
        # Build names-only list = every attribute NOT in this batch
        batch_names = {a.get("attribute_name") for a in batch}
        remaining_names = [
            a.get("attribute_name")
            for a in all_attrs
            if a.get("attribute_name") not in batch_names
        ]

        prompt = build_batch_prompt(
            config=config,
            source_type=source_type,
            compact_catalog=compact_catalog,
            source_entity=source_entity,
            domain_description=domain_description,
            batch_attrs=batch,
            remaining_names=remaining_names,
            batch_num=bi,
            total_batches=n_batches,
            entity_evaluation=entity_evaluation,
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert healthcare data engineer mapping source attributes to a CDM. "
                    "Return ONLY valid JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ]

        print(f"       batch {bi}/{n_batches} ({len(batch)} attrs)...", end=" ", flush=True)

        result = _parse_response(llm.chat(messages)[0])

        # Capture entity_evaluation from batch 1
        if bi == 1 and "entity_evaluation" in result:
            entity_evaluation = result["entity_evaluation"]

        batch_mappings = result.get("attribute_mappings", [])
        all_mappings.extend(batch_mappings)

        batch_summary = result.get("summary", {})
        print(
            f"mapped: {batch_summary.get('mapped', len(batch_mappings))}, "
            f"unmapped: {batch_summary.get('unmapped', 0)}, "
            f"review: {batch_summary.get('requires_review', 0)}"
        )

        if len(batch_mappings) != len(batch):
            print(
                f"       ⚠️  WARNING: batch {bi} returned {len(batch_mappings)} "
                f"mappings for {len(batch)} attributes"
            )

    # Fallback if batch 1 didn't return entity_evaluation
    if entity_evaluation is None:
        entity_evaluation = {
            "maps_to_cdm_entity": "",
            "confidence": "low",
            "reasoning": "Entity evaluation not returned by AI",
        }

    return entity_evaluation, all_mappings


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def generate_match_file(
    config: AppConfig,
    source_type: str,
    rationalized_file: Path,
    full_cdm: Dict,
    llm: LLMClient,
    full_cdm_dir: Path,
    domain_description: str,
    dry_run: bool = False,
    max_workers: int = 1,
) -> Optional[Path]:
    """
    Generate match file for a single source.

    Small entities (<=BATCH_THRESHOLD attrs): single AI call.
    Large entities  (> BATCH_THRESHOLD attrs): sequential batches of
        BATCH_SIZE; entity_evaluation from batch 1 is reused for 2..N
        so batches stay in order within a single entity.

    Per-entity execution is parallelisable: with ``max_workers > 1``
    the per-entity loop runs in a ThreadPoolExecutor.  LLM calls are
    I/O-bound so threads are sufficient.  Entities are dispatched
    largest-first (longest-processing-time scheduling) so the longest
    entity occupies one worker for the duration while smaller entities
    chew through the rest in parallel — total wall-clock approaches the
    time of the largest entity.

    Args:
        config: App configuration
        source_type: Source type (e.g., "guardrails", "edw", "glue")
        rationalized_file: Path to rationalized source file
        full_cdm: Full CDM dict
        llm: LLM client
        full_cdm_dir: Output directory for match files
        domain_description: Domain context description
        dry_run: If True, save example prompts only
        max_workers: Concurrent per-entity worker count (default 1 =
            sequential).  Per-entity calls are independent, so threads
            are safe.  Tier 4: 8-16 reasonable.

    Returns:
        Path to match file, or None if dry_run
    """
    print(f"\n   {'─'*50}")
    print(f"   Generating match file: {source_type.upper()}")
    print(f"   {'─'*50}")

    # Load rationalized source
    with open(rationalized_file, "r", encoding="utf-8") as fh:
        rationalized = json.load(fh)

    source_entities = rationalized.get("entities", [])
    total_attrs = sum(len(e.get("attributes", [])) for e in source_entities)
    print(f"   Source: {rationalized_file.name}")
    print(f"   Entities: {len(source_entities)}, Attributes: {total_attrs}")

    # Build compact catalog once — shared across all entity calls
    compact_catalog = build_compact_catalog(full_cdm)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── DRY RUN ──────────────────────────────────────────────────────────────
    if dry_run:
        if source_entities:
            prompts_dir = full_cdm_dir / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            example_entity = source_entities[0]
            attr_count = len(example_entity.get("attributes", []))

            if attr_count > BATCH_THRESHOLD:
                all_attrs = example_entity.get("attributes", [])
                batch = all_attrs[:BATCH_SIZE]
                remaining_names = [a.get("attribute_name") for a in all_attrs[BATCH_SIZE:]]
                n_batches = -(-attr_count // BATCH_SIZE)
                example_prompt = build_batch_prompt(
                    config, source_type, compact_catalog, example_entity,
                    domain_description, batch, remaining_names, 1, n_batches,
                )
            else:
                example_prompt = build_source_entity_prompt(
                    config, source_type, compact_catalog, example_entity, domain_description
                )

            output_file = prompts_dir / f"match_{source_type}_example_{timestamp}.txt"
            output_file.write_text(example_prompt, encoding="utf-8")
            print(f"   ✓ Example prompt saved: {output_file.name}")
        return None

    # ── LIVE ─────────────────────────────────────────────────────────────────
    entity_mappings: List[Dict[str, Any]] = []
    ai_failures: List[Dict[str, Any]] = []

    # Sort largest-first so the longest entity occupies one worker for the
    # full duration while the remaining workers churn through smaller
    # entities in parallel — minimises total wall-clock under threading.
    sorted_entities = sorted(
        source_entities,
        key=lambda e: -len(e.get("attributes", [])),
    )
    total = len(sorted_entities)
    workers = max(1, int(max_workers))

    log_lock = threading.Lock()
    progress = {"done": 0}

    def _process_entity(source_entity: Dict[str, Any]) -> Tuple[Optional[Dict], Optional[Dict]]:
        entity_name = source_entity.get("entity_name")
        attr_count = len(source_entity.get("attributes", []))
        n_batches = -(-attr_count // BATCH_SIZE)  # ceiling division

        try:
            if attr_count > BATCH_THRESHOLD:
                entity_evaluation, all_mappings = _map_entity_chunked(
                    config, source_type, compact_catalog,
                    source_entity, domain_description, llm,
                )
                mapped = sum(1 for m in all_mappings if m.get("disposition") == "mapped")
                unmapped = sum(1 for m in all_mappings if m.get("disposition") == "unmapped")
                review = sum(1 for m in all_mappings if m.get("requires_review"))
                result = {
                    "source_type": source_type,
                    "source_entity": entity_name,
                    "entity_evaluation": entity_evaluation,
                    "attribute_mappings": all_mappings,
                    "summary": {
                        "total_attributes": attr_count,
                        "mapped": mapped,
                        "unmapped": unmapped,
                        "requires_review": review,
                    },
                }
                shape = f"{attr_count} attrs → {n_batches} batches"
            else:
                prompt = build_source_entity_prompt(
                    config, source_type, compact_catalog,
                    source_entity, domain_description,
                )
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are an expert healthcare data engineer and data analyst "
                            "experienced mapping source to target data. Return ONLY valid JSON."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ]
                result = _parse_response(llm.chat(messages)[0])
                shape = f"{attr_count} attrs"

            with log_lock:
                progress["done"] += 1
                idx = progress["done"]
                summary = result.get("summary", {})
                print(
                    f"   [{idx}/{total}] {entity_name} ({shape}) — "
                    f"mapped: {summary.get('mapped', 0)}, "
                    f"unmapped: {summary.get('unmapped', 0)}, "
                    f"review: {summary.get('requires_review', 0)}"
                )
            return result, None

        except Exception as exc:
            failure = {
                "source_entity": entity_name,
                "attribute_count": attr_count,
                "error": str(exc),
                "timestamp": datetime.now().isoformat(),
            }
            with log_lock:
                progress["done"] += 1
                idx = progress["done"]
                print(f"   [{idx}/{total}] {entity_name} FAILED: {exc}")
            return None, failure

    if workers == 1:
        # Sequential — preserves prior log shape for users running serial
        for source_entity in sorted_entities:
            r, f = _process_entity(source_entity)
            if r is not None:
                entity_mappings.append(r)
            if f is not None:
                ai_failures.append(f)
    else:
        # Parallel — each entity is independent; futures complete in any order
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_process_entity, e) for e in sorted_entities]
            for fut in as_completed(futures):
                r, f = fut.result()
                if r is not None:
                    entity_mappings.append(r)
                if f is not None:
                    ai_failures.append(f)

    # ── Save match file ───────────────────────────────────────────────────────
    match_file_data = {
        "source_type": source_type,
        "source_file": rationalized_file.name,
        "generated_timestamp": datetime.now().isoformat(),
        "source_entity_count": len(source_entities),
        "source_attribute_count": total_attrs,
        "ai_failures": ai_failures,
        "entity_mappings": entity_mappings,
    }

    match_file_path = full_cdm_dir / f"match_{source_type}_{timestamp}.json"
    with open(match_file_path, "w", encoding="utf-8") as fh:
        json.dump(match_file_data, fh, indent=2)

    print(f"   ✓ Match file saved: {match_file_path.name}")
    print(f"     Processed: {len(entity_mappings)} success, {len(ai_failures)} failures")
    if ai_failures:
        print(f"   ⚠️  AI failures logged - review match file for details")

    return match_file_path