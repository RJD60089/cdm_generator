# src/artifacts/common/rule_consolidator.py
"""
AI-driven business-rule consolidation.

Processes ONE attribute per LLM call. For every attribute with at least
one business or validation rule, the LLM receives only that attribute's
rules, consolidates duplicates / near-duplicates, flags conflicts
(nullable vs non-nullable, size 5 vs 10, etc.), and returns Included and
Rejected sets.

Per-attribute calls give:
  - Small, focused prompts (faster and higher quality than per-entity).
  - Per-attribute error isolation — one failure does not lose the rest
    of the entity.
  - Natural parallelism if we ever add it.

Output JSON lives at:
  {outdir}/full_cdm/business_rules_consolidated_{domain}_{timestamp}.json

Schema (unchanged — grouped by entity so the Excel tab keeps working):
{
  "generated_date": "ISO",
  "domain": "...",
  "source_cdm": "cdm file name",
  "entities": [
    {
      "entity_name": "...",
      "included": [
        {
          "attribute_name": "...",
          "consolidated_rule": "...",
          "source_rule_ids": [1, 3],
          "sources": ["fhir", "ncpdp"],
          "conflict_type": "NULL | SIZE | TYPE | REQ | OTHER | NONE",
          "conflict_detail": "...",
          "rationale": "..."
        }
      ],
      "rejected": [
        {
          "attribute_name": "...",
          "rule": "...",
          "sources": ["..."],
          "source_rule_id": 2,
          "reason": "duplicate_of_1 | near_duplicate | conflicting | low_quality"
        }
      ]
    }
  ]
}
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.llm_client import LLMClient


SYSTEM_PROMPT = (
    "You are a senior data architect consolidating business rules from "
    "multiple source systems for a Common Data Model. Return ONLY valid "
    "JSON — no markdown, no code fences, no commentary."
)


USER_PROMPT_TEMPLATE = """Consolidate the business rules below for a single CDM attribute.

ENTITY: {entity_name}
ATTRIBUTE: {attribute_name}

Review every rule (each with a numeric id, text, and source systems). Produce
two sets for THIS attribute only:

INCLUDED — one entry per distinct, meaningful rule. When multiple source rules
say the same thing or are near-duplicates, merge them into a single
consolidated rule that preserves the detail (exact field lengths, thresholds,
allowed-value lists, temporal constraints). List the ids of the source rules
that were merged, and the union of their sources.

REJECTED — source rules dropped as duplicates, near-duplicates, or lower-
quality restatements. Reference the id of the rule they were merged into via
reason "duplicate_of_<id>" or "near_duplicate". Conflicting rules should NOT
be rejected — keep them in INCLUDED and flag the conflict.

CONFLICTS — when two source rules disagree (nullable vs non-nullable, max
length 5 vs 10, required vs optional, numeric vs string), keep BOTH in
INCLUDED and set:
  conflict_type: one of "NULL" (nullability), "SIZE" (length/precision),
                 "TYPE" (data type), "REQ" (required vs optional), "OTHER"
  conflict_detail: a brief description of the disagreement (e.g.
                   "fhir says max length 10, ncpdp says max length 15")
Rules without a conflict have conflict_type "NONE" and empty conflict_detail.

Required JSON output — do not change the keys. The attribute_name field must
be "{attribute_name}" for every entry:
{{
  "attribute_name": "{attribute_name}",
  "included": [
    {{
      "attribute_name": "{attribute_name}",
      "consolidated_rule": "...",
      "source_rule_ids": [<int>, ...],
      "sources": ["..."],
      "conflict_type": "NULL|SIZE|TYPE|REQ|OTHER|NONE",
      "conflict_detail": "...",
      "rationale": "short reason for how this was consolidated"
    }}
  ],
  "rejected": [
    {{
      "attribute_name": "{attribute_name}",
      "rule": "...",
      "sources": ["..."],
      "source_rule_id": <int>,
      "reason": "duplicate_of_<id> | near_duplicate | low_quality"
    }}
  ]
}}

RULES FOR {entity_name}.{attribute_name}:
{rules_json}
"""


def _collect_attribute_rules(cdm: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten the CDM into a list of (entity, attribute, rules) units — one
    entry per attribute that has at least one rule. Assigns stable
    numeric ids to each rule so the LLM can reference them.
    """
    units: List[Dict[str, Any]] = []
    next_id = 1
    for entity in cdm.get("entities", []):
        entity_name = entity.get("entity_name", "")
        for attr in entity.get("attributes", []):
            rules: List[Dict[str, Any]] = []
            for r in attr.get("business_rules", []) or []:
                if isinstance(r, dict):
                    text = r.get("rule", "")
                    sources = r.get("sources", []) or []
                else:
                    text = str(r)
                    sources = []
                if not text:
                    continue
                rules.append({"id": next_id, "rule": text, "sources": sources, "type": "business"})
                next_id += 1
            for r in attr.get("validation_rules", []) or []:
                if isinstance(r, dict):
                    text = r.get("rule", "")
                    sources = r.get("sources", []) or []
                else:
                    text = str(r)
                    sources = []
                if not text:
                    continue
                rules.append({"id": next_id, "rule": text, "sources": sources, "type": "validation"})
                next_id += 1
            if rules:
                units.append({
                    "entity_name": entity_name,
                    "attribute_name": attr.get("attribute_name", ""),
                    "rules": rules,
                })
    return units


def _parse_json_response(response: str) -> Dict[str, Any]:
    """Strip code fences (if any) and load as JSON."""
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines and lines[0].strip().lower() in ("```json", "```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return json.loads(text)


def _consolidate_attribute(
    llm: LLMClient,
    entity_name: str,
    attribute_name: str,
    rules: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Call the LLM once for a single attribute and return its consolidation."""
    prompt = USER_PROMPT_TEMPLATE.format(
        entity_name=entity_name,
        attribute_name=attribute_name,
        rules_json=json.dumps(rules, indent=2),
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    response, _ = llm.chat(messages)

    try:
        parsed = _parse_json_response(response)
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse LLM response for {entity_name}.{attribute_name}: {e}")
        return {"included": [], "rejected": [], "parse_error": str(e)}

    included = parsed.get("included", []) or []
    rejected = parsed.get("rejected", []) or []

    # Ensure every entry carries the attribute_name (the LLM sometimes omits it
    # or uses a different casing).
    for item in included:
        item["attribute_name"] = attribute_name
    for item in rejected:
        item["attribute_name"] = attribute_name

    return {"included": included, "rejected": rejected}


def run_rule_consolidation(
    cdm_path: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False,
    max_workers: int = 1,
) -> Optional[Path]:
    """
    Run AI consolidation of business rules, one attribute per LLM call.

    Args:
        cdm_path: Path to the Full CDM JSON.
        outdir: Base output directory (writes to outdir/full_cdm/).
        llm: LLM client. Required when dry_run is False.
        dry_run: If True, write the prompts to disk and skip LLM calls.
        max_workers: Number of concurrent LLM calls. 1 = strictly
            sequential (preserves prior behaviour). Higher values use a
            ThreadPoolExecutor — LLM calls are I/O bound so threads are
            safe.  Safe ceiling depends on OpenAI tier (Tier 4 handles
            16-24 workers comfortably).  Failed calls retry per-thread
            via the existing tenacity retry on LLMClient.chat().

    Returns:
        Path to the consolidation JSON (or the prompts directory in dry-run).
    """
    with open(cdm_path, "r", encoding="utf-8") as f:
        cdm = json.load(f)

    units = _collect_attribute_rules(cdm)
    if not units:
        print("   ℹ️  No business rules found in CDM — nothing to consolidate.")
        return None

    full_cdm_dir = outdir / "full_cdm"
    full_cdm_dir.mkdir(parents=True, exist_ok=True)
    domain = cdm.get("domain", "unknown")
    domain_safe = domain.lower().replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Dry-run: write one prompt file per attribute
    if dry_run:
        prompts_dir = full_cdm_dir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        for unit in units:
            prompt = USER_PROMPT_TEMPLATE.format(
                entity_name=unit["entity_name"],
                attribute_name=unit["attribute_name"],
                rules_json=json.dumps(unit["rules"], indent=2),
            )
            safe = f"{unit['entity_name']}_{unit['attribute_name']}".lower().replace(" ", "_")
            (prompts_dir / f"rule_consolidation_{safe}_{timestamp}.txt").write_text(
                prompt, encoding="utf-8"
            )
        print(f"   ✓ Dry-run: {len(units)} attribute prompts saved to {prompts_dir}")
        return prompts_dir

    if llm is None:
        raise ValueError("LLM client is required when dry_run is False")

    # Count distinct entities touched so the progress log stays meaningful
    unique_entities = {u["entity_name"] for u in units}
    workers_display = max(1, int(max_workers))
    print(
        f"   🤖 Consolidating rules for {len(units)} attributes "
        f"across {len(unique_entities)} entities "
        f"({'sequential' if workers_display == 1 else f'{workers_display} parallel workers'})..."
    )

    # Collect per-attribute results.  We seed the OrderedDict with every
    # entity in the order they appear so the output JSON is deterministic
    # regardless of the order the parallel workers finish in.
    entities_acc: "OrderedDict[str, Dict[str, List[Dict[str, Any]]]]" = OrderedDict()
    for u in units:
        entities_acc.setdefault(u["entity_name"], {"included": [], "rejected": []})

    # Thread-safe progress counter and log lock
    progress_lock = threading.Lock()
    log_lock = threading.Lock()
    completed = {"n": 0}
    total = len(units)

    def _process(unit: Dict[str, Any]) -> Dict[str, Any]:
        """Worker body: run one consolidation, return aggregated result dict."""
        ent = unit["entity_name"]
        attr = unit["attribute_name"]
        try:
            result = _consolidate_attribute(llm, ent, attr, unit["rules"])
        except Exception as e:
            # Isolate per-attribute failures so one bad call doesn't kill the run
            with log_lock:
                print(f"   ⚠️  {ent}.{attr} failed: {e}")
            result = {"included": [], "rejected": [], "error": str(e)}
        with progress_lock:
            completed["n"] += 1
            idx = completed["n"]
        with log_lock:
            print(f"      [{idx}/{total}] {ent}.{attr}")
        return {"entity_name": ent, "result": result}

    if workers_display == 1:
        # Sequential path — preserves prior behaviour exactly
        outputs = [_process(u) for u in units]
    else:
        # Parallel path — I/O bound, threads release GIL on network waits
        outputs = []
        with ThreadPoolExecutor(max_workers=workers_display) as pool:
            futures = [pool.submit(_process, u) for u in units]
            for fut in as_completed(futures):
                outputs.append(fut.result())

    # Aggregate into entities_acc (order-stable via pre-seeding above)
    for item in outputs:
        ent = item["entity_name"]
        result = item["result"]
        bucket = entities_acc[ent]
        bucket["included"].extend(result.get("included", []))
        bucket["rejected"].extend(result.get("rejected", []))

    entity_results = [
        {"entity_name": name, "included": data["included"], "rejected": data["rejected"]}
        for name, data in entities_acc.items()
    ]

    output = {
        "generated_date": datetime.now().isoformat(),
        "domain": domain,
        "source_cdm": cdm_path.name,
        "entities": entity_results,
    }
    output_file = full_cdm_dir / f"business_rules_consolidated_{domain_safe}_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"   ✓ Consolidation saved: {output_file.name}")
    return output_file


def find_consolidated_rules_file(outdir: Path, domain: str) -> Optional[Path]:
    """Locate the most recent consolidated rules JSON for a domain."""
    full_cdm_dir = outdir / "full_cdm"
    if not full_cdm_dir.exists():
        return None
    domain_safe = domain.lower().replace(" ", "_")
    matches = list(full_cdm_dir.glob(f"business_rules_consolidated_{domain_safe}_*.json"))
    if not matches:
        return None
    matches.sort(reverse=True)
    return matches[0]
