# src/artifacts/common/rule_consolidator.py
"""
AI-driven business-rule consolidation.

Reads business + validation rules from a Full CDM, groups by entity, then
asks the LLM to consolidate per entity: eliminate duplicates and near-
duplicates, flag conflicts (nullable vs non-nullable, size 5 vs 10, etc.),
and produce an Included set (consolidated rules) and a Rejected set
(duplicates or lower-quality versions) with rationale.

Output JSON lives at:
  {outdir}/full_cdm/business_rules_consolidated_{domain}_{timestamp}.json

Schema:
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
          "conflict_type": "NULL | SIZE | TYPE | REQ | NONE",
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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.llm_client import LLMClient


SYSTEM_PROMPT = (
    "You are a senior data architect consolidating business rules from "
    "multiple source systems for a Common Data Model. Return ONLY valid "
    "JSON — no markdown, no code fences, no commentary."
)


USER_PROMPT_TEMPLATE = """Consolidate the business rules below for entity: {entity_name}

For each attribute, review every rule (each with a numeric id, text, and
source systems). Produce two sets:

INCLUDED — one entry per distinct, meaningful rule. If multiple source rules
say the same thing or are near-duplicates, merge them into a single
consolidated rule that preserves the detail (exact field lengths, thresholds,
allowed-value lists, temporal constraints). List the ids of the source rules
that were merged, and the union of their sources.

REJECTED — source rules that were dropped as duplicates, near-duplicates, or
lower-quality restatements. Reference the id of the rule they were merged
into via reason "duplicate_of_<id>" or "near_duplicate". Conflicting rules
should NOT be rejected — keep them in INCLUDED and flag the conflict.

CONFLICTS — when two source rules on the SAME attribute disagree (e.g.,
nullable vs non-nullable, max length 5 vs 10, required vs optional, numeric
vs string), keep BOTH in INCLUDED and set:
  conflict_type: one of "NULL" (nullability), "SIZE" (length/precision),
                 "TYPE" (data type), "REQ" (required vs optional), "OTHER"
  conflict_detail: a brief description of the disagreement (e.g.
                   "fhir says max length 10, ncpdp says max length 15")
Rules without a conflict have conflict_type "NONE" and empty conflict_detail.

Required JSON output — do not change the keys:
{{
  "entity_name": "{entity_name}",
  "included": [
    {{
      "attribute_name": "...",
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
      "attribute_name": "...",
      "rule": "...",
      "sources": ["..."],
      "source_rule_id": <int>,
      "reason": "duplicate_of_<id> | near_duplicate | low_quality"
    }}
  ]
}}

RULES FOR ENTITY {entity_name}:
{rules_json}
"""


def _collect_entity_rules(cdm: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Group business + validation rules per entity, with stable numeric ids
    so the LLM can reference them.
    """
    grouped: List[Dict[str, Any]] = []
    next_id = 1
    for entity in cdm.get("entities", []):
        entity_name = entity.get("entity_name", "")
        attrs_out = []
        for attr in entity.get("attributes", []):
            rules = []
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
                attrs_out.append({
                    "attribute_name": attr.get("attribute_name", ""),
                    "rules": rules,
                })
        if attrs_out:
            grouped.append({"entity_name": entity_name, "attributes": attrs_out})
    return grouped


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


def _consolidate_entity(
    llm: LLMClient,
    entity_name: str,
    attributes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Call the LLM once for a single entity."""
    prompt = USER_PROMPT_TEMPLATE.format(
        entity_name=entity_name,
        rules_json=json.dumps(attributes, indent=2),
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    response, _ = llm.chat(messages)
    try:
        parsed = _parse_json_response(response)
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse LLM response for {entity_name}: {e}")
        return {"entity_name": entity_name, "included": [], "rejected": [], "parse_error": str(e)}

    parsed.setdefault("entity_name", entity_name)
    parsed.setdefault("included", [])
    parsed.setdefault("rejected", [])
    return parsed


def run_rule_consolidation(
    cdm_path: Path,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False,
) -> Optional[Path]:
    """
    Run AI consolidation of business rules for every entity in the Full CDM.

    Args:
        cdm_path: Path to the Full CDM JSON.
        outdir: Base output directory (writes to outdir/full_cdm/).
        llm: LLM client. Required when dry_run is False.
        dry_run: If True, write the prompts to disk and skip LLM calls.

    Returns:
        Path to the consolidation JSON (or the prompts directory when dry_run).
    """
    with open(cdm_path, "r", encoding="utf-8") as f:
        cdm = json.load(f)

    grouped = _collect_entity_rules(cdm)
    if not grouped:
        print("   ℹ️  No business rules found in CDM — nothing to consolidate.")
        return None

    full_cdm_dir = outdir / "full_cdm"
    full_cdm_dir.mkdir(parents=True, exist_ok=True)
    domain = cdm.get("domain", "unknown")
    domain_safe = domain.lower().replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if dry_run:
        prompts_dir = full_cdm_dir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        for group in grouped:
            prompt = USER_PROMPT_TEMPLATE.format(
                entity_name=group["entity_name"],
                rules_json=json.dumps(group["attributes"], indent=2),
            )
            safe_entity = group["entity_name"].lower().replace(" ", "_")
            (prompts_dir / f"rule_consolidation_{safe_entity}_{timestamp}.txt").write_text(
                prompt, encoding="utf-8"
            )
        print(f"   ✓ Dry-run: {len(grouped)} consolidation prompts saved to {prompts_dir}")
        return prompts_dir

    if llm is None:
        raise ValueError("LLM client is required when dry_run is False")

    print(f"   🤖 Consolidating rules for {len(grouped)} entities...")
    entity_results: List[Dict[str, Any]] = []
    for i, group in enumerate(grouped, 1):
        entity_name = group["entity_name"]
        print(f"      [{i}/{len(grouped)}] {entity_name}")
        entity_results.append(
            _consolidate_entity(llm, entity_name, group["attributes"])
        )

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
