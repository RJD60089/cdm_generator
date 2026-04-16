# src/cdm_builder/build_ancillary_prefoundation.py
"""
Ancillary Pre-Foundation: Build Preliminary CDM from Ancillary Sources

When ancillary sources are configured in Driver mode, this module generates
a compact preliminary CDM from the ancillary data BEFORE the main foundational
CDM prompt runs. The preliminary CDM (entities, attributes, relationships)
is then injected as a structural scaffold into the foundational prompt.

This two-pass approach avoids:
1. Blowing up the foundational prompt context with raw DDL data
2. Disrupting the highly-tuned foundational prompt structure

The preliminary CDM is lightweight — no standards alignment, no business
rules — just structural extraction from the ancillary schema.

Usage:
    Called from orchestrator before run_step3a() when Driver-mode
    ancillary sources exist.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


def build_prefoundation_prompt(config: AppConfig, ancillary_data: Dict) -> str:
    """Build prompt to generate a preliminary CDM from ancillary source data.

    This is a lightweight CDM generation — entities, attributes, relationships
    derived from the ancillary schema. No standards alignment, no complex
    business rules — just structural extraction.

    Args:
        config: AppConfig with domain context
        ancillary_data: Rationalized ancillary data (standard template format)

    Returns:
        Complete prompt string
    """
    entities = ancillary_data.get("entities", [])

    # Build a compact representation of the ancillary entities
    compact_entities = []
    for entity in entities:
        compact_entity = {
            "entity_name": entity.get("entity_name"),
            "description": entity.get("description"),
            "attributes": []
        }
        for attr in entity.get("attributes", []):
            compact_attr = {
                "attribute_name": attr.get("attribute_name"),
                "data_type": attr.get("data_type"),
                "required": attr.get("required"),
                "description": attr.get("description"),
            }
            compact_attr = {k: v for k, v in compact_attr.items() if v is not None}
            compact_entity["attributes"].append(compact_attr)
        compact_entities.append(compact_entity)

    ancillary_json = json.dumps(compact_entities, indent=2)

    prompt = f"""You are a senior data architect. Your task is to generate a preliminary
Conceptual Data Model (CDM) from an ancillary source system schema.

=============================================================================
DOMAIN CONTEXT
=============================================================================

DOMAIN: {config.cdm.domain}
TYPE: {config.cdm.type}
DESCRIPTION: {config.cdm.description}

=============================================================================
YOUR TASK
=============================================================================

Analyze the ancillary source entities below and generate a preliminary CDM
that captures the structural essence of this source system. This preliminary
model will be used as scaffolding for a more complete CDM generation step.

Focus on:
1. Identifying core business entities (not audit/logging tables)
2. Normalizing entity and attribute names to PascalCase / snake_case
3. Identifying primary keys, foreign keys, and relationships
4. Grouping related tables into logical entities
5. Removing pure technical columns (audit timestamps, ETL flags, etc.)
   unless they carry business meaning

Do NOT:
- Add entities not present in the source
- Apply industry standards (FHIR, NCPDP) — that happens later
- Over-consolidate: keep entities distinct if they represent different concepts

=============================================================================
ANCILLARY SOURCE ENTITIES ({len(compact_entities)} entities)
=============================================================================

{ancillary_json}

=============================================================================
OUTPUT FORMAT
=============================================================================

Return ONLY valid JSON matching this structure:

{{
  "domain": "{config.cdm.domain}",
  "cdm_version": "0.1-prefoundation",
  "entities": [
    {{
      "entity_name": "EntityName",
      "description": "Business description derived from source",
      "classification": "Core|Reference|Junction",
      "attributes": [
        {{"name": "entity_name_id", "type": "INTEGER", "pk": true, "required": true, "description": "Surrogate primary key"}},
        {{"name": "attr_name", "type": "VARCHAR(50)", "pk": false, "required": true, "description": "..."}}
      ],
      "relationships": [
        {{"to": "OtherEntity", "type": "M:1", "fk": "other_entity_id", "description": "..."}}
      ]
    }}
  ]
}}

RELATIONSHIP FORMAT — MANDATORY:
Every relationship entry MUST contain these exact keys:
  - "to": the target entity name (e.g., "RemittanceRequest")
  - "type": cardinality using ONLY "M:1", "1:M", "M:N", or "1:1"
  - "fk": the foreign key attribute name on THIS entity (e.g., "remittance_request_id")
  - "description": brief description of the relationship
  - ONLY use these key names, DO NOT SUBSTITUTE other key names

Return ONLY the JSON. No explanation, no markdown code blocks."""

    return prompt


def find_latest_prefoundation(outdir: Path, domain: str, source_id: Optional[str] = None) -> Optional[Path]:
    """Find the latest ancillary prefoundation file.

    Args:
        outdir: CDM output directory
        domain: CDM domain name
        source_id: If provided, find prefoundation for this specific source.
                   If None, find any ancillary prefoundation.
    """
    domain_safe = domain.lower().replace(" ", "_")
    if source_id:
        pattern = f"ancillary_prefoundation_{source_id}_{domain_safe}_*.json"
    else:
        pattern = f"ancillary_prefoundation_*_{domain_safe}_*.json"
    matches = sorted(outdir.glob(pattern), reverse=True)
    return matches[0] if matches else None


def run_ancillary_prefoundation(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False,
    rationalized_dir: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Generate preliminary CDM from Driver-mode ancillary sources.

    Processes each Driver-mode ancillary source independently and merges
    all prefoundation results into a single combined scaffold.

    Args:
        config: AppConfig with domain and ancillary configuration
        outdir: Output directory for CDM files
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
        rationalized_dir: Directory containing rationalized files

    Returns:
        Combined preliminary CDM dict (None in dry run or if no data)
    """
    driver_entries = config.get_ancillary_by_mode("driver")
    if not driver_entries:
        return None

    if rationalized_dir is None:
        rationalized_dir = outdir.parent / "rationalized"

    domain_safe = config.cdm.domain.replace(" ", "_")
    combined_entities = []

    for entry in driver_entries:
        source_id = entry.get("source_id", "ancillary")
        print(f"\n   Building pre-foundation for {source_id}...")

        # Find rationalized file for this specific source_id
        pattern = f"rationalized_{source_id}_{domain_safe}*.json"
        matches = sorted(rationalized_dir.glob(pattern), reverse=True)

        if not matches:
            print(f"      No rationalized file found for {source_id}. Skipping.")
            continue

        ancillary_file = matches[0]
        print(f"      Source: {ancillary_file.name}")

        with open(ancillary_file, "r", encoding="utf-8") as f:
            ancillary_data = json.load(f)

        entity_count = len(ancillary_data.get("entities", []))
        if entity_count == 0:
            print(f"      No entities. Skipping.")
            continue

        print(f"      Entities: {entity_count}")

        # Build prompt
        prompt = build_prefoundation_prompt(config, ancillary_data)

        # Ensure output directory exists
        outdir.mkdir(parents=True, exist_ok=True)

        if dry_run:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            prompts_dir = outdir / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            output_file = prompts_dir / f"ancillary_prefoundation_{source_id}_{timestamp}.txt"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(prompt)
            print(f"      Prompt saved: {output_file.name}")
            continue

        # Call LLM
        print(f"      Calling LLM...")
        print(f"      Prompt size: {len(prompt):,} chars (~{len(prompt) // 4:,} tokens)")

        messages = [
            {
                "role": "system",
                "content": "You are a senior data architect. Return ONLY valid JSON.",
            },
            {"role": "user", "content": prompt},
        ]

        try:
            response, _ = llm.chat(messages)

            response_clean = response.strip()
            if response_clean.startswith("```"):
                lines = response_clean.split("\n")
                if lines[0].strip().lower() in ("```json", "```"):
                    response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean

            prefoundation = json.loads(response_clean)

            if "entities" not in prefoundation:
                raise ValueError("Response missing 'entities' key")

            # Save per-source prefoundation
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = outdir / f"ancillary_prefoundation_{source_id}_{domain_safe}_{timestamp}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(prefoundation, f, indent=2)

            pf_entity_count = len(prefoundation.get("entities", []))
            print(f"      Pre-foundation: {pf_entity_count} entities -> {output_file.name}")

            # Accumulate for combined scaffold
            combined_entities.extend(prefoundation.get("entities", []))

        except json.JSONDecodeError as e:
            print(f"      ERROR: Failed to parse LLM response: {e}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_file = outdir / f"ancillary_prefoundation_error_{source_id}_{timestamp}.txt"
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(response)
            print(f"      Full response saved to: {error_file}")
            continue

        except ValueError as e:
            print(f"      ERROR: {e}")
            continue

    # Return combined scaffold from all driver sources
    if not combined_entities:
        if dry_run:
            return None
        print(f"\n   No pre-foundation entities produced.")
        return None

    combined = {
        "domain": config.cdm.domain,
        "cdm_version": "0.1-prefoundation",
        "generated_date": datetime.now().isoformat(),
        "generator": "ancillary_prefoundation",
        "entities": combined_entities,
    }

    print(f"\n   Combined pre-foundation: {len(combined_entities)} entities from {len(driver_entries)} source(s)")
    return combined
