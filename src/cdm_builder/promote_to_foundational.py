"""
Promote a rationalized source file to a foundational CDM (anchored mode).

When a source is marked `processing_mode="foundational"`, its rationalized
output IS the user's canonical CDM.  This module performs the
deterministic transform from rationalized schema into the foundational
CDM schema expected by Step 5 — no LLM call required.

Transformations applied:
  * Entity classification defaulted to "Core" when missing.  User can
    pre-set classification on the rationalized entity to override.
  * Surrogate PK added as `{snake_entity}_id` when no attribute carries
    pk=True.
  * Audit columns (`created_at`, `updated_at`) appended when absent.
  * Attribute field names normalized to the keys the foundational CDM
    consumers expect (attribute_name, data_type, pk, required,
    description).
  * Relationships passed through unchanged.

The transform is purely structural.  It does NOT invent business
semantics — anything the rationalized file doesn't carry is filled with
the safest possible default and surfaced in the output for review.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


_AUDIT_COLUMNS = (
    {
        "attribute_name": "created_at",
        "data_type": "TIMESTAMP",
        "required": True,
        "pk": False,
        "description": "Record creation timestamp (audit).",
    },
    {
        "attribute_name": "updated_at",
        "data_type": "TIMESTAMP",
        "required": True,
        "pk": False,
        "description": "Record last-update timestamp (audit).",
    },
)


def _snake(name: str) -> str:
    """Convert PascalCase/CamelCase to snake_case.  Acronym-safe for
    sequences of uppercase letters (e.g. NPIRecord -> npi_record)."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name or "")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def _normalize_attribute(attr: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a rationalized attribute into foundational CDM shape."""
    return {
        "attribute_name": attr.get("attribute_name") or attr.get("name"),
        "data_type": attr.get("data_type") or "VARCHAR(255)",
        "pk": bool(attr.get("pk", False)),
        "required": bool(attr.get("required", False)),
        "nullable": attr.get("nullable", not attr.get("required", False)),
        "description": attr.get("description") or "",
        # Carry through optional metadata that downstream consumers honor.
        **{
            k: attr[k]
            for k in (
                "is_pii", "is_phi", "validation_rules", "business_rules",
                "binding", "source_ref",
            )
            if k in attr
        },
    }


def _ensure_surrogate_pk(entity_name: str, attributes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepend a surrogate `{snake_entity}_id` PK when no PK is present."""
    if any(a.get("pk") for a in attributes):
        return attributes
    surrogate = {
        "attribute_name": f"{_snake(entity_name)}_id",
        "data_type": "BIGINT",
        "pk": True,
        "required": True,
        "nullable": False,
        "description": f"Surrogate primary key for {entity_name}.",
    }
    return [surrogate] + attributes


def _ensure_audit_columns(attributes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Append created_at / updated_at when missing."""
    existing_names = {(a.get("attribute_name") or "").lower() for a in attributes}
    additions = [dict(col) for col in _AUDIT_COLUMNS if col["attribute_name"] not in existing_names]
    return attributes + additions


def _normalize_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a rationalized entity into foundational CDM shape."""
    entity_name = entity.get("entity_name") or "UnnamedEntity"

    attributes = [_normalize_attribute(a) for a in entity.get("attributes", []) if a]
    attributes = _ensure_surrogate_pk(entity_name, attributes)
    attributes = _ensure_audit_columns(attributes)

    return {
        "entity_name": entity_name,
        "description": entity.get("description") or "",
        "classification": entity.get("classification") or "Core",
        "attributes": attributes,
        "relationships": list(entity.get("relationships", []) or []),
    }


def promote(
    rationalized: Dict[str, Any],
    domain: str,
    source_id: str,
) -> Dict[str, Any]:
    """Promote a rationalized payload to a foundational CDM dict.

    Args:
        rationalized: Parsed JSON of a rationalized source file.
        domain: CDM domain name.
        source_id: source_id of the foundational source (recorded in
            metadata for traceability).

    Returns:
        Foundational CDM dict ready to be saved as
        cdm/cdm_foundational_{domain}_{timestamp}.json.
    """
    raw_entities = rationalized.get("entities") or rationalized.get("rationalized_entities") or []
    entities = [_normalize_entity(e) for e in raw_entities if e]

    return {
        "domain": domain,
        "cdm_version": "1.0",
        "generated_timestamp": datetime.now().isoformat(),
        "source_files": {"foundational": source_id},
        "anchored": True,
        "entities": entities,
    }


def promote_file(
    rationalized_path: Path,
    domain: str,
    source_id: str,
    outdir: Path,
) -> Path:
    """Convenience wrapper: read rationalized file, promote, write foundational CDM.

    Returns the path to the written foundational CDM file.
    """
    with open(rationalized_path, "r", encoding="utf-8") as f:
        rationalized = json.load(f)

    cdm = promote(rationalized, domain=domain, source_id=source_id)

    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = domain.lower().replace(" ", "_")
    # Naming convention is cdm_{domain}_{module}_{ts}.json so that
    # find_latest_foundational_cdm's glob (`cdm_{domain}_*.json`) picks
    # this file up.  The earlier `cdm_foundational_{domain}_*` form was
    # silently invisible to discovery, causing Step 5 to fall back to
    # whatever older `cdm_{domain}_*` file was lying around in the
    # directory.
    out_path = outdir / f"cdm_{domain_safe}_foundational_{timestamp}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cdm, f, indent=2)
    return out_path
