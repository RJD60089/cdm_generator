"""
EDW configuration generator for CDM.

AI selects relevant EDW entity IDs from the catalog based on CDM domain
and description. No index file required -- fully CDM-agnostic.

Each catalog file is a lightweight entity summary:
  entity_id, source_table, field_count, sample field names, domain_hints

AI selects which entities belong to the CDM being built.
Result goes into config input_files.edw as a list of entity IDs.

Follows the same pattern as config_gen_fhir.py:
  - Inherits ConfigGeneratorBase
  - Uses self.call_llm() / self.parse_ai_json_response()
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import config_utils
from .config_gen_core import ConfigGeneratorBase


CATALOG_DIR_RELATIVE = "input/edw_catalog"


class EDWConfigGenerator(ConfigGeneratorBase):
    """AI-driven EDW entity selection for CDM configuration."""

    def __init__(self, cdm_name: str, llm_client=None):
        super().__init__(cdm_name, llm_client)
        self.catalog_dir = self.project_root / CATALOG_DIR_RELATIVE

    # -------------------------------------------------------------------------
    # Catalog loading
    # -------------------------------------------------------------------------

    def _load_entity_summaries(self) -> List[Dict[str, Any]]:
        """
        Load lightweight summary of every entity in the EDW catalog.

        Each summary contains:
          entity_id, source_table, field_count, sample_fields (first 10),
          domain_hints (engineer-supplied tags, used as guidance not rules).

        Full field data is NOT included -- keeps the selection prompt lean.
        Full data is loaded later during rationalization.
        """
        summaries: List[Dict[str, Any]] = []

        if not self.catalog_dir.exists():
            print(f"   ⚠️  EDW catalog directory not found: {self.catalog_dir}")
            return summaries

        for catalog_file in sorted(self.catalog_dir.glob("edw_*.json")):
            # Skip the source-to-target file variant.  Each EDW entity has
            # up to two files in the catalog directory:
            #   edw_<id>.json                       (catalog — table layout)
            #   edw_<id> - source to target.json    (s2t — full lineage)
            # Only the canonical catalog filename should appear in the
            # entity selection list; the rationalizer's _load_entity_file()
            # prefers the s2t variant at load time when both exist.  Without
            # this skip, every entity is enumerated TWICE (once with a
            # mangled "<ID> - SOURCE TO TARGET" entity_id) and the LLM
            # selects both as if they were distinct entities.
            if catalog_file.stem.lower().endswith(" - source to target"):
                continue
            try:
                wrapper = json.loads(catalog_file.read_text(encoding="utf-8"))
                entity = wrapper.get("entity", wrapper)

                entity_id    = entity.get("entity_id",
                                          catalog_file.stem.replace("edw_", "").upper())
                src_table    = entity.get("source_table", entity_id)
                fields       = entity.get("fields", [])

                # Business fields only -- exclude SCD2 meta and derived columns
                biz_fields   = [f for f in fields
                                 if not f.get("is_derived") and not f.get("is_scd2_meta")]
                # Sample field names for semantic context -- NP canonical name preferred
                sample: List[str] = []
                for f in biz_fields[:10]:
                    name = (f.get("np_column") or f.get("ni_column")
                            or f.get("source_column") or "")
                    if name:
                        sample.append(name.lower())

                summaries.append({
                    "entity_id":     entity_id,
                    "source_table":  src_table,
                    "field_count":   len(biz_fields),
                    "sample_fields": sample,
                })

            except Exception as exc:
                print(f"   ⚠️  Could not load {catalog_file.name}: {exc}")

        return summaries

    # -------------------------------------------------------------------------
    # Prompt
    # -------------------------------------------------------------------------

    def _build_selection_prompt(self, config: Dict,
                                summaries: List[Dict[str, Any]]) -> str:
        cdm         = config.get("cdm", {})
        domain      = cdm.get("domain", "")
        description = cdm.get("description", "")
        cdm_type    = cdm.get("type", "Core")

        return f"""You are a data architect selecting EDW source entities for a CDM configuration.

# CDM Context

- **Domain**: {domain}
- **Type**: {cdm_type}
- **Description**: {description}

The CDM's scope is defined by the **Description** above, not by the domain
name alone. Treat the Description's in-scope concepts and any explicit
Includes/Excludes language as the authoritative scope.

# EDW Entity Summaries ({len(summaries)} entities)

{json.dumps(summaries, indent=2)}

# Selection Task

Select the EDW entities whose fields directly support the CDM scope as
established by the Description above.

# Selection Rules

1. **Match against the Description, not the domain name** - Each selected
   entity MUST contain data that directly supports an entity, attribute, or
   relationship that the Description establishes as in-scope. The domain
   name alone is insufficient grounds for selection.

2. **Ground every decision in the Description** - For both selections and
   exclusions, the rationale MUST cite the specific Description language
   (in-scope concepts or out-of-scope language) that drives the decision.

3. **Select only what is required** - Build the smallest set that fully
   satisfies the Description's scope. Each selected entity MUST trace to a
   specific in-scope concept stated in the Description; entities that do
   not trace to a Description concept belong in the excluded list.

# Output Format

Return ONLY valid JSON -- no markdown, no code blocks:

{{
  "selected_entities": ["ENTITY_ID_1", "ENTITY_ID_2"],
  "excluded_entities": [
    {{"entity_id": "ENTITY_ID", "reason": "brief reason citing Description"}}
  ],
  "selection_rationale": "Brief explanation grounded in Description scope."
}}

Return JSON only:"""

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def run_analysis(self, config: Dict, dry_run: bool = False) -> Dict:
        """
        Run AI-driven EDW entity selection.

        Args:
            config: Config dict containing cdm.domain, cdm.description, cdm.type
            dry_run: If True, save prompt to file but do not call LLM

        Returns:
            Dict with keys:
              'edw'               - list of selected entity IDs (strings)
              'domain_assessment' - AI exclusions + rationale (for config metadata)
        """
        print("\n🤖 EDW Entity Selection")

        summaries = self._load_entity_summaries()

        if not summaries:
            print("   No EDW catalog entities found -- skipping EDW selection")
            return {"edw": []}

        print(f"   Catalog entities available: {len(summaries)}")

        prompt = self._build_selection_prompt(config, summaries)

        # ── Dry run: save prompt, return empty ──────────────────────────────
        if dry_run:
            prompts_dir = self.config_dir / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            prompt_file = prompts_dir / f"edw_selection_{self.safe_name}.txt"
            prompt_file.write_text(prompt, encoding="utf-8")
            print(f"   Prompt saved: {prompt_file.name}")
            return {"edw": [], "domain_assessment": {}}

        # ── Live: call LLM via base class helper ─────────────────────────────
        try:
            print(f"   Prompt: ~{len(prompt)//4:,} tokens")
            response_text = self.call_llm(prompt)
            result        = self.parse_ai_json_response(response_text)

            selected  = result.get("selected_entities", [])
            excluded  = result.get("excluded_entities", [])
            rationale = result.get("selection_rationale", "")

            print(f"   ✓ Selected: {len(selected)} entities")
            for eid in selected:
                print(f"     + {eid}")

            if excluded:
                print(f"   ✗ Excluded: {len(excluded)}")
                for e in excluded[:5]:
                    print(f"     - {e['entity_id']}: {e.get('reason', '')}")
                if len(excluded) > 5:
                    print(f"     ... and {len(excluded) - 5} more")

            if rationale:
                print(f"   Rationale: {rationale[:200]}")

            return {
                "edw": selected,
                "domain_assessment": {
                    "excluded_entities": excluded,
                    "selection_rationale": rationale,
                },
            }

        except json.JSONDecodeError as exc:
            print(f"   ❌ JSON parse error: {exc}")
            return {"edw": []}
        except Exception as exc:
            print(f"   ❌ Error: {exc}")
            import traceback; traceback.print_exc()
            return {"edw": []}