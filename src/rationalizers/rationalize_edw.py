# src/rationalizers/rationalize_edw.py
"""
EDW Rationalization Module — Three-Pass Design

Pass 1 — Entity Description (AI, one call per entity)
    AI receives table name, field names/lineage, and returns entity_name,
    description, business_context, technical_context.  No attributes.

Pass 2 — Attribute Extraction (programmatic, no AI)
    For each entity, extract all business fields (exclude is_derived=True,
    is_scd2_meta=True) from the catalog/source-to-target file.
    Build structured attribute records: source→NI→NP lineage, data type
    (normalized), nullable, pk_order, transformation_note.

Pass 3 — Attribute Enrichment (AI, one call per entity)
    Send all extracted business fields to AI.  AI returns description,
    business_context, is_pii, is_phi, and data_classification for each.
    Results are merged back into the attribute records programmatically.

Outputs
-------
rationalized_edw_entities_{domain}_{ts}.json   — entities only (Pass 1)
    Used by Step 3 (Build Foundational CDM) as a REFERENCE source.
    CDM entities are NOT modeled after EDW tables; they inform scope.

rationalized_edw_{domain}_{ts}.json            — entities + attributes (P1+2+3)
    Used by Step 6 (Build Full CDM) for attribute mapping.

File loading preference (per entity):
    edw_{id} - source to target.json   (preferred — full lineage)
    edw_{id}.json                       (fallback — table layout only)
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data type normalisation
# ---------------------------------------------------------------------------

_DT_MAP = [
    (r"^varchar2\s*\(\d+\s*(byte|char)?\)$",   "varchar"),
    (r"^varchar2$",                             "varchar"),
    (r"^varchar\s*\(\d+\)$",                   "varchar"),
    (r"^char\s*\(\d+\)$",                      "char"),
    (r"^nvarchar2\s*\(\d+\)$",                 "varchar"),
    (r"^number\s*\(\d+\)$",                    "integer"),
    (r"^number$",                               "decimal"),
    (r"^integer$",                              "integer"),
    (r"^int$",                                  "integer"),
    (r"^bigint$",                               "bigint"),
    (r"^smallint$",                             "smallint"),
    (r"^float(\s*\(\d+\))?$",                  "float"),
    (r"^date$",                                 "date"),
    (r"^timestamp(\s*\(\d+\))?(\s*with.*)?$",  "timestamp"),
    (r"^clob$",                                 "text"),
    (r"^blob$",                                 "binary"),
    (r"^boolean$",                              "boolean"),
    (r"^raw\s*\(\d+\)$",                        "binary"),
]


def _normalize_data_type(raw: Optional[str]) -> str:
    """Normalise Oracle/SQL data type to lowercase portable equivalent."""
    if not raw:
        return "varchar"
    s = raw.strip().lower()
    # NUMBER(p,s) -> decimal(p,s)
    m = re.match(r"^number\s*\((\d+),\s*(\d+)\)$", s)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})"
    for pattern, result in _DT_MAP:
        if re.match(pattern, s):
            return result if result else s
    return s  # unknown — pass through


def _attribute_name_from_np(np_col: Optional[str], ni_col: Optional[str] = None) -> str:
    """Derive attribute_name from np_column.

    Strips:
      1. NP_/NI_ layer prefix  (NP_F201_SUBMIT_PHARM_NUM -> F201_SUBMIT_PHARM_NUM)
      2. NCPDP/EDW field code prefix WITH underscore separator:
           F201_SUBMIT_PHARM_NUM  -> submit_pharm_num
           UN001_OTHER_PAYOR_AMT  -> other_payor_amt
           UT006_SOMETHING        -> something
      3. Bare codes with no suffix (F455, UN006, UT007) are returned as-is
         in lowercase — the cross-entity resolution pass substitutes the best
         known descriptive name if one exists in another entity.
    """
    col = np_col or ni_col or ""
    # Step 1: strip NP_/NI_ layer prefix
    name = re.sub(r"^(NP_|NI_)", "", col, flags=re.IGNORECASE)
    # Step 2: strip F/UN/UT field code prefix (requires underscore — bare codes
    #         are left as-is for cross-entity resolution to handle)
    name = re.sub(r"^(F|UN|UT)\d+_", "", name, flags=re.IGNORECASE)
    return name.lower()






# ---------------------------------------------------------------------------
# Pass 4 — cross-entity bare F-code name resolution
# ---------------------------------------------------------------------------

_BARE_CODE_RE = re.compile(r"^(f|un|ut)\d+$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class EDWRationalizer:
    """
    Three-pass EDW rationalization.
    Independent per entity — no accumulated state.
    """

    CATALOG_DIR = Path("input/edw_catalog")

    def __init__(
        self,
        config_path: str,
        llm: Optional[Any] = None,
        dry_run: bool = False,
    ):
        self.llm     = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None

        with open(config_path, encoding="utf-8") as fh:
            self.config = json.load(fh)

        cdm = self.config.get("cdm", {})
        self.cdm_domain:      str = cdm.get("domain", "")
        self.cdm_type:        str = cdm.get("type", "Core")
        self.cdm_description: str = cdm.get("description", "")

        self.entity_ids: List[str] = self.config.get("input_files", {}).get("edw", [])

        print(f"  Config:   {config_path}")
        print(f"  Domain:   {self.cdm_domain}")
        print(f"  Entities: {len(self.entity_ids)}")

    # -------------------------------------------------------------------------
    # File loading
    # -------------------------------------------------------------------------

    def _load_entity_file(self, entity_id: str) -> Optional[Tuple[Dict[str, Any], str]]:
        """
        Load entity data.  Prefer source-to-target, fall back to catalog.
        Strips cdm_domains at entity and field level to avoid anchoring AI.
        Returns (entity_dict, file_type) or None.
        """
        eid_lower = entity_id.lower()

        s2t_path = self.CATALOG_DIR / f"edw_{eid_lower} - source to target.json"
        cat_path = self.CATALOG_DIR / f"edw_{eid_lower}.json"

        if s2t_path.exists():
            path, file_type = s2t_path, "source_to_target"
        elif cat_path.exists():
            path, file_type = cat_path, "catalog"
        else:
            print(f"    WARNING: No file found for entity: {entity_id}")
            print(f"      Tried: {s2t_path.name}")
            print(f"      Tried: {cat_path.name}")
            return None

        print(f"    File: {path.name} ({file_type})")

        with open(path, encoding="utf-8") as fh:
            wrapper = json.load(fh)

        entity = wrapper.get("entity", wrapper)

        # Strip cdm_domains to avoid anchoring
        entity.pop("cdm_domains", None)
        for field in entity.get("fields", []):
            field.pop("cdm_domains", None)

        return entity, file_type

    # -------------------------------------------------------------------------
    # Pass 1 — entity description (AI)
    # -------------------------------------------------------------------------

    @staticmethod
    def _business_fields(entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return fields that are not derived and not SCD2 metadata."""
        return [
            f for f in entity.get("fields", [])
            if not f.get("is_derived") and not f.get("is_scd2_meta")
        ]

    def _build_pass1_prompt(self, entity: Dict[str, Any]) -> str:
        entity_id  = entity.get("entity_id", "")
        src_table  = entity.get("source_table", entity_id)
        src_schema = entity.get("source_schema", "")
        src_db     = entity.get("source_database", "")
        ni_table   = entity.get("ni_table", "")
        np_table   = entity.get("np_table", "")
        proc_notes = entity.get("processing_notes", [])

        biz_fields = self._business_fields(entity)

        field_summary = []
        for f in biz_fields:
            entry: Dict[str, Any] = {
                "source_column": f.get("source_column"),
                "ni_column":     f.get("ni_column"),
                "np_column":     f.get("np_column"),
                "data_type":     f.get("data_type"),
                "pk_order":      f.get("pk_order"),
            }
            note = (f.get("transformation_note") or "")[:200]
            if note:
                entry["transformation_note"] = note
            field_summary.append(entry)

        return (
            f"You are a data architect documenting an EDW source table for a "
            f"Pharmacy Benefits Management (PBM) organization.\n\n"
            f"This organization uses a pass-through pricing model (not spread pricing).\n\n"
            f"## CDM CONTEXT\n\n"
            f"Domain:      {self.cdm_domain}\n"
            f"Type:        {self.cdm_type}\n"
            f"Description: {self.cdm_description}\n\n"
            f"## SOURCE TABLE\n\n"
            f"Entity ID:        {entity_id}\n"
            f"Source Table:     {src_table}\n"
            f"Source Schema:    {src_schema}\n"
            f"Source DB:        {src_db}\n"
            f"NI Table:         {ni_table}\n"
            f"NP Table:         {np_table}\n"
            f"Business Fields:  {len(biz_fields)}\n"
            f"Processing Notes: {json.dumps(proc_notes) if proc_notes else 'none'}\n\n"
            f"## BUSINESS FIELDS (source -> NI -> NP lineage)\n\n"
            f"{json.dumps(field_summary, indent=2)}\n\n"
            f"## TASK\n\n"
            f"Based on the table name, field names, and lineage above, provide:\n\n"
            f"1. entity_name - NP table name without NP_ prefix, PascalCase\n"
            f"   (e.g. NP_PAIDHISTORY -> PaidHistory)\n\n"
            f"2. description - Clear, concise business description of what this table\n"
            f"   represents and what data it contains (2-4 sentences).\n\n"
            f"3. business_context - How this table is used in the {self.cdm_domain} domain.\n"
            f"   What business process does it support? What questions can it answer?\n\n"
            f"4. technical_context - Key technical notes: SCD2 pattern if present,\n"
            f"   primary key structure, any notable transformation patterns.\n\n"
            f"## OUTPUT FORMAT\n\n"
            f"Return ONLY valid JSON, no markdown, no code blocks:\n\n"
            f"{{\n"
            f'  "entity_name": "<PascalCase name>",\n'
            f'  "description": "<business description>",\n'
            f'  "business_context": "<how used in {self.cdm_domain}>",\n'
            f'  "technical_context": "<technical notes>"\n'
            f"}}\n"
        )

    # -------------------------------------------------------------------------
    # Pass 2 — programmatic attribute extraction (no AI)
    # -------------------------------------------------------------------------

    def _extract_attributes(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Pass 2: programmatic attribute extraction.
        Builds a structured attribute record for each business field.
        No AI involved.

        Deduplication: when two fields share the same np_column (multiple NI
        sources mapping to one NP column), keep the entry that has a
        source_column value.  If both or neither have a source_column, keep
        the first occurrence.
        """
        scd2_cols  = {c.lower() for c in entity.get("scd2_columns", [])}
        seen_names: Dict[str, int] = {}   # attr_name -> index in attributes list
        attributes: List[Dict[str, Any]] = []

        for field in self._business_fields(entity):
            src_col  = field.get("source_column")
            ni_col   = field.get("ni_column")
            np_col   = field.get("np_column")
            raw_type = field.get("data_type", "")
            pk_order = field.get("pk_order")
            nullable = field.get("nullable", True)
            t_note   = field.get("transformation_note", "")

            attr_name = _attribute_name_from_np(np_col, ni_col)
            norm_type = _normalize_data_type(raw_type)

            # Extract length / precision / scale from raw type
            length: Optional[int]    = None
            precision: Optional[int] = None
            scale: Optional[int]     = None

            m_vc = re.match(r"varchar2?\s*\((\d+)", raw_type or "", re.IGNORECASE)
            if m_vc:
                length = int(m_vc.group(1))

            m_num = re.match(r"number\s*\((\d+),\s*(\d+)\)", raw_type or "", re.IGNORECASE)
            if m_num:
                precision, scale = int(m_num.group(1)), int(m_num.group(2))
            else:
                m_num2 = re.match(r"number\s*\((\d+)\)", raw_type or "", re.IGNORECASE)
                if m_num2:
                    precision = int(m_num2.group(1))

            business_rules = []
            if t_note and t_note.strip():
                business_rules.append(t_note.strip())

            attr: Dict[str, Any] = {
                "attribute_name":       attr_name,
                "description":          None,           # filled by Pass 3
                "data_type":            norm_type,
                "source_attribute":     [src_col] if src_col else [],
                "source_files":         [],              # stamped in _build_entity_record
                "required":             (pk_order is not None) or (not nullable),
                "nullable":             nullable,
                "cardinality":          {"min": 0 if nullable else 1, "max": "1"},
                "length":               length,
                "precision":            precision,
                "scale":                scale,
                "default_value":        None,
                "is_array":             False,
                "is_nested":            False,
                "is_pii":               None,           # filled by Pass 3
                "is_phi":               None,           # filled by Pass 3
                "data_classification":  None,           # filled by Pass 3
                "business_context":     None,           # filled by Pass 3
                "business_rules":       business_rules,
                "validation_rules":     [],
                "is_calculated":        field.get("is_derived", False),
                "calculation_dependency": None,
                "source_metadata": {
                    "source_column": src_col,
                    "ni_column":     ni_col,
                    "np_column":     np_col,
                    "raw_data_type": raw_type,
                    "pk_order":      pk_order,
                    "is_scd2":       (np_col or "").lower() in scd2_cols,
                },
            }
            # Dedup: if seen this attr_name, prefer the record with a source_column
            if attr_name in seen_names:
                existing_idx = seen_names[attr_name]
                existing = attributes[existing_idx]
                if not existing["source_metadata"].get("source_column") and src_col:
                    attributes[existing_idx] = attr  # replace with better-sourced record
                # else: keep first occurrence
            else:
                seen_names[attr_name] = len(attributes)
                attributes.append(attr)

        return attributes

    # -------------------------------------------------------------------------
    # Pass 3 — AI attribute enrichment
    # -------------------------------------------------------------------------

    def _build_pass3_prompt(
        self,
        entity_name: str,
        entity_description: str,
        attributes: List[Dict[str, Any]],
    ) -> str:
        field_list = []
        for attr in attributes:
            entry: Dict[str, Any] = {
                "attribute_name": attr["attribute_name"],
                "data_type":      attr["data_type"],
                "required":       attr["required"],
            }
            meta = attr.get("source_metadata", {})
            src  = meta.get("source_column")
            if src:
                entry["source_column"] = src
            rules = attr.get("business_rules", [])
            if rules:
                entry["transformation_note"] = rules[0][:150]
            field_list.append(entry)

        example_name = field_list[0]["attribute_name"] if field_list else "attribute_name"

        return f"""You are a data architect enriching EDW attribute metadata for a Pharmacy Benefits Management (PBM) organization.

This organization uses a pass-through pricing model (not spread pricing).

## ENTITY

Name:        {entity_name}
Description: {entity_description}
Domain:      {self.cdm_domain}

## ATTRIBUTES TO ENRICH ({len(attributes)} total)

```json
{json.dumps(field_list, indent=2)}
```

## OUTPUT FORMAT

Return ONLY a JSON object with a single key "attributes" containing an array — one object per attribute, in the same order as the input above:

```json
{{
  "attributes": [
    {{
      "attribute_name": "{example_name}",
      "description": "Plain business description of what this field contains (1-2 sentences).",
      "business_context": "How this field is used in {self.cdm_domain} operations or reporting (1 sentence).",
      "is_pii": false,
      "is_phi": false,
      "data_classification": "Internal"
    }},
    {{
      "attribute_name": "[NEXT ATTRIBUTE NAME]",
      "description": "...",
      "business_context": "...",
      "is_pii": false,
      "is_phi": false,
      "data_classification": "Internal"
    }}
  ]
}}
```

## CRITICAL - MUST DO ALL OF THE FOLLOWING

- Output ONLY the JSON object shown above — no markdown, no code fences, no preamble, no explanation
- The top-level key MUST be "attributes" and its value MUST be an array
- Return EXACTLY {len(attributes)} objects inside the array — one for every attribute in the input, in the same order
- `attribute_name` must exactly match the input value
- `is_pii` = true only if field could identify a person (name, DOB, SSN, address, phone, email, MBI, member_id)
- `is_phi` = true only if field is Protected Health Information under HIPAA (diagnosis, prescription, health plan info)
- `data_classification` must be one of: Public, Internal, Confidential, Restricted
  - Default: Internal
  - Confidential: contains PII or PHI
  - Restricted: financial identifiers, clinical detail, or highly sensitive operational data

Generate the enriched JSON object now."""

    # -------------------------------------------------------------------------
    # LLM helpers
    # -------------------------------------------------------------------------

    def _call_llm_json_object(self, prompt: str) -> Dict[str, Any]:
        if not self.llm:
            raise ValueError("LLM not configured")
        messages = [
            {"role": "system", "content": "You are a data architect. Return ONLY valid JSON, no markdown."},
            {"role": "user",   "content": prompt},
        ]
        text, _ = self.llm.chat(messages)
        return self._parse_json(text)  # type: ignore[return-value]

    def _call_llm_json_array(self, prompt: str) -> List[Dict[str, Any]]:
        if not self.llm:
            raise ValueError("LLM not configured")
        messages = [
            {"role": "system", "content": "You are a data architect. Return ONLY a valid JSON object with an 'attributes' key containing an array. No markdown, no code fences."},
            {"role": "user",   "content": prompt},
        ]
        text, _ = self.llm.chat(messages)
        result = self._parse_json(text)
        # Unwrap if model returned {"attributes": [...]} or any single-key dict with a list value
        if isinstance(result, dict):
            list_vals = [v for v in result.values() if isinstance(v, list)]
            if list_vals:
                result = list_vals[0]
            else:
                raise ValueError(
                    f"Expected JSON array, got dict with keys: {list(result.keys())}"
                )
        if not isinstance(result, list):
            raise ValueError(f"Expected JSON array, got {type(result).__name__}")
        return result  # type: ignore[return-value]

    @staticmethod
    def _parse_json(text: str) -> Any:
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if len(lines) > 2 else lines[1:]).strip()
        return json.loads(text)

    def _save_prompt(self, prompt: str, label: str) -> Path:
        assert self.prompts_dir
        path = self.prompts_dir / f"{label}_{datetime.now().strftime('%H%M%S')}.txt"
        path.write_text(prompt, encoding="utf-8")
        return path

    # -------------------------------------------------------------------------
    # Merge Pass 3 enrichment into attribute list
    # -------------------------------------------------------------------------

    @staticmethod
    def _merge_enrichment(
        attributes: List[Dict[str, Any]],
        enrichment: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Merge AI enrichment results back into attribute records.
        Primary match by attribute_name; positional fallback.
        """
        enrich_by_name = {e.get("attribute_name", ""): e for e in enrichment}

        for i, attr in enumerate(attributes):
            name   = attr["attribute_name"]
            enrich = enrich_by_name.get(name)

            # Positional fallback if name not matched
            if enrich is None and i < len(enrichment):
                enrich = enrichment[i]

            if enrich:
                attr["description"]         = enrich.get("description")
                attr["business_context"]    = enrich.get("business_context")
                attr["is_pii"]              = bool(enrich.get("is_pii", False))
                attr["is_phi"]              = bool(enrich.get("is_phi", False))
                attr["data_classification"] = enrich.get("data_classification", "Internal")
            else:
                attr["description"]         = f"Field: {name}"
                attr["business_context"]    = ""
                attr["is_pii"]              = False
                attr["is_phi"]              = False
                attr["data_classification"] = "Internal"

        return attributes

    # -------------------------------------------------------------------------
    # Pass 3 — attribute enrichment (chunked)
    # -------------------------------------------------------------------------

    PASS3_CHUNK_SIZE = 125  # model suggested 100-150; 125 is a safe middle ground

    def _run_pass3_chunked(
        self,
        entity_name: str,
        entity_desc: str,
        attributes: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Run Pass 3 in chunks. The model cannot return 500+ enriched objects
        in a single response — it self-reports a practical limit of ~100-150.
        Chunk at PASS3_CHUNK_SIZE, merge all results at the end.
        """
        chunk_size     = self.PASS3_CHUNK_SIZE
        chunks         = [attributes[i:i + chunk_size] for i in range(0, len(attributes), chunk_size)]
        n_chunks       = len(chunks)
        all_enrichment: List[Dict[str, Any]] = []

        for ci, chunk in enumerate(chunks, 1):
            print(f"         chunk {ci}/{n_chunks} ({len(chunk)} attrs)")
            prompt = self._build_pass3_prompt(entity_name, entity_desc, chunk)

            if self.dry_run:
                self._save_prompt(prompt, f"p3_chunk{ci:02d}_edw_{entity_name.lower()}")
                for attr in chunk:
                    all_enrichment.append({
                        "attribute_name":      attr["attribute_name"],
                        "description":         "[dry run]",
                        "business_context":    "[dry run]",
                        "is_pii":              False,
                        "is_phi":              False,
                        "data_classification": "Internal",
                    })
                continue

            try:
                chunk_enrichment = self._call_llm_json_array(prompt)
                if len(chunk_enrichment) != len(chunk):
                    print(f"         WARNING: chunk {ci} returned {len(chunk_enrichment)} "
                          f"enrichments for {len(chunk)} attributes")
                all_enrichment.extend(chunk_enrichment)
            except Exception as exc:
                print(f"         ERROR chunk {ci}: {exc} — using safe defaults")
                for attr in chunk:
                    all_enrichment.append({
                        "attribute_name":      attr["attribute_name"],
                        "description":         f"Field: {attr['attribute_name']}",
                        "business_context":    "",
                        "is_pii":              False,
                        "is_phi":              False,
                        "data_classification": "Internal",
                    })

        return self._merge_enrichment(attributes, all_enrichment)

    def _build_entity_record(
        self,
        entity: Dict[str, Any],
        pass1: Dict[str, Any],
        file_type: str,
        attributes: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Assemble the final entity record in standard rationalized schema."""
        entity_id  = entity.get("entity_id", "")
        src_table  = entity.get("source_table", entity_id)
        ni_table   = entity.get("ni_table", "")
        np_table   = entity.get("np_table", "")
        src_schema = entity.get("source_schema", "")
        src_db     = entity.get("source_database", "")

        biz_count = len(self._business_fields(entity))
        file_ref  = f"edw_{entity_id.lower()}.json"

        if attributes is not None:
            for attr in attributes:
                attr["source_files"] = [file_ref]

        record: Dict[str, Any] = {
            "entity_name": pass1.get("entity_name", np_table.replace("NP_", "")),
            "description": pass1.get("description", ""),
            "source_type": "EDW",
            "source_info": {
                "entity_id":            entity_id,
                "source_table":         src_table,
                "source_schema":        src_schema,
                "source_database":      src_db,
                "ni_table":             ni_table,
                "np_table":             np_table,
                "file_type":            file_type,
                "files":                [file_ref],
                "field_count_business": biz_count,
                "scd2_columns":         entity.get("scd2_columns", []),
                "api":                  None,
                "url":                  None,
                "version":              None,
            },
            "business_context":  pass1.get("business_context", ""),
            "technical_context": pass1.get("technical_context", ""),
            "ai_metadata": {
                "selection_reasoning": None,
                "pruning_notes":       None,
            },
        }

        if attributes is not None:
            record["attributes"] = attributes

        return record

    # -------------------------------------------------------------------------
    # Run — orchestrates all three passes
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Cross-entity name resolution
    # -------------------------------------------------------------------------

    def run(self, output_dir: str) -> Optional[str]:
        """
        Execute all three passes for each configured entity.

        Saves TWO output files:
          rationalized_edw_entities_{domain}_{ts}.json  — entities only (Step 3)
          rationalized_edw_{domain}_{ts}.json            — with attributes (Step 6)

        Returns path to the full file, or None on failure / dry run.
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        if not self.entity_ids:
            print("  No EDW entities configured — skipping")
            return None

        self.prompts_dir = out / "prompts"
        if self.dry_run:
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            print(f"  DRY RUN — prompts -> {self.prompts_dir}")

        total          = len(self.entity_ids)
        entities_only: List[Dict[str, Any]] = []
        entities_full: List[Dict[str, Any]] = []
        skipped:        List[str]           = []

        print(f"\n  Processing {total} EDW entities (3-pass)...")

        # ── PHASE A: Pass 1 (AI entity desc) + Pass 2 (programmatic attr extraction) ──
        # Collect all entities before running Pass 3 so cross-entity name
        # resolution can see the full set of attribute names first.
        phase_a: List[Dict[str, Any]] = []   # [(entity_id, pass1_result, file_type, attributes)]

        for idx, entity_id in enumerate(self.entity_ids, 1):
            print(f"\n  [{idx}/{total}] {entity_id} — P1+P2")

            load_result = self._load_entity_file(entity_id)
            if load_result is None:
                skipped.append(entity_id)
                continue

            entity, file_type = load_result

            # Pass 1: entity description (AI)
            print(f"    [P1] Entity description (AI)")
            p1_prompt = self._build_pass1_prompt(entity)
            print(f"         ~{len(p1_prompt)//4:,} tokens")

            if self.dry_run:
                self._save_prompt(p1_prompt, f"p1_edw_{entity_id.lower()}")
                pass1_result: Dict[str, Any] = {
                    "entity_name":       entity_id,
                    "description":       "[dry run]",
                    "business_context":  "[dry run]",
                    "technical_context": "[dry run]",
                }
            else:
                try:
                    pass1_result = self._call_llm_json_object(p1_prompt)
                    print(f"         -> {pass1_result.get('entity_name', '?')}")
                except Exception as exc:
                    print(f"    ERROR Pass 1: {exc}")
                    skipped.append(entity_id)
                    continue

            # entities-only record (no attributes)
            entity_only = self._build_entity_record(entity, pass1_result, file_type)
            entities_only.append(entity_only)

            # Pass 2: programmatic attribute extraction
            print(f"    [P2] Attribute extraction (programmatic)")
            attributes = self._extract_attributes(entity)
            print(f"         {len(attributes)} business attributes")

            phase_a.append((entity_id, entity, pass1_result, file_type, attributes))

        # ── CROSS-ENTITY NAME RESOLUTION (between P2 and P3) ─────────────────
        # Build a lookup of bare F/UN/UT code -> best descriptive name across
        # all entities, then backfill any entity that only had the bare code.
        # This runs BEFORE Pass 3 so the LLM sees clean names, not bare stubs.
        all_attributes_flat = [a for (_, _, _, _, attrs) in phase_a for a in attrs]
        bare_re = re.compile(r"^(f|un|ut)\d+$", re.IGNORECASE)
        best_names: Dict[str, str] = {}
        for attr in all_attributes_flat:
            aname = attr.get("attribute_name", "")
            if bare_re.match(aname):
                continue
            for code in (attr.get("source_attribute") or []):
                code_upper = code.upper().strip()
                if code_upper and code_upper not in best_names:
                    best_names[code_upper] = aname

        resolved_count = 0
        for attr in all_attributes_flat:
            aname = attr.get("attribute_name", "")
            if not bare_re.match(aname):
                continue
            for code in (attr.get("source_attribute") or [aname]):
                better = best_names.get(code.upper().strip())
                if better:
                    attr["attribute_name"] = better
                    resolved_count += 1
                    break

        if resolved_count:
            print(f"\n  [name resolution] {resolved_count} bare-code name(s) resolved before P3")

        # ── PHASE B: Pass 3 (AI attribute enrichment) ────────────────────────
        for idx, (entity_id, entity, pass1_result, file_type, attributes) in enumerate(phase_a, 1):
            print(f"\n  [{idx}/{len(phase_a)}] {entity_id} — P3")

            if not attributes:
                print(f"    [P3] No attributes — skipping")
                entities_full.append(
                    self._build_entity_record(entity, pass1_result, file_type, attributes=[])
                )
                continue

            print(f"    [P3] Attribute enrichment (AI, chunk_size={self.PASS3_CHUNK_SIZE})")
            entity_name = pass1_result.get("entity_name", entity_id)
            entity_desc = pass1_result.get("description", "")
            n_chunks = (len(attributes) + self.PASS3_CHUNK_SIZE - 1) // self.PASS3_CHUNK_SIZE
            print(f"         {len(attributes)} attributes → {n_chunks} chunk(s)")

            try:
                attributes = self._run_pass3_chunked(entity_name, entity_desc, attributes)
                print(f"         OK: {len(attributes)} attributes enriched")
            except Exception as exc:
                print(f"    ERROR Pass 3: {exc}")
                for attr in attributes:
                    attr.setdefault("description",         f"Field: {attr['attribute_name']}")
                    attr.setdefault("business_context",    "")
                    attr.setdefault("is_pii",              False)
                    attr.setdefault("is_phi",              False)
                    attr.setdefault("data_classification", "Internal")

            entities_full.append(
                self._build_entity_record(entity, pass1_result, file_type, attributes=attributes)
            )

        # ── SUMMARY ──────────────────────────────────────────────────────────
        print(f"\n  Done: {len(entities_only)} entities | {len(skipped)} skipped")
        if skipped:
            print(f"  Skipped: {skipped}")

        if self.dry_run:
            print("  Dry run complete.")
            return None

        if not entities_only:
            print("  ERROR: nothing produced")
            return None

        domain_safe = self.cdm_domain.replace(" ", "_")
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")

        # ── Save entities-only file (Step 3 / Foundational CDM) ─────────────
        entities_path = out / f"rationalized_edw_entities_{domain_safe}_{timestamp}.json"
        entities_path.write_text(
            json.dumps(
                {
                    "rationalization_metadata": {
                        "source_type":                "EDW",
                        "cdm_domain":                 self.cdm_domain,
                        "cdm_classification":         self.cdm_type,
                        "rationalization_timestamp":  datetime.now().isoformat(),
                        "pass":                       "1-entities-only",
                        "note": (
                            "Entities only — no attributes. "
                            "For use by Step 3 (Foundational CDM) as a REFERENCE source. "
                            "EDW tables inform CDM scope; they do not define CDM structure."
                        ),
                        "source_entities_configured": total,
                        "source_entities_skipped":    len(skipped),
                        "cdm_entities_produced":      len(entities_only),
                    },
                    "entities":       entities_only,
                    "reference_data": {"value_sets": [], "code_systems": []},
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        print(f"\n  [entities-only] {entities_path.name}")

        # ── Save full file (Step 6 / Build Full CDM) ─────────────────────────
        attr_counts = [len(e.get("attributes", [])) for e in entities_full]
        full_path   = out / f"rationalized_edw_{domain_safe}_{timestamp}.json"
        full_path.write_text(
            json.dumps(
                {
                    "rationalization_metadata": {
                        "source_type":                "EDW",
                        "cdm_domain":                 self.cdm_domain,
                        "cdm_classification":         self.cdm_type,
                        "rationalization_timestamp":  datetime.now().isoformat(),
                        "pass":                       "1+2+3-with-attributes",
                        "note": (
                            "Entities with full attribute metadata. "
                            "For use by Step 6 (Build Full CDM) for attribute mapping."
                        ),
                        "source_entities_configured": total,
                        "source_entities_skipped":    len(skipped),
                        "cdm_entities_produced":      len(entities_full),
                        "total_attributes":           sum(attr_counts),
                        "avg_attributes_per_entity":  (
                            round(sum(attr_counts) / len(attr_counts), 1)
                            if attr_counts else 0
                        ),
                    },
                    "entities":       entities_full,
                    "reference_data": {"value_sets": [], "code_systems": []},
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        print(f"  [full + attrs]  {full_path.name}")
        print(f"    Entities:    {len(entities_full)}")
        print(f"    Total attrs: {sum(attr_counts)}")

        return str(full_path)


# =============================================================================
# ORCHESTRATOR WRAPPER
# =============================================================================

def run_edw_rationalization(
    config: Any,
    outdir: Any,
    llm: Optional[Any] = None,
    dry_run: bool = False,
    config_path: Optional[str] = None,
) -> Optional[str]:
    """Wrapper called by cdm_orchestrator.py Step 1d."""
    if not config_path:
        raise ValueError("config_path is required for EDW rationalization")
    return EDWRationalizer(config_path, llm=llm, dry_run=dry_run).run(str(outdir))


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python rationalize_edw.py <config_file> <output_dir> [--dry-run]")
        sys.exit(1)

    _config_path = sys.argv[1]
    _output_dir  = sys.argv[2]
    _dry         = "--dry-run" in sys.argv

    _llm = None
    if not _dry:
        try:
            from src.core.llm_client import LLMClient
            from src.core.model_selector import MODEL_OPTIONS

            _key  = list(MODEL_OPTIONS.keys())[0]
            _conf = MODEL_OPTIONS[_key]
            _llm  = LLMClient(
                model=_conf["model"],
                base_url=_conf["base_url"](),
                temperature=0.2,
                timeout=1800,
            )
            print(f"LLM: {_conf['name']}")
        except ImportError:
            print("WARNING: Could not import LLMClient — switching to dry-run mode")
            _dry = True

    EDWRationalizer(_config_path, llm=_llm, dry_run=_dry).run(_output_dir)