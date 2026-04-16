# src/rationalizers/rationalize_ancillary.py
"""
Ancillary Definition Source Rationalization Module

Rationalizes ancillary source files (DDL, API specs, spreadsheets, etc.)
into unified entities and attributes following the standard rationalization
template schema.

Supports heterogeneous file types via preprocessing dispatch:
  - DDL SQL: uses ddl_converter.convert_ddl_to_json()
  - XLSX: uses guardrails_converter.convert_guardrails_to_json()
  - JSON: loaded directly
  - Other: raw text wrapped in simple JSON structure

Follows the GuardrailsRationalizer pattern:
  - Iterative file-by-file processing with incremental accumulation
  - Each file processed against prior_state from previous files
  - Output in standard rationalization_template.json format
  - source_type = "ancillary"

Processing mode (driver/refiner/mapper) does NOT affect rationalization.
All ancillary files are rationalized together into one output file.
Mode routing happens downstream in the orchestrator/pipeline.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.config import config_utils


# =============================================================================
# FILE PREPROCESSING
# =============================================================================

def _preprocess_ddl(file_path: str) -> Dict:
    """Convert DDL SQL file to JSON via existing converter."""
    from src.converters.ddl_converter import convert_ddl_to_json
    json_str = convert_ddl_to_json(file_path)
    if isinstance(json_str, str):
        return json.loads(json_str)
    return json_str


def _preprocess_xlsx(file_path: str) -> Dict:
    """Convert XLSX file to JSON via guardrails converter."""
    from src.converters import convert_guardrails_to_json
    return convert_guardrails_to_json(file_path)


def _preprocess_json(file_path: str) -> Dict:
    """Load JSON file directly."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _preprocess_raw(file_path: str) -> Dict:
    """Wrap raw text file in simple JSON structure."""
    encodings = ["utf-8", "utf-16", "latin-1"]
    text = None
    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f:
                text = f.read()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    if text is None:
        raise ValueError(f"Unable to read file with any supported encoding: {file_path}")
    return {"source_file": Path(file_path).name, "raw_content": text}


def preprocess_file(file_path: str, file_type: str) -> Dict:
    """Preprocess a source file based on its type.

    Args:
        file_path: Full path to the source file
        file_type: File type from config ('ddl', 'api_spec', 'data_model',
                   'spreadsheet', 'other')

    Returns:
        Dict representation of the file content
    """
    ext = Path(file_path).suffix.lower()

    if file_type == "ddl" and ext == ".sql":
        return _preprocess_ddl(file_path)
    elif file_type == "ddl" and ext == ".json":
        return _preprocess_json(file_path)
    elif file_type == "spreadsheet" or ext == ".xlsx":
        return _preprocess_xlsx(file_path)
    elif ext == ".json":
        return _preprocess_json(file_path)
    else:
        return _preprocess_raw(file_path)


# =============================================================================
# RATIONALIZER
# =============================================================================

class AncillaryRationalizer:
    """Rationalize ancillary definition source files into standard format."""

    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run

        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.cdm_domain = self.config.get("cdm", {}).get("domain", "")
        self.cdm_classification = self.config.get("cdm", {}).get("type", "Core")
        self.cdm_description = self.config.get("cdm", {}).get("description", "")

        # Get ancillary files from config (list of dicts with file, file_type, processing_mode)
        input_files = self.config.get("input_files", {})
        self.ancillary_files: List[Dict[str, Any]] = input_files.get("ancillary", [])

        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  Ancillary files: {len(self.ancillary_files)}")

    def build_prompt(self, file_content: Dict, filename: str,
                     file_type: str, prior_state: Optional[Dict] = None) -> str:
        """Build rationalization prompt for a single ancillary file.

        Args:
            file_content: Preprocessed file content (dict)
            filename: Original filename for lineage
            file_type: File type for context
            prior_state: Previously rationalized state (None for first file)

        Returns:
            Complete prompt string
        """
        # Type-specific guidance
        type_guidance = {
            "ddl": "This is a SQL DDL schema with CREATE TABLE statements converted to JSON. "
                   "Each table represents a potential entity; columns are potential attributes. "
                   "Primary keys, foreign keys, and constraints provide relationship context.",
            "api_spec": "This is an API specification. Endpoints and data structures represent "
                        "potential entities; request/response fields are potential attributes.",
            "data_model": "This is a data model definition. Entities and their fields should be "
                          "extracted and rationalized.",
            "spreadsheet": "This is tabular data from a spreadsheet. Sheets may represent entities; "
                           "columns represent potential attributes.",
            "other": "This is a raw data source. Extract entities and attributes based on the "
                     "structure and content.",
        }

        guidance = type_guidance.get(file_type, type_guidance["other"])

        prompt = f"""You are a business analyst engaged in developing a CDM for a PBM organization.

## CDM CONTEXT

**CDM Domain:** {self.cdm_domain}

**CDM Description:** {self.cdm_description}

## SCOPE FILTERING

Use the CDM Description's Includes/Excludes to determine relevance:
- INCLUDE: Entities/attributes that directly define what's listed in "Includes:"
- EXCLUDE: Entities/attributes that belong to domains listed in "Excludes:"
- When uncertain, check if the element's PRIMARY PURPOSE aligns with this CDM

## SOURCE TYPE

{guidance}

## YOUR TASK

Analyze the provided ancillary source file and extract all business-relevant
entities and attributes for this CDM domain. This is a CDM-specific source
system that may contain schema definitions, data structures, or specifications
that need to be rationalized into the standard CDM format.

## RATIONALIZATION GOALS

1. Identify all unique business entities relevant to this CDM domain
2. For DDL sources: tables become entities, columns become attributes
3. Preserve source lineage: track schema.table.column for DDL sources
4. Preserve data types, constraints (PK, FK, NOT NULL), and descriptions
5. Consolidate duplicate or overlapping attributes across tables
6. Focus on business-relevant entities; exclude pure audit/logging tables
   unless they contain business-meaningful data
7. Preserve foreign key relationships as they define entity relationships
"""

        if prior_state:
            prompt += """
## INCREMENTAL RATIONALIZATION

A previously rationalized output state is included below. You must:
1. Treat it as the current rationalized output state
2. Analyze the NEW source file against this state
3. For matching entities: merge attributes, append to source_files lists
4. For new entities: add to rationalized_entities
5. For duplicate attributes: consolidate, preserve all source lineage
6. For conflicts: prefer the more complete/specific definition, note in business_context
7. Return the COMPLETE updated rationalized output (not just changes)
"""

        prompt += f"""
## OUTPUT FORMAT

Return ONLY valid JSON in this structure:

```json
{{
  "domain": "{self.cdm_domain}",
  "rationalized_entities": [
    {{
      "entity_name": "[ENTITY NAME]",
      "source_files": ["{filename}::[SCHEMA].[TABLE]"],
      "description": "...",
      "business_context": "...",
      "attributes": [
        {{
          "attribute_name": "[ATTRIBUTE NAME]",
          "source_files_element": ["{filename}::[SCHEMA].[TABLE]::[COLUMN]"],
          "data_type": "string|number|date|boolean|decimal",
          "required": true,
          "allow_null": false,
          "description": "...",
          "is_pii": false,
          "is_phi": false,
          "data_classification": "Internal",
          "validation_rules": [],
          "business_rules": []
        }}
      ]
    }}
  ]
}}
```

## CRITICAL REQUIREMENTS

- Output ONLY valid JSON (no markdown, no code blocks)
- `attribute_name` = your rationalized/cleaned name for the CDM
- `source_files_element` = source reference in file::schema.table::column format
- Preserve data types from source (map to string/number/date/boolean/decimal)
- Track PK/FK constraints in descriptions or business_rules
- Focus on elements relevant to: {self.cdm_description}

---
"""

        if prior_state:
            prompt += f"""
## PREVIOUSLY RATIONALIZED OUTPUT STATE

```json
{json.dumps(prior_state, indent=2)}
```

---
"""

        prompt += f"""
## ANCILLARY SOURCE FILE TO PROCESS

### {filename} (type: {file_type})

```json
{json.dumps(file_content, indent=2)}
```

---

Generate the rationalized JSON now."""

        return prompt

    def save_prompt(self, prompt: str, output_dir: Path, file_index: int) -> dict:
        """Save prompt to file and return stats."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = output_dir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)

        domain_safe = self.cdm_domain.replace(" ", "_")
        prompt_file = prompts_dir / f"ancillary_prompt_{domain_safe}_{file_index}_{timestamp}.txt"

        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt)

        char_count = len(prompt)
        token_estimate = char_count // 4

        return {
            "file": str(prompt_file),
            "characters": char_count,
            "tokens_estimate": token_estimate,
        }

    def _call_llm(self, prompt: str) -> Dict:
        """Call LLM and parse JSON response."""
        if self.llm is None:
            raise ValueError("LLM client not configured")

        messages = [{"role": "user", "content": prompt}]
        response_text, _ = self.llm.chat(messages)

        # Clean response - remove markdown code blocks if present
        text = response_text.strip()
        if text.startswith("```"):
            lines = text.split("```")
            if len(lines) >= 2:
                text = lines[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

        return json.loads(text)

    def _transform_to_common_format(self, raw_output: Dict) -> List[Dict]:
        """Transform raw AI output to common entity format."""
        entities = []

        for raw_entity in raw_output.get("rationalized_entities", []):
            attributes = []

            for raw_attr in raw_entity.get("attributes", []):
                attr: Dict[str, Any] = {
                    "attribute_name": raw_attr.get("attribute_name", ""),
                    "description": raw_attr.get("description", ""),
                    "data_type": raw_attr.get("data_type", "string"),
                    "source_attribute": raw_attr.get("source_files_element", []),
                    "source_files": raw_attr.get("source_files_element", []),
                    "required": raw_attr.get("required", False),
                    "nullable": raw_attr.get("allow_null", True),
                    "cardinality": {
                        "min": 1 if raw_attr.get("required", False) else 0,
                        "max": "1",
                    },
                    "length": None,
                    "precision": None,
                    "scale": None,
                    "default_value": None,
                    "is_array": False,
                    "is_nested": False,
                    "is_pii": raw_attr.get("is_pii", False),
                    "is_phi": raw_attr.get("is_phi", False),
                    "data_classification": raw_attr.get("data_classification"),
                    "business_context": raw_attr.get("business_context"),
                    "business_rules": raw_attr.get("business_rules"),
                    "validation_rules": raw_attr.get("validation_rules"),
                    "is_calculated": raw_attr.get("is_calculated", False),
                    "calculation_dependency": raw_attr.get("calculation_dependency"),
                    "source_metadata": {},
                }
                attributes.append(attr)

            entity: Dict[str, Any] = {
                "entity_name": raw_entity.get("entity_name", ""),
                "description": raw_entity.get("description", ""),
                "source_type": "Ancillary",
                "source_info": {
                    "files": raw_entity.get("source_files", []),
                    "api": None,
                    "schema": None,
                    "table": None,
                    "url": None,
                    "version": None,
                },
                "business_context": raw_entity.get("business_context"),
                "technical_context": None,
                "ai_metadata": {
                    "selection_reasoning": None,
                    "pruning_notes": None,
                },
                "attributes": attributes,
                "source_metadata": {},
            }
            entities.append(entity)

        return entities

    # Max tokens (estimated) for a single LLM call — leave room for prompt
    # overhead and response.  922K model limit; target ~700K content max.
    MAX_CONTENT_TOKENS = 700_000
    # Characters-per-token estimate (conservative)
    CHARS_PER_TOKEN = 4

    def _estimate_tokens(self, content: Any) -> int:
        """Estimate token count from content."""
        return len(json.dumps(content)) // self.CHARS_PER_TOKEN

    def _split_into_chunks(self, file_content: Dict, filename: str) -> List[Dict]:
        """Split large file content into sheet-level chunks.

        If the file has a 'sheets' key (XLSX converted via guardrails
        converter), split by sheet.  Each chunk is a dict with the same
        structure but containing only one sheet.  If a single sheet still
        exceeds the limit, split its rows into batches.

        Returns list of content dicts, each small enough for one LLM call.
        """
        sheets = file_content.get("sheets")
        if not sheets or not isinstance(sheets, dict):
            # Not a multi-sheet file — return as-is (caller will handle error)
            return [file_content]

        chunks: List[Dict] = []
        source_file = file_content.get("source_file", filename)

        for sheet_name, rows in sheets.items():
            sheet_content = {"source_file": source_file, "sheets": {sheet_name: rows}}
            est = self._estimate_tokens(sheet_content)

            if est <= self.MAX_CONTENT_TOKENS:
                chunks.append(sheet_content)
            else:
                # Sheet too large — split rows into batches
                if not isinstance(rows, list):
                    chunks.append(sheet_content)
                    continue
                batch_size = max(1, len(rows) * self.MAX_CONTENT_TOKENS // est)
                for start in range(0, len(rows), batch_size):
                    batch = rows[start:start + batch_size]
                    batch_content = {"source_file": source_file, "sheets": {sheet_name: batch}}
                    chunks.append(batch_content)
                print(f"      Sheet '{sheet_name}' split into {(len(rows) + batch_size - 1) // batch_size} batches ({len(rows)} rows)")

        return chunks

    def _merge_rationalized(self, results: List[Dict]) -> Dict:
        """Merge multiple rationalized results into one.

        Combines rationalized_entities from all chunks.  Entities with the
        same name are merged (attributes combined, source_files unioned).
        """
        merged_entities: Dict[str, Dict] = {}

        for result in results:
            for entity in result.get("rationalized_entities", []):
                name = entity.get("entity_name", "")
                if name in merged_entities:
                    existing = merged_entities[name]
                    # Merge source_files
                    existing_sources = set(existing.get("source_files", []))
                    existing_sources.update(entity.get("source_files", []))
                    existing["source_files"] = sorted(existing_sources)
                    # Merge attributes by name
                    existing_attrs = {a["attribute_name"]: a for a in existing.get("attributes", [])}
                    for attr in entity.get("attributes", []):
                        attr_name = attr.get("attribute_name", "")
                        if attr_name not in existing_attrs:
                            existing_attrs[attr_name] = attr
                    existing["attributes"] = list(existing_attrs.values())
                else:
                    merged_entities[name] = entity

        return {"rationalized_entities": list(merged_entities.values())}

    def run(self, output_dir: str) -> Optional[List[str]]:
        """Run ancillary rationalization — one output per ancillary file.

        Each ancillary file is rationalized independently (no incremental
        merging). Output filenames use the source_id from config so each
        ancillary source flows through the pipeline independently.

        Large files (e.g. XLSX with many rows) are automatically split into
        sheet-level or row-batch chunks to stay within LLM context limits.

        Args:
            output_dir: Directory to save output files

        Returns:
            List of output file paths (empty list in dry run, None if no files)
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        if not self.ancillary_files:
            print("  No ancillary files configured, skipping")
            return None

        total_files = len(self.ancillary_files)
        output_files: List[str] = []

        print(f"\n  Processing {total_files} ancillary file(s) independently...")

        for idx, entry in enumerate(self.ancillary_files, 1):
            filename = entry.get("file", "")
            file_type = entry.get("file_type", "other")
            source_id = entry.get("source_id", "ancillary")
            preprocessed = entry.get("preprocessed_file")

            print(f"\n  [{idx}/{total_files}] {source_id} ({filename}, type={file_type})")

            # Resolve to full path — prefer preprocessed JSON if available
            if preprocessed:
                file_path = config_utils.resolve_ancillary_file(
                    self.cdm_domain, preprocessed, preprocessed=True
                )
            else:
                file_path = config_utils.resolve_ancillary_file(
                    self.cdm_domain, filename, preprocessed=False
                )

            if not file_path.exists():
                print(f"    Warning: File not found: {file_path}")
                continue

            # Preprocess file into JSON dict
            try:
                if preprocessed and file_path.suffix.lower() == ".json":
                    file_content = _preprocess_json(str(file_path))
                else:
                    file_content = preprocess_file(str(file_path), file_type)
            except Exception as e:
                print(f"    Error: Failed to preprocess {filename}: {e}")
                continue

            # Check if content needs chunking
            est_tokens = self._estimate_tokens(file_content)
            if est_tokens > self.MAX_CONTENT_TOKENS:
                chunks = self._split_into_chunks(file_content, filename)
                print(f"    Content too large ({est_tokens:,} est. tokens), split into {len(chunks)} chunk(s)")
            else:
                chunks = [file_content]

            # Process each chunk
            chunk_results: List[Dict] = []

            for chunk_idx, chunk_content in enumerate(chunks, 1):
                if len(chunks) > 1:
                    chunk_label = f"[chunk {chunk_idx}/{len(chunks)}]"
                    # Identify which sheet is in this chunk
                    sheet_names = list(chunk_content.get("sheets", {}).keys())
                    sheet_info = f" (sheet: {sheet_names[0]})" if sheet_names else ""
                    print(f"    {chunk_label}{sheet_info}")
                    chunk_filename = f"{filename}::{sheet_names[0]}" if sheet_names else filename
                else:
                    chunk_filename = filename

                prompt = self.build_prompt(
                    chunk_content, chunk_filename, file_type, prior_state=None
                )

                # Dry run — save prompt
                if self.dry_run:
                    stats = self.save_prompt(prompt, output_path, idx * 100 + chunk_idx)
                    print(f"    Prompt saved: {Path(stats['file']).name}")
                    print(f"      Characters: {stats['characters']:,}")
                    print(f"      Tokens (est): {stats['tokens_estimate']:,}")
                    continue

                # Live mode — call LLM
                print(f"    Calling LLM...")

                try:
                    rationalized_state = self._call_llm(prompt)

                    entity_count = len(
                        rationalized_state.get("rationalized_entities", [])
                    )
                    attr_count = sum(
                        len(e.get("attributes", []))
                        for e in rationalized_state.get("rationalized_entities", [])
                    )
                    print(f"    Rationalized: {entity_count} entities, {attr_count} attributes")
                    chunk_results.append(rationalized_state)

                except json.JSONDecodeError as e:
                    print(f"    ERROR: Failed to parse LLM response for {filename}: {e}")
                    raise

            if self.dry_run:
                continue

            # Merge chunk results if multiple
            if len(chunk_results) > 1:
                rationalized_state = self._merge_rationalized(chunk_results)
                entity_count = len(rationalized_state.get("rationalized_entities", []))
                attr_count = sum(
                    len(e.get("attributes", []))
                    for e in rationalized_state.get("rationalized_entities", [])
                )
                print(f"    Merged: {entity_count} entities, {attr_count} attributes")
            elif chunk_results:
                rationalized_state = chunk_results[0]
            else:
                continue

            # Transform and save — per file, using source_id
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            domain_safe = self.cdm_domain.replace(" ", "_")

            entities = self._transform_to_common_format(rationalized_state)
            # Set source_type to source_id so it flows through the pipeline correctly
            for entity in entities:
                entity["source_type"] = source_id

            total_attrs = sum(len(e.get("attributes", [])) for e in entities)

            output: Dict[str, Any] = {
                "rationalization_metadata": {
                    "source_type": source_id,
                    "cdm_domain": self.cdm_domain,
                    "cdm_classification": self.cdm_classification,
                    "rationalization_timestamp": datetime.now().isoformat(),
                    "files_processed": 1,
                    "entities_processed": len(entities),
                    "attributes_processed": total_attrs,
                },
                "entities": entities,
                "reference_data": {"value_sets": [], "code_systems": []},
            }

            # Filename uses source_id: rationalized_{source_id}_{domain}_{timestamp}.json
            output_file = (
                output_path / f"rationalized_{source_id}_{domain_safe}_{timestamp}.json"
            )

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)

            print(f"    Saved: {output_file.name}")
            print(f"      Entities: {len(entities)}")
            print(f"      Attributes: {total_attrs}")

            output_files.append(str(output_file))

        # Summary
        if self.dry_run:
            print(f"\n  Dry run complete. {total_files} prompts saved.")
            return []

        print(f"\n  Ancillary rationalization complete: {len(output_files)} file(s) produced")
        return output_files


# =============================================================================
# ORCHESTRATOR WRAPPER
# =============================================================================

def run_ancillary_rationalization(
    config: Any,
    outdir: str,
    llm: Optional[Any] = None,
    dry_run: bool = False,
    config_path: Optional[str] = None,
) -> Optional[List[str]]:
    """Wrapper function for orchestrator compatibility.

    Args:
        config: AppConfig instance (unused, for interface consistency)
        outdir: Output directory path
        llm: LLM client instance
        dry_run: If True, save prompts only
        config_path: Path to config JSON file (required)

    Returns:
        List of output file paths, or None if no files
    """
    if not config_path:
        raise ValueError("config_path is required for Ancillary rationalization")

    rationalizer = AncillaryRationalizer(config_path, llm=llm, dry_run=dry_run)
    return rationalizer.run(str(outdir))
