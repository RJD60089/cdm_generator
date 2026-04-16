# src/config/config_gen_ancillary.py
"""
Ancillary Definition Source Configuration Generator

Discovers ancillary source files (SQL DDL, JSON, XLSX, etc.) from the
CDM-specific ancillary/source/ directory, preprocesses them (e.g., DDL
SQL -> JSON), and prompts the user for file type and processing mode.

Flow:
1. Discover files in input/business/cdm_{name}/ancillary/source/
2. For each file, preprocess if needed (DDL SQL -> JSON)
3. Prompt user for file_type and processing_mode per file
4. Return structured list for config['input_files']['ancillary']

Processing modes:
  - driver  : Shapes the foundational CDM (injected as structural scaffold)
  - refiner : Maps to CDM, then refines based on gaps (analyze/review/apply)
  - mapper  : Maps to CDM for source lineage only (no structural changes)
"""

import re
from pathlib import Path
from typing import Dict, List, Optional

from . import config_utils
from .config_gen_core import ConfigGeneratorBase, prompt_user_choice


# Supported file type choices
FILE_TYPES = [
    ("ddl", "SQL DDL (CREATE TABLE statements)"),
    ("api_spec", "API specification"),
    ("data_model", "Data model definition"),
    ("spreadsheet", "Spreadsheet / tabular data"),
    ("other", "Other format"),
]

# Processing mode choices
PROCESSING_MODES = [
    ("driver", "Driver — shape the foundational CDM from this source"),
    ("refiner", "Refiner — map first, then refine CDM based on gaps"),
    ("mapper", "Mapper — map to CDM for lineage only (no CDM changes)"),
]


def _prompt_choice(label: str, choices: List[tuple], default_idx: int = 0) -> str:
    """Prompt user to select from a numbered list of choices.

    Args:
        label: Prompt header text
        choices: List of (key, description) tuples
        default_idx: 0-based index of the default choice

    Returns:
        The key of the selected choice
    """
    print(f"\n      {label}")
    for i, (key, desc) in enumerate(choices):
        marker = " *" if i == default_idx else ""
        print(f"        {i + 1}. {desc}{marker}")

    while True:
        response = input(f"      Select [1-{len(choices)}] (default {default_idx + 1}): ").strip()
        if not response:
            return choices[default_idx][0]
        try:
            idx = int(response) - 1
            if 0 <= idx < len(choices):
                return choices[idx][0]
        except ValueError:
            pass
        print(f"      Invalid selection. Enter 1-{len(choices)}.")


def _generate_source_id(filename: str) -> str:
    """Generate a unique source_id from filename.

    No underscores allowed (would break discovery filename parsing).
    Prefixed with 'ancillary-' to identify as ancillary in the pipeline.

    Examples:
        'PC2-DDL.txt'          -> 'ancillary-pc2-ddl'
        'claims_api_spec.json' -> 'ancillary-claims-api-spec'
        'pharmacy_schema.sql'  -> 'ancillary-pharmacy-schema'
    """
    stem = Path(filename).stem.lower()
    # Replace underscores, spaces, and non-alphanumeric chars with hyphens
    clean = re.sub(r'[^a-z0-9]+', '-', stem).strip('-')
    return f"ancillary-{clean}"


def _detect_file_type(filename: str) -> int:
    """Detect likely file type from extension. Returns default index into FILE_TYPES."""
    ext = Path(filename).suffix.lower()
    if ext == ".sql":
        return 0  # ddl
    elif ext == ".xlsx":
        return 3  # spreadsheet
    elif ext == ".json":
        return 2  # data_model
    elif ext in (".yaml", ".yml"):
        return 1  # api_spec
    return 4  # other


def _preprocess_file(source_path: Path, file_type: str, ancillary_dir: Path) -> Optional[str]:
    """Preprocess a source file into JSON if needed.

    For DDL SQL files, converts to JSON via the existing ddl_converter.
    For other types, no preprocessing is done.

    Args:
        source_path: Full path to the raw source file
        file_type: Detected/selected file type
        ancillary_dir: The ancillary/ directory (parent of source/)

    Returns:
        Preprocessed filename (relative to ancillary/) or None if no
        preprocessing was needed/possible
    """
    if file_type == "ddl" and source_path.suffix.lower() in (".sql", ".ddl", ".txt"):
        try:
            from src.converters.ddl_converter import convert_ddl_to_json
            import json

            json_str = convert_ddl_to_json(str(source_path))
            # convert_ddl_to_json returns a JSON string
            json_data = json.loads(json_str) if isinstance(json_str, str) else json_str

            output_name = source_path.stem + ".json"
            output_path = ancillary_dir / output_name

            ancillary_dir.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)

            print(f"      Preprocessed: {source_path.name} -> {output_name}")
            return output_name

        except Exception as e:
            print(f"      Warning: DDL preprocessing failed for {source_path.name}: {e}")
            return None

    return None


class AncillaryConfigGenerator(ConfigGeneratorBase):
    """Discover and configure ancillary definition sources."""

    def __init__(self, cdm_name: str, llm_client=None):
        super().__init__(cdm_name, llm_client)
        self.ancillary_dir = config_utils.get_ancillary_dir(cdm_name)

    def run_analysis(self, source_config: Dict, dry_run: bool = False) -> Optional[Dict]:
        """Discover ancillary files and prompt user for type + mode.

        Args:
            source_config: Current config dict (for context)
            dry_run: If True, skip preprocessing

        Returns:
            Dict with 'ancillary' key containing list of file entries,
            or None if no ancillary files found
        """
        # Discover files
        files = config_utils.list_ancillary_files(self.cdm_name)

        if not files:
            print(f"\n   No ancillary source files found in:")
            print(f"      {self.ancillary_dir / 'source'}/")
            print(f"      Place source files (*.sql, *.json, *.xlsx) there to use ancillary sources.")
            return None

        print(f"\n   Found {len(files)} ancillary source file(s):")
        for f in files:
            print(f"      - {f}")

        # Process each file
        entries: List[Dict] = []

        for filename in files:
            source_path = config_utils.resolve_ancillary_file(self.cdm_name, filename, preprocessed=False)
            print(f"\n   --- {filename} ---")

            # Generate and confirm source_id
            default_id = _generate_source_id(filename)
            source_id_input = input(f"      Source ID [{default_id}]: ").strip()
            source_id = source_id_input if source_id_input else default_id
            # Enforce no underscores
            source_id = source_id.replace("_", "-")
            if not source_id.startswith("ancillary-"):
                source_id = f"ancillary-{source_id}"

            # Detect and confirm file type
            default_type_idx = _detect_file_type(filename)
            file_type = _prompt_choice("File type:", FILE_TYPES, default_idx=default_type_idx)

            # Select processing mode (default to refiner)
            processing_mode = _prompt_choice("Processing mode:", PROCESSING_MODES, default_idx=1)

            # Preprocess if needed
            preprocessed_file = None
            if not dry_run:
                preprocessed_file = _preprocess_file(source_path, file_type, self.ancillary_dir)

            entry = {
                "file": filename,
                "file_type": file_type,
                "processing_mode": processing_mode,
                "source_id": source_id,
            }
            if preprocessed_file:
                entry["preprocessed_file"] = preprocessed_file

            entries.append(entry)

        print(f"\n   Ancillary configuration complete: {len(entries)} file(s)")
        for e in entries:
            print(f"      {e['file']}: id={e['source_id']}, type={e['file_type']}, mode={e['processing_mode']}")

        return {"ancillary": entries}
