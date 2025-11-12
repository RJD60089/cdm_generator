"""
DDL converter.
Handles both JSON (pass through) and SQL DDL files (convert).
"""
import json
import re
from pathlib import Path
from collections import defaultdict


def convert_ddl_to_json(file_path: str) -> str:
    """
    Convert DDL to JSON string.
    - JSON files: Pass through as-is
    - SQL files: Convert to JSON
    
    Args:
        file_path: Path to DDL file (.json or .sql)
        
    Returns:
        JSON string representation of DDL schema
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is unsupported
    """
    file = Path(file_path)
    
    if not file.exists():
        raise FileNotFoundError(f"DDL file not found: {file_path}")
    
    if file.suffix == '.json':
        # JSON - pass through directly
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Validate JSON
            json.loads(content)
            return content
    
    elif file.suffix == '.sql':
        # SQL - convert to JSON
        return _convert_sql_ddl(file_path)
    
    else:
        raise ValueError(f"Unsupported DDL format: {file.suffix}. Use .json or .sql")


def _convert_sql_ddl(file_path: str) -> str:
    """
    Convert SQL DDL to JSON format.
    Based on provided GenerateJsonFromDdl.py module.
    """
    # Load with encoding fallback
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            ddl_text = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="utf-16") as f:
                ddl_text = f.read()
        except:
            with open(file_path, "r", encoding="latin-1") as f:
                ddl_text = f.read()
    
    # Clean comments
    ddl_text = re.sub(r'--.*', '', ddl_text)
    ddl_text = re.sub(r'/\*.*?\*/', '', ddl_text, flags=re.S)
    
    # Base JSON structure
    ddl_json = {
        "source_file": Path(file_path).name,
        "schemas": defaultdict(lambda: {
            "tables": defaultdict(lambda: {
                "columns": [],
                "primary_key": [],
                "foreign_keys": {},
                "description": ""
            })
        }),
        "udts": {}
    }
    
    # Regex patterns
    create_table_pattern = re.compile(
        r"CREATE TABLE \[(\w+)\]\.\[(\w+)\]\((.*?)\)\s*(?:ON \[\w+\])?",
        re.S | re.I)
    
    column_line_pattern = re.compile(
        r"\[(\w+)\]\s+\[?(\w+(?:\(\d+(?:,\s*\d+)?\))?)\]?(.*?)(?:,|$)", re.S)
    
    udt_pattern = re.compile(r"CREATE TYPE \[(\w+)\] FROM (\w+\(\d+\)) NULL;", re.S)
    
    pk_pattern = re.compile(
        r"ALTER TABLE\s+\[(\w+)\]\.\[(\w+)\]\s+(?:WITH CHECK\s+)?ADD\s+CONSTRAINT\s+\[(\w+)\]\s+PRIMARY KEY\s+(?:CLUSTERED|NONCLUSTERED)?\s*\((.*?)\)",
        re.S | re.I)
    
    fk_block_pattern = re.compile(
        r"ALTER TABLE\s+\[(?P<schema>\w+)\]\.\[(?P<table>\w+)\]\s+ADD\s+(?P<constraints>.+?);",
        re.S | re.I
    )
    
    fk_inner_pattern = re.compile(
        r"CONSTRAINT\s+\[(?P<constraint>\w+)\]\s+FOREIGN KEY\s*\(\s*\[(?P<col>\w+)\]\s*\)\s+REFERENCES\s+\[(?P<ref_schema>\w+)\]\.\[(?P<ref_table>\w+)\]\s*\(\s*\[(?P<ref_col>\w+)\]\s*\)",
        re.S | re.I
    )
    
    extprop_pattern = re.compile(
        r"EXECUTE\s+\[?sys\]?\.\[?sp_addextendedproperty\]?\s+N?'MS_Description',\s+N?'(.*?)',\s+N?'SCHEMA',\s+\[?(\w+)\]?,\s+N?'TABLE',\s+\[?(\w+)\]?(?:,\s+N?'COLUMN',\s+\[?(\w+)\]?)?",
        re.S | re.I
    )
    
    # === CREATE TABLEs ===
    for schema, table, block in create_table_pattern.findall(ddl_text):
        for line in block.splitlines():
            col_match = column_line_pattern.match(line.strip())
            if col_match:
                col_name, col_type, extras = col_match.groups()
                nullable = 'NOT NULL' not in extras.upper()
                ddl_json["schemas"][schema]["tables"][table]["columns"].append({
                    "name": col_name,
                    "type": col_type.strip(),
                    "nullable": nullable
                })
    
    # === UDTs ===
    for name, base_type in udt_pattern.findall(ddl_text):
        ddl_json["udts"][name] = {
            "type": base_type,
            "nullable": True
        }
    
    # === PRIMARY KEYS ===
    for schema, table, constraint, col_block in pk_pattern.findall(ddl_text):
        cols = [c.strip("[] \n") for c in col_block.split(',')]
        ddl_json["schemas"][schema]["tables"][table]["primary_key"] = cols
    
    # === FOREIGN KEYS ===
    for match in fk_block_pattern.finditer(ddl_text):
        schema = match.group("schema")
        table = match.group("table")
        constraint_text = match.group("constraints")
        for fk in fk_inner_pattern.finditer(constraint_text):
            ddl_json["schemas"][schema]["tables"][table]["foreign_keys"][fk.group("constraint")] = {
                "column": fk.group("col"),
                "references": f"{fk.group('ref_schema')}.{fk.group('ref_table')}.{fk.group('ref_col')}"
            }
    
    # === EXTENDED PROPERTIES (Descriptions) ===
    for match in extprop_pattern.finditer(ddl_text):
        description = match.group(1).strip()
        schema = match.group(2)
        table = match.group(3)
        column = match.group(4) if match.group(4) else None
        if column:
            for col_obj in ddl_json["schemas"][schema]["tables"][table]["columns"]:
                if col_obj["name"] == column:
                    col_obj["description"] = description
                    break
        else:
            ddl_json["schemas"][schema]["tables"][table]["description"] = description
    
    # Convert defaultdicts to regular dicts
    ddl_json = _convert_defaultdict(ddl_json)
    
    return json.dumps(ddl_json, indent=2)


def _convert_defaultdict(obj):
    """Recursively convert defaultdict to dict"""
    if isinstance(obj, defaultdict):
        return {k: _convert_defaultdict(v) for k, v in obj.items()}
    return obj


def extract_tables_from_ddl(ddl_json_str: str):
    """Extract table names from DDL JSON string."""
    data = json.loads(ddl_json_str)
    
    result = {}
    if "schemas" in data:
        for schema, schema_data in data["schemas"].items():
            result[schema] = list(schema_data.get("tables", {}).keys())
    elif isinstance(data, list):
        result["default"] = [table.get("Name") for table in data]
    
    return result