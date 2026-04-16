"""
DDL converter.
Handles both JSON (pass through) and SQL DDL files (convert).

Supports two SQL dialects:
  - SQL Server: CREATE TABLE [schema].[table] (...)
  - Oracle:     CREATE TABLE "schema"."table" (...)

Auto-detects dialect from quoting style in the DDL text.
"""
import json
import re
from pathlib import Path
from collections import defaultdict


def convert_ddl_to_json(file_path: str) -> str:
    """
    Convert DDL to JSON string.
    - JSON files: Pass through as-is
    - SQL files: Convert to JSON (auto-detects SQL Server vs Oracle)

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

    elif file.suffix in ('.sql', '.ddl', '.txt'):
        # SQL - convert to JSON (supports .sql, .ddl, .txt extensions)
        return _convert_sql_ddl(file_path)

    else:
        raise ValueError(f"Unsupported DDL format: {file.suffix}. Use .json, .sql, .ddl, or .txt")


def _load_ddl_text(file_path: str) -> str:
    """Load DDL file with encoding fallback."""
    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            with open(file_path, "r", encoding=enc) as f:
                return f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Unable to read DDL file with any supported encoding: {file_path}")


def _detect_dialect(ddl_text: str) -> str:
    """Auto-detect SQL dialect from quoting style.

    Returns 'oracle' or 'sqlserver'.
    """
    # Count occurrences of each quoting style
    bracket_count = len(re.findall(r'CREATE TABLE\s+\[', ddl_text, re.I))
    quote_count = len(re.findall(r'CREATE TABLE\s+"', ddl_text, re.I))

    if quote_count > bracket_count:
        return "oracle"
    return "sqlserver"


def _clean_ddl(ddl_text: str) -> str:
    """Strip comments from DDL text."""
    ddl_text = re.sub(r'--.*', '', ddl_text)
    ddl_text = re.sub(r'/\*.*?\*/', '', ddl_text, flags=re.S)
    return ddl_text


def _make_empty_ddl_json(file_path: str) -> dict:
    """Create base JSON structure for DDL output."""
    return {
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


# ============================================================================
# IDENTIFIER HELPERS — normalise both [bracket] and "double-quote" styles
# ============================================================================

def _unquote(identifier: str) -> str:
    """Remove surrounding quotes or brackets from an identifier.

    Examples:
        '[FOO]'  -> 'FOO'
        '"FOO"'  -> 'FOO'
        'FOO'    -> 'FOO'
    """
    s = identifier.strip()
    if s.startswith("[") and s.endswith("]"):
        return s[1:-1]
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


# Universal identifier pattern: matches [name], "name", or bare name
_ID = r'(?:\[(\w+)\]|"(\w+)"|(\w+))'


def _id(match_groups: tuple) -> str:
    """Extract identifier from a group of 3 alternatives."""
    for g in match_groups:
        if g:
            return g
    return ""


# ============================================================================
# SQL SERVER PARSER
# ============================================================================

def _parse_sqlserver(ddl_text: str, ddl_json: dict) -> None:
    """Parse SQL Server DDL (bracket-quoted identifiers)."""

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
        re.S | re.I)

    fk_inner_pattern = re.compile(
        r"CONSTRAINT\s+\[(?P<constraint>\w+)\]\s+FOREIGN KEY\s*\(\s*\[(?P<col>\w+)\]\s*\)\s+REFERENCES\s+\[(?P<ref_schema>\w+)\]\.\[(?P<ref_table>\w+)\]\s*\(\s*\[(?P<ref_col>\w+)\]\s*\)",
        re.S | re.I)

    extprop_pattern = re.compile(
        r"EXECUTE\s+\[?sys\]?\.\[?sp_addextendedproperty\]?\s+N?'MS_Description',\s+N?'(.*?)',\s+N?'SCHEMA',\s+\[?(\w+)\]?,\s+N?'TABLE',\s+\[?(\w+)\]?(?:,\s+N?'COLUMN',\s+\[?(\w+)\]?)?",
        re.S | re.I)

    # CREATE TABLEs
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

    # UDTs
    for name, base_type in udt_pattern.findall(ddl_text):
        ddl_json["udts"][name] = {"type": base_type, "nullable": True}

    # PRIMARY KEYS
    for schema, table, constraint, col_block in pk_pattern.findall(ddl_text):
        cols = [c.strip("[] \n") for c in col_block.split(',')]
        ddl_json["schemas"][schema]["tables"][table]["primary_key"] = cols

    # FOREIGN KEYS
    for match in fk_block_pattern.finditer(ddl_text):
        schema = match.group("schema")
        table = match.group("table")
        constraint_text = match.group("constraints")
        for fk in fk_inner_pattern.finditer(constraint_text):
            ddl_json["schemas"][schema]["tables"][table]["foreign_keys"][fk.group("constraint")] = {
                "column": fk.group("col"),
                "references": f"{fk.group('ref_schema')}.{fk.group('ref_table')}.{fk.group('ref_col')}"
            }

    # EXTENDED PROPERTIES (Descriptions)
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


# ============================================================================
# ORACLE PARSER
# ============================================================================

def _parse_oracle(ddl_text: str, ddl_json: dict) -> None:
    """Parse Oracle DDL (double-quoted or bare identifiers).

    Handles:
      - CREATE TABLE "SCHEMA"."TABLE" ( ... )
      - CREATE TABLE SCHEMA.TABLE ( ... )
      - Column definitions with Oracle types (NUMBER, VARCHAR2, DATE, etc.)
      - NOT NULL ENABLE / NOT NULL constraints
      - DEFAULT values (extracted but stored as default_value)
      - Inline PRIMARY KEY / CONSTRAINT ... PRIMARY KEY in CREATE TABLE
      - ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY
      - ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES
      - COMMENT ON TABLE / COMMENT ON COLUMN
    """
    # --- Identifier pattern: "quoted" or bare ---
    _QID = r'(?:"(\w+)"|(\w+))'  # Captures into 2 groups

    def _qid(groups: tuple) -> str:
        """Extract identifier from quoted-or-bare pair."""
        return groups[0] if groups[0] else groups[1] if len(groups) > 1 and groups[1] else ""

    # ─── CREATE TABLE ────────────────────────────────────────────
    # Matches: CREATE TABLE "PC2"."ADDENDUM" ( ... )
    # The closing ) is followed by SEGMENT CREATION, PCTFREE, TABLESPACE, or ;
    # We use a non-greedy match that terminates at ) followed by a storage keyword or ;
    create_table_pattern = re.compile(
        r'CREATE\s+TABLE\s+' + _QID + r'\s*\.\s*' + _QID +
        r'\s*\(\s*(.*?)\)\s*(?:SEGMENT\s|PCTFREE\s|TABLESPACE\s|ORGANIZATION\s|;)',
        re.S | re.I
    )

    # Column line pattern — handles Oracle types like:
    #   "ADDENDUM_SK" NUMBER NOT NULL ENABLE,
    #   "SPECIFICATION" VARCHAR2(500),
    #   "DAY_SUPPLY_MIN" NUMBER(15,5),
    #   "REC_CREATE_DT" DATE DEFAULT SYSDATE NOT NULL,
    oracle_col_pattern = re.compile(
        r'"?(\w+)"?\s+'                          # column name (quoted or bare)
        r'(\w+(?:\([^)]*\))?)'                   # data type with optional precision
        r'(.*?)$',                                # remainder (constraints, defaults)
        re.I
    )

    # ─── ALTER TABLE ... PRIMARY KEY ─────────────────────────────
    oracle_pk_pattern = re.compile(
        r'ALTER\s+TABLE\s+' + _QID + r'\s*\.\s*' + _QID +
        r'\s+ADD\s+CONSTRAINT\s+' + _QID +
        r'\s+PRIMARY\s+KEY\s*\(\s*(.*?)\s*\)',
        re.S | re.I
    )

    # ─── Inline CONSTRAINT ... PRIMARY KEY within CREATE TABLE ───
    inline_pk_pattern = re.compile(
        r'CONSTRAINT\s+' + _QID + r'\s+PRIMARY\s+KEY\s*\(\s*(.*?)\s*\)',
        re.S | re.I
    )

    # ─── ALTER TABLE ... FOREIGN KEY ... REFERENCES ──────────────
    oracle_fk_pattern = re.compile(
        r'ALTER\s+TABLE\s+' + _QID + r'\s*\.\s*' + _QID +
        r'\s+ADD\s+CONSTRAINT\s+' + _QID +
        r'\s+FOREIGN\s+KEY\s*\(\s*' + _QID + r'\s*\)'
        r'\s+REFERENCES\s+' + _QID + r'\s*\.\s*' + _QID +
        r'\s*\(\s*' + _QID + r'\s*\)',
        re.S | re.I
    )

    # ─── COMMENT ON TABLE / COLUMN ───────────────────────────────
    comment_table_pattern = re.compile(
        r"COMMENT\s+ON\s+TABLE\s+" + _QID + r'\s*\.\s*' + _QID +
        r"\s+IS\s+'((?:[^']|'')*?)'\s*;",
        re.S | re.I
    )
    comment_column_pattern = re.compile(
        r"COMMENT\s+ON\s+COLUMN\s+" + _QID + r'\s*\.\s*' + _QID +
        r'\s*\.\s*' + _QID +
        r"\s+IS\s+'((?:[^']|'')*?)'\s*;",
        re.S | re.I
    )

    # === CREATE TABLEs ===
    for match in create_table_pattern.finditer(ddl_text):
        schema = _qid((match.group(1), match.group(2)))
        table = _qid((match.group(3), match.group(4)))
        block = match.group(5)

        for line in block.splitlines():
            stripped = line.strip().rstrip(',')
            if not stripped:
                continue

            # Skip inline constraint lines
            upper = stripped.upper()
            if upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY") or upper.startswith("FOREIGN KEY") or upper.startswith("CHECK") or upper.startswith("UNIQUE"):
                # But still extract inline PK from this block
                continue

            col_match = oracle_col_pattern.match(stripped)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2).strip()
                extras = col_match.group(3) or ""

                # Skip if the "column name" is actually a keyword (constraint, etc.)
                if col_name.upper() in ("CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE",
                                         "CHECK", "INDEX", "USING", "TABLESPACE",
                                         "STORAGE", "PCTFREE", "LOGGING", "NOCOMPRESS",
                                         "SUPPLEMENTAL"):
                    continue

                nullable = "NOT NULL" not in extras.upper()

                # Extract DEFAULT value
                default_value = None
                default_match = re.search(r'DEFAULT\s+(.+?)(?:\s+NOT\s+NULL|\s+NULL|\s+ENABLE|\s*$)',
                                          extras, re.I)
                if default_match:
                    default_value = default_match.group(1).strip().rstrip(',')

                col_entry = {
                    "name": col_name,
                    "type": col_type,
                    "nullable": nullable,
                }
                if default_value:
                    col_entry["default"] = default_value

                ddl_json["schemas"][schema]["tables"][table]["columns"].append(col_entry)

        # Check for inline PK in the CREATE TABLE block
        for ipk_match in inline_pk_pattern.finditer(block):
            pk_cols_str = ipk_match.group(3) if ipk_match.group(3) else ipk_match.group(4)
            pk_cols = [_unquote(c.strip()) for c in pk_cols_str.split(',')]
            ddl_json["schemas"][schema]["tables"][table]["primary_key"] = pk_cols

    # === ALTER TABLE ... PRIMARY KEY ===
    for match in oracle_pk_pattern.finditer(ddl_text):
        schema = _qid((match.group(1), match.group(2)))
        table = _qid((match.group(3), match.group(4)))
        # constraint name is groups 5,6
        col_block = match.group(7) if match.group(7) else match.group(8) if len(match.groups()) > 7 else ""
        # The col_block is captured after the 3 _QID patterns (6 groups) + 1 capture
        # Let's re-extract from the raw match
        pk_cols_raw = match.groups()[-1]  # Last group is the column list
        pk_cols = [_unquote(c.strip()) for c in pk_cols_raw.split(',')]
        ddl_json["schemas"][schema]["tables"][table]["primary_key"] = pk_cols

    # === ALTER TABLE ... FOREIGN KEY ===
    for match in oracle_fk_pattern.finditer(ddl_text):
        groups = match.groups()
        # Groups: schema(2), table(2), constraint(2), fk_col(2), ref_schema(2), ref_table(2), ref_col(2) = 14 groups
        schema = _qid((groups[0], groups[1]))
        table = _qid((groups[2], groups[3]))
        constraint = _qid((groups[4], groups[5]))
        fk_col = _qid((groups[6], groups[7]))
        ref_schema = _qid((groups[8], groups[9]))
        ref_table = _qid((groups[10], groups[11]))
        ref_col = _qid((groups[12], groups[13]))

        ddl_json["schemas"][schema]["tables"][table]["foreign_keys"][constraint] = {
            "column": fk_col,
            "references": f"{ref_schema}.{ref_table}.{ref_col}"
        }

    # === COMMENT ON TABLE ===
    for match in comment_table_pattern.finditer(ddl_text):
        schema = _qid((match.group(1), match.group(2)))
        table = _qid((match.group(3), match.group(4)))
        description = match.group(5).replace("''", "'").strip()
        ddl_json["schemas"][schema]["tables"][table]["description"] = description

    # === COMMENT ON COLUMN ===
    for match in comment_column_pattern.finditer(ddl_text):
        schema = _qid((match.group(1), match.group(2)))
        table = _qid((match.group(3), match.group(4)))
        column = _qid((match.group(5), match.group(6)))
        description = match.group(7).replace("''", "'").strip()
        for col_obj in ddl_json["schemas"][schema]["tables"][table]["columns"]:
            if col_obj["name"].upper() == column.upper():
                col_obj["description"] = description
                break


# ============================================================================
# MAIN CONVERTER
# ============================================================================

def _convert_sql_ddl(file_path: str) -> str:
    """
    Convert SQL DDL to JSON format.
    Auto-detects SQL Server vs Oracle dialect and dispatches accordingly.
    """
    ddl_text = _load_ddl_text(file_path)
    ddl_text = _clean_ddl(ddl_text)

    ddl_json = _make_empty_ddl_json(file_path)

    dialect = _detect_dialect(ddl_text)

    if dialect == "oracle":
        _parse_oracle(ddl_text, ddl_json)
    else:
        _parse_sqlserver(ddl_text, ddl_json)

    # Convert defaultdicts to regular dicts
    ddl_json = _convert_defaultdict(ddl_json)

    # Remove empty tables (views/other objects that matched the regex but have no columns)
    for schema_data in ddl_json.get("schemas", {}).values():
        tables = schema_data.get("tables", {})
        empty = [name for name, tdata in tables.items() if not tdata.get("columns")]
        for name in empty:
            del tables[name]

    # Report what was parsed
    total_tables = sum(
        len(schema_data.get("tables", {}))
        for schema_data in ddl_json.get("schemas", {}).values()
    )
    total_cols = sum(
        len(table_data.get("columns", []))
        for schema_data in ddl_json.get("schemas", {}).values()
        for table_data in schema_data.get("tables", {}).values()
    )
    print(f"   DDL converter: {dialect} dialect, {total_tables} tables, {total_cols} columns")

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