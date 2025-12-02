# src/cdm_builder/ddl_to_lucidchart.py
"""
Convert SQL DDL to LucidChart ERD Import CSV

Parses SQL DDL (CREATE TABLE statements) and generates a CSV file
in the format expected by LucidChart for ERD import.

The output format matches the query:
SELECT 'sqlserver' dbms, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 
       COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
       CONSTRAINT_TYPE, FK_TABLE_SCHEMA, FK_TABLE_NAME, FK_COLUMN_NAME
FROM INFORMATION_SCHEMA...

Usage:
    python -m src.cdm_builder.ddl_to_lucidchart input.sql --output lucidchart.csv
"""

import re
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class Column:
    """Represents a table column."""
    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    is_pk: bool = False
    ordinal: int = 0


@dataclass
class ForeignKey:
    """Represents a foreign key constraint."""
    column_name: str
    ref_schema: str
    ref_table: str
    ref_column: str


@dataclass
class Table:
    """Represents a database table."""
    schema: str
    name: str
    columns: List[Column] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[ForeignKey] = field(default_factory=list)


# =============================================================================
# DDL PARSER
# =============================================================================

class DDLParser:
    """Parse SQL DDL statements."""
    
    def __init__(self, dialect: str = "sqlserver", default_schema: str = "dbo", catalog: str = "CDM"):
        self.dialect = dialect
        self.default_schema = default_schema
        self.catalog = catalog
        self.tables: Dict[str, Table] = {}
    
    def parse(self, ddl: str) -> List[Table]:
        """Parse DDL string and return list of tables."""
        
        # Remove comments
        ddl = self._remove_comments(ddl)
        
        # Parse CREATE TABLE statements
        self._parse_create_tables(ddl)
        
        # Parse ALTER TABLE for foreign keys
        self._parse_alter_tables(ddl)
        
        # Parse inline PRIMARY KEY constraints
        self._parse_inline_constraints(ddl)
        
        return list(self.tables.values())
    
    def _remove_comments(self, ddl: str) -> str:
        """Remove SQL comments."""
        # Remove -- comments
        ddl = re.sub(r'--.*$', '', ddl, flags=re.MULTILINE)
        # Remove /* */ comments
        ddl = re.sub(r'/\*.*?\*/', '', ddl, flags=re.DOTALL)
        return ddl
    
    def _parse_create_tables(self, ddl: str) -> None:
        """Parse CREATE TABLE statements."""
        
        # Pattern for CREATE TABLE [schema.]table_name (...)
        pattern = r'CREATE\s+TABLE\s+(?:(\w+)\.)?(\w+)\s*\((.*?)\)\s*;'
        
        for match in re.finditer(pattern, ddl, re.IGNORECASE | re.DOTALL):
            schema = match.group(1) or self.default_schema
            table_name = match.group(2)
            columns_def = match.group(3)
            
            table = Table(schema=schema, name=table_name)
            self._parse_columns(table, columns_def)
            
            self.tables[f"{schema}.{table_name}"] = table
    
    def _parse_columns(self, table: Table, columns_def: str) -> None:
        """Parse column definitions from CREATE TABLE body."""
        
        # Split by comma, but not commas inside parentheses
        parts = self._split_columns(columns_def)
        ordinal = 0
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Check if this is a constraint (PRIMARY KEY, FOREIGN KEY, CONSTRAINT)
            if re.match(r'^\s*(CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|INDEX)', part, re.IGNORECASE):
                self._parse_table_constraint(table, part)
                continue
            
            # Parse column definition
            col = self._parse_column_def(part)
            if col:
                ordinal += 1
                col.ordinal = ordinal
                table.columns.append(col)
    
    def _split_columns(self, columns_def: str) -> List[str]:
        """Split column definitions, respecting parentheses."""
        parts = []
        current = ""
        depth = 0
        
        for char in columns_def:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                parts.append(current)
                current = ""
            else:
                current += char
        
        if current.strip():
            parts.append(current)
        
        return parts
    
    def _parse_column_def(self, col_def: str) -> Optional[Column]:
        """Parse a single column definition."""
        
        # Pattern: column_name data_type[(length)] [NOT NULL] [NULL] [DEFAULT ...]
        pattern = r'^(\w+)\s+(\w+)(?:\s*\(([^)]+)\))?(.*)$'
        match = re.match(pattern, col_def.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        name = match.group(1)
        data_type = match.group(2).upper()
        type_params = match.group(3)
        modifiers = match.group(4) or ""
        
        # Parse length/precision/scale
        max_length = None
        precision = None
        scale = None
        
        if type_params:
            params = [p.strip() for p in type_params.split(',')]
            if data_type in ('VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR', 'VARBINARY', 'BINARY'):
                if params[0].upper() != 'MAX':
                    max_length = int(params[0])
            elif data_type in ('DECIMAL', 'NUMERIC'):
                precision = int(params[0])
                if len(params) > 1:
                    scale = int(params[1])
        
        # Check nullability
        nullable = 'NOT NULL' not in modifiers.upper()
        
        # Check if PRIMARY KEY inline
        is_pk = 'PRIMARY KEY' in modifiers.upper()
        
        return Column(
            name=name,
            data_type=data_type,
            max_length=max_length,
            precision=precision,
            scale=scale,
            nullable=nullable,
            is_pk=is_pk
        )
    
    def _parse_table_constraint(self, table: Table, constraint_def: str) -> None:
        """Parse table-level constraints (PRIMARY KEY, FOREIGN KEY)."""
        
        # PRIMARY KEY constraint
        pk_pattern = r'(?:CONSTRAINT\s+\w+\s+)?PRIMARY\s+KEY\s*\(([^)]+)\)'
        pk_match = re.search(pk_pattern, constraint_def, re.IGNORECASE)
        if pk_match:
            pk_cols = [c.strip() for c in pk_match.group(1).split(',')]
            table.primary_keys.extend(pk_cols)
            # Mark columns as PK
            for col in table.columns:
                if col.name in pk_cols:
                    col.is_pk = True
        
        # FOREIGN KEY constraint
        fk_pattern = r'(?:CONSTRAINT\s+\w+\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(([^)]+)\)'
        fk_match = re.search(fk_pattern, constraint_def, re.IGNORECASE)
        if fk_match:
            fk_cols = [c.strip() for c in fk_match.group(1).split(',')]
            ref_schema = fk_match.group(2) or self.default_schema
            ref_table = fk_match.group(3)
            ref_cols = [c.strip() for c in fk_match.group(4).split(',')]
            
            for fk_col, ref_col in zip(fk_cols, ref_cols):
                table.foreign_keys.append(ForeignKey(
                    column_name=fk_col,
                    ref_schema=ref_schema,
                    ref_table=ref_table,
                    ref_column=ref_col
                ))
    
    def _parse_alter_tables(self, ddl: str) -> None:
        """Parse ALTER TABLE statements for foreign keys."""
        
        # Pattern: ALTER TABLE [schema.]table ADD CONSTRAINT ... FOREIGN KEY (...) REFERENCES ...
        pattern = r'ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ADD\s+CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(([^)]+)\)'
        
        for match in re.finditer(pattern, ddl, re.IGNORECASE):
            schema = match.group(1) or self.default_schema
            table_name = match.group(2)
            fk_cols = [c.strip() for c in match.group(3).split(',')]
            ref_schema = match.group(4) or self.default_schema
            ref_table = match.group(5)
            ref_cols = [c.strip() for c in match.group(6).split(',')]
            
            table_key = f"{schema}.{table_name}"
            if table_key in self.tables:
                table = self.tables[table_key]
                for fk_col, ref_col in zip(fk_cols, ref_cols):
                    table.foreign_keys.append(ForeignKey(
                        column_name=fk_col,
                        ref_schema=ref_schema,
                        ref_table=ref_table,
                        ref_column=ref_col
                    ))
    
    def _parse_inline_constraints(self, ddl: str) -> None:
        """Mark PK columns from parsed constraints."""
        for table in self.tables.values():
            for pk_col in table.primary_keys:
                for col in table.columns:
                    if col.name.lower() == pk_col.lower():
                        col.is_pk = True


# =============================================================================
# LUCIDCHART CSV GENERATOR
# =============================================================================

class LucidChartCSVGenerator:
    """Generate LucidChart ERD import CSV."""
    
    def __init__(self, dialect: str = "sqlserver", catalog: str = "CDM"):
        self.dialect = dialect
        self.catalog = catalog
    
    def generate(self, tables: List[Table]) -> List[Dict[str, Any]]:
        """Generate CSV rows from parsed tables."""
        
        rows = []
        
        # Build FK lookup for quick reference
        fk_lookup = {}  # (table_key, column_name) -> ForeignKey
        for table in tables:
            table_key = f"{table.schema}.{table.name}"
            for fk in table.foreign_keys:
                fk_lookup[(table_key, fk.column_name.lower())] = fk
        
        for table in tables:
            table_key = f"{table.schema}.{table.name}"
            
            for col in table.columns:
                # Determine constraint type
                constraint_type = None
                fk_schema = None
                fk_table = None
                fk_column = None
                
                if col.is_pk:
                    constraint_type = "PRIMARY KEY"
                
                # Check if this column is a foreign key
                fk = fk_lookup.get((table_key, col.name.lower()))
                if fk:
                    constraint_type = "FOREIGN KEY"
                    fk_schema = fk.ref_schema
                    fk_table = fk.ref_table
                    fk_column = fk.ref_column
                
                # For columns that are both PK and FK, show as FK (LucidChart limitation)
                # Or we could output two rows - but typically FK takes precedence for display
                
                rows.append({
                    "dbms": self.dialect,
                    "TABLE_CATALOG": self.catalog,
                    "TABLE_SCHEMA": table.schema,
                    "TABLE_NAME": table.name,
                    "COLUMN_NAME": col.name,
                    "ORDINAL_POSITION": col.ordinal,
                    "DATA_TYPE": col.data_type,
                    "CHARACTER_MAXIMUM_LENGTH": col.max_length,
                    "CONSTRAINT_TYPE": constraint_type,
                    "FK_TABLE_SCHEMA": fk_schema,
                    "FK_TABLE_NAME": fk_table,
                    "FK_COLUMN_NAME": fk_column
                })
        
        return rows
    
    def write_csv(self, rows: List[Dict[str, Any]], output_file: Path) -> None:
        """Write rows to CSV file."""
        
        fieldnames = [
            "dbms", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
            "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH",
            "CONSTRAINT_TYPE", "FK_TABLE_SCHEMA", "FK_TABLE_NAME", "FK_COLUMN_NAME"
        ]
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def ddl_to_lucidchart(
    ddl_file: Path,
    output_file: Path,
    dialect: str = "sqlserver",
    schema: str = "dbo",
    catalog: str = "CDM"
) -> List[Dict[str, Any]]:
    """
    Convert SQL DDL file to LucidChart CSV.
    
    Args:
        ddl_file: Path to SQL DDL file
        output_file: Output CSV file path
        dialect: SQL dialect (sqlserver, postgresql, mysql)
        schema: Default schema name
        catalog: Database catalog name
    
    Returns:
        List of CSV row dictionaries
    """
    
    # Read DDL
    with open(ddl_file, 'r', encoding='utf-8') as f:
        ddl = f.read()
    
    # Parse DDL
    parser = DDLParser(dialect=dialect, default_schema=schema, catalog=catalog)
    tables = parser.parse(ddl)
    
    print(f"Parsed {len(tables)} tables from DDL")
    
    # Generate CSV
    generator = LucidChartCSVGenerator(dialect=dialect, catalog=catalog)
    rows = generator.generate(tables)
    
    # Write CSV
    generator.write_csv(rows, output_file)
    
    print(f"LucidChart CSV saved to: {output_file}")
    print(f"  Tables: {len(tables)}")
    print(f"  Columns: {len(rows)}")
    
    return rows


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert SQL DDL to LucidChart CSV")
    parser.add_argument("ddl_file", help="Path to SQL DDL file")
    parser.add_argument("--output", "-o", required=True, help="Output CSV file path")
    parser.add_argument("--dialect", "-d", default="sqlserver",
                        choices=["sqlserver", "postgresql", "mysql"],
                        help="SQL dialect (default: sqlserver)")
    parser.add_argument("--schema", "-s", default="dbo", help="Default schema (default: dbo)")
    parser.add_argument("--catalog", "-c", default="CDM", help="Catalog name (default: CDM)")
    
    args = parser.parse_args()
    
    ddl_to_lucidchart(
        ddl_file=Path(args.ddl_file),
        output_file=Path(args.output),
        dialect=args.dialect,
        schema=args.schema,
        catalog=args.catalog
    )