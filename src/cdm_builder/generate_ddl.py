# src/cdm_builder/generate_ddl.py
"""
Generate SQL DDL from CDM JSON

Converts a minimal CDM JSON structure into SQL DDL statements.
Supports multiple SQL dialects (SQL Server, PostgreSQL, MySQL).

Input: CDM JSON with entities, attributes, relationships
Output: SQL DDL file with CREATE TABLE statements

Usage:
    python -m src.cdm_builder.generate_ddl cdm_file.json --dialect sqlserver --output output.sql
"""

import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional


# =============================================================================
# DATA TYPE MAPPINGS
# =============================================================================

# CDM logical types → SQL dialect types
TYPE_MAPPINGS = {
    "sqlserver": {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "NVARCHAR(MAX)",
        "STRING": "VARCHAR",
        "INTEGER": "INT",
        "INT": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "DOUBLE": "FLOAT",
        "BOOLEAN": "BIT",
        "BOOL": "BIT",
        "DATE": "DATE",
        "TIME": "TIME",
        "DATETIME": "DATETIME2",
        "TIMESTAMP": "DATETIME2",
        "BINARY": "VARBINARY",
        "BLOB": "VARBINARY(MAX)",
        "UUID": "UNIQUEIDENTIFIER",
        "JSON": "NVARCHAR(MAX)",
    },
    "postgresql": {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "STRING": "VARCHAR",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "FLOAT": "REAL",
        "REAL": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIME": "TIME",
        "DATETIME": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "BINARY": "BYTEA",
        "BLOB": "BYTEA",
        "UUID": "UUID",
        "JSON": "JSONB",
    },
    "mysql": {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "STRING": "VARCHAR",
        "INTEGER": "INT",
        "INT": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "FLOAT": "FLOAT",
        "REAL": "FLOAT",
        "DOUBLE": "DOUBLE",
        "BOOLEAN": "TINYINT(1)",
        "BOOL": "TINYINT(1)",
        "DATE": "DATE",
        "TIME": "TIME",
        "DATETIME": "DATETIME",
        "TIMESTAMP": "TIMESTAMP",
        "BINARY": "BLOB",
        "BLOB": "LONGBLOB",
        "UUID": "CHAR(36)",
        "JSON": "JSON",
    }
}


# =============================================================================
# DDL GENERATOR CLASS
# =============================================================================

class DDLGenerator:
    """Generate SQL DDL from CDM JSON structure."""
    
    def __init__(self, dialect: str = "sqlserver", schema: str = "dbo", catalog: str = "CDM"):
        self.dialect = dialect.lower()
        self.schema = schema
        self.catalog = catalog
        self.type_map = TYPE_MAPPINGS.get(self.dialect, TYPE_MAPPINGS["sqlserver"])
        
    def generate(self, cdm: Dict[str, Any]) -> str:
        """Generate complete DDL from CDM JSON."""
        
        lines = []
        
        # Header
        lines.append(self._header(cdm))
        
        # Drop tables (in reverse order for FK dependencies)
        entities = cdm.get("entities", [])
        lines.append(self._drop_tables(entities))
        
        # Create tables
        for entity in entities:
            lines.append(self._create_table(entity))
        
        # Add foreign keys (after all tables created)
        for entity in entities:
            fk_statements = self._foreign_keys(entity)
            if fk_statements:
                lines.append(fk_statements)
        
        return "\n".join(lines)
    
    def _header(self, cdm: Dict[str, Any]) -> str:
        """Generate DDL header comment."""
        domain = cdm.get("domain", "Unknown")
        version = cdm.get("cdm_version", "1.0")
        timestamp = datetime.now().isoformat()
        
        return f"""-- =============================================================================
-- CDM DDL: {domain}
-- Version: {version}
-- Generated: {timestamp}
-- Dialect: {self.dialect}
-- Schema: {self.schema}
-- =============================================================================

"""
    
    def _drop_tables(self, entities: List[Dict]) -> str:
        """Generate DROP TABLE statements."""
        lines = ["-- Drop existing tables (reverse order for FK dependencies)"]
        
        # Reverse order to handle foreign key dependencies
        for entity in reversed(entities):
            table_name = self._table_name(entity.get("entity_name", ""))
            
            if self.dialect == "sqlserver":
                lines.append(f"IF OBJECT_ID('{self.schema}.{table_name}', 'U') IS NOT NULL DROP TABLE {self.schema}.{table_name};")
            elif self.dialect == "postgresql":
                lines.append(f"DROP TABLE IF EXISTS {self.schema}.{table_name} CASCADE;")
            else:  # mysql
                lines.append(f"DROP TABLE IF EXISTS {table_name};")
        
        lines.append("")
        return "\n".join(lines)
    
    def _create_table(self, entity: Dict[str, Any]) -> str:
        """Generate CREATE TABLE statement for an entity."""
        
        entity_name = entity.get("entity_name", "Unknown")
        table_name = self._table_name(entity_name)
        description = entity.get("description", "")
        attributes = entity.get("attributes", [])
        
        lines = []
        lines.append(f"-- {entity_name}: {description[:80]}{'...' if len(description) > 80 else ''}")
        lines.append(f"CREATE TABLE {self.schema}.{table_name} (")
        
        # Columns
        column_defs = []
        pk_columns = []
        
        for attr in attributes:
            col_def = self._column_definition(attr)
            column_defs.append(f"    {col_def}")
            
            if attr.get("pk") or attr.get("primary_key"):
                pk_columns.append(self._column_name(attr.get("name") or attr.get("attribute_name", "")))
        
        # Primary key constraint
        if pk_columns:
            pk_name = f"PK_{table_name}"
            column_defs.append(f"    CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk_columns)})")
        
        lines.append(",\n".join(column_defs))
        lines.append(");")
        lines.append("")
        
        return "\n".join(lines)
    
    def _column_definition(self, attr: Dict[str, Any]) -> str:
        """Generate column definition from attribute."""
        
        name = self._column_name(attr.get("name") or attr.get("attribute_name", ""))
        data_type = self._map_data_type(attr)
        nullable = attr.get("nullable", True)
        required = attr.get("required", False)
        default = attr.get("default_value")
        
        parts = [name, data_type]
        
        # NULL / NOT NULL
        if required or not nullable:
            parts.append("NOT NULL")
        else:
            parts.append("NULL")
        
        # Default value
        if default is not None:
            if isinstance(default, str):
                parts.append(f"DEFAULT '{default}'")
            elif isinstance(default, bool):
                parts.append(f"DEFAULT {1 if default else 0}")
            else:
                parts.append(f"DEFAULT {default}")
        
        return " ".join(parts)
    
    def _map_data_type(self, attr: Dict[str, Any]) -> str:
        """Map CDM data type to SQL dialect type."""
        
        cdm_type = (attr.get("type") or attr.get("data_type", "VARCHAR")).upper()
        
        # Extract base type and length/precision
        base_type = cdm_type
        length = attr.get("max_length") or attr.get("length")
        precision = attr.get("precision")
        scale = attr.get("scale")
        
        # Handle types like VARCHAR(50) already in the input
        if "(" in cdm_type:
            base_type = cdm_type.split("(")[0]
            # Extract length from type string if not provided separately
            if not length:
                try:
                    length = int(cdm_type.split("(")[1].rstrip(")").split(",")[0])
                except:
                    pass
        
        # Map to dialect
        sql_type = self.type_map.get(base_type, base_type)
        
        # Add length/precision
        if base_type in ("VARCHAR", "CHAR", "STRING", "BINARY") and length:
            return f"{sql_type}({length})"
        elif base_type in ("DECIMAL", "NUMERIC") and precision:
            if scale:
                return f"{sql_type}({precision},{scale})"
            return f"{sql_type}({precision})"
        elif base_type == "VARCHAR" and not length:
            return f"{sql_type}(255)"  # Default length
        
        return sql_type
    
    def _foreign_keys(self, entity: Dict[str, Any]) -> str:
        """Generate ALTER TABLE statements for foreign keys."""
        
        relationships = entity.get("relationships", [])
        if not relationships:
            return ""
        
        table_name = self._table_name(entity.get("entity_name", ""))
        lines = []
        
        for rel in relationships:
            fk_column = rel.get("fk") or rel.get("foreign_key")
            to_entity = rel.get("to") or rel.get("to_entity")
            to_column = rel.get("to_column") or rel.get("fk")  # Usually same as FK column
            
            if not fk_column or not to_entity:
                continue
            
            to_table = self._table_name(to_entity)
            fk_name = f"FK_{table_name}_{to_table}"
            fk_col = self._column_name(fk_column)
            to_col = self._column_name(to_column or fk_column)
            
            lines.append(
                f"ALTER TABLE {self.schema}.{table_name} "
                f"ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({fk_col}) REFERENCES {self.schema}.{to_table}({to_col});"
            )
        
        if lines:
            lines.insert(0, f"-- Foreign keys for {entity.get('entity_name', '')}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _table_name(self, entity_name: str) -> str:
        """Convert entity name to table name."""
        # Convert PascalCase/camelCase to snake_case, or just use as-is
        return entity_name.replace(" ", "_")
    
    def _column_name(self, attr_name: str) -> str:
        """Convert attribute name to column name."""
        return attr_name.replace(" ", "_").lower()


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def generate_ddl(
    cdm_file: Path,
    output_file: Optional[Path] = None,
    dialect: str = "sqlserver",
    schema: str = "dbo",
    catalog: str = "CDM"
) -> str:
    """
    Generate DDL from CDM JSON file.
    
    Args:
        cdm_file: Path to CDM JSON file
        output_file: Optional output path (if None, returns string)
        dialect: SQL dialect (sqlserver, postgresql, mysql)
        schema: Database schema name
        catalog: Database catalog name
    
    Returns:
        DDL string
    """
    
    # Load CDM
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    # Generate DDL
    generator = DDLGenerator(dialect=dialect, schema=schema, catalog=catalog)
    ddl = generator.generate(cdm)
    
    # Save if output specified
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(ddl)
        print(f"DDL saved to: {output_file}")
    
    return ddl


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL DDL from CDM JSON")
    parser.add_argument("cdm_file", help="Path to CDM JSON file")
    parser.add_argument("--output", "-o", help="Output DDL file path")
    parser.add_argument("--dialect", "-d", default="sqlserver", 
                        choices=["sqlserver", "postgresql", "mysql"],
                        help="SQL dialect (default: sqlserver)")
    parser.add_argument("--schema", "-s", default="dbo", help="Schema name (default: dbo)")
    parser.add_argument("--catalog", "-c", default="CDM", help="Catalog name (default: CDM)")
    
    args = parser.parse_args()
    
    output_path = Path(args.output) if args.output else None
    
    ddl = generate_ddl(
        cdm_file=Path(args.cdm_file),
        output_file=output_path,
        dialect=args.dialect,
        schema=args.schema,
        catalog=args.catalog
    )
    
    if not output_path:
        print(ddl)