# src/artifacts/word/generate_ddl.py
"""
DDL Generator - Creates SQL DDL from Full CDM

Generates CREATE TABLE statements with:
- Column definitions with data types
- Primary key constraints
- Foreign key constraints
- Comments/descriptions

Supports dialects: SQL Server, PostgreSQL, MySQL
"""

from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

from src.artifacts.common.cdm_extractor import CDMExtractor


# =============================================================================
# TYPE MAPPING
# =============================================================================

TYPE_MAP = {
    "sqlserver": {
        "VARCHAR": "VARCHAR",
        "NVARCHAR": "NVARCHAR",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "DATE": "DATE",
        "DATETIME": "DATETIME2",
        "TIMESTAMP": "DATETIME2",
        "TIME": "TIME",
        "BOOLEAN": "BIT",
        "BOOL": "BIT",
        "TEXT": "NVARCHAR(MAX)",
        "CHAR": "CHAR",
        "BINARY": "VARBINARY",
        "UUID": "UNIQUEIDENTIFIER",
        "JSON": "NVARCHAR(MAX)",
    },
    "postgresql": {
        "VARCHAR": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "DECIMAL": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "FLOAT": "DOUBLE PRECISION",
        "REAL": "REAL",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "TIME",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "TEXT": "TEXT",
        "CHAR": "CHAR",
        "BINARY": "BYTEA",
        "UUID": "UUID",
        "JSON": "JSONB",
    },
    "mysql": {
        "VARCHAR": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "FLOAT": "FLOAT",
        "REAL": "FLOAT",
        "DATE": "DATE",
        "DATETIME": "DATETIME",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "TIME",
        "BOOLEAN": "TINYINT(1)",
        "BOOL": "TINYINT(1)",
        "TEXT": "TEXT",
        "CHAR": "CHAR",
        "BINARY": "BLOB",
        "UUID": "CHAR(36)",
        "JSON": "JSON",
    }
}


def _map_type(data_type: str, dialect: str, max_length: Optional[int] = None,
              precision: Optional[int] = None, scale: Optional[int] = None) -> str:
    """Map CDM data type to dialect-specific type."""
    
    base_type = data_type.upper().split("(")[0].strip()
    type_map = TYPE_MAP.get(dialect, TYPE_MAP["sqlserver"])
    mapped = type_map.get(base_type, "VARCHAR")
    
    # Add length/precision
    if base_type in ("VARCHAR", "NVARCHAR", "CHAR"):
        length = max_length or 255
        return f"{mapped}({length})"
    elif base_type in ("DECIMAL", "NUMERIC"):
        p = precision or 18
        s = scale or 2
        return f"{mapped}({p},{s})"
    elif base_type == "BINARY":
        length = max_length or 255
        if dialect == "sqlserver":
            return f"VARBINARY({length})"
        return mapped
    
    return mapped


def _quote_identifier(name: str, dialect: str) -> str:
    """Quote identifier based on dialect."""
    if dialect == "mysql":
        return f"`{name}`"
    elif dialect == "postgresql":
        return f'"{name}"'
    else:  # sqlserver
        return f"[{name}]"


def generate_ddl(
    extractor: CDMExtractor,
    dialect: str = "sqlserver",
    schema: str = "dbo",
    include_comments: bool = True,
    include_fk: bool = True
) -> str:
    """
    Generate DDL from Full CDM.
    
    Args:
        extractor: CDMExtractor with loaded Full CDM
        dialect: SQL dialect (sqlserver, postgresql, mysql)
        schema: Schema name
        include_comments: Include column comments
        include_fk: Include foreign key constraints
    
    Returns:
        DDL script as string
    """
    
    lines = []
    
    # Header
    lines.append(f"-- ============================================================")
    lines.append(f"-- {extractor.domain} Canonical Data Model - DDL Script")
    lines.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"-- Dialect: {dialect.upper()}")
    lines.append(f"-- Schema: {schema}")
    lines.append(f"-- ============================================================")
    lines.append("")
    
    # Schema creation (if not default)
    if schema and schema.lower() not in ("dbo", "public"):
        if dialect == "postgresql":
            lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        elif dialect == "sqlserver":
            lines.append(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')")
            lines.append(f"    EXEC('CREATE SCHEMA [{schema}]');")
        lines.append("")
    
    entities = extractor.get_entities()
    all_attributes = extractor.get_all_attributes()
    relationships = extractor.get_relationships()
    
    # Group attributes by entity
    attrs_by_entity = {}
    for attr in all_attributes:
        if attr.entity_name not in attrs_by_entity:
            attrs_by_entity[attr.entity_name] = []
        attrs_by_entity[attr.entity_name].append(attr)
    
    # Create tables
    for entity in entities:
        entity_name = entity.name
        q_table = _quote_identifier(entity_name, dialect)
        q_schema = _quote_identifier(schema, dialect) if schema else ""
        full_table = f"{q_schema}.{q_table}" if schema else q_table
        
        lines.append(f"-- ------------------------------------------------------------")
        lines.append(f"-- Table: {entity_name}")
        if entity.description:
            lines.append(f"-- {entity.description[:80]}")
        lines.append(f"-- ------------------------------------------------------------")
        
        lines.append(f"CREATE TABLE {full_table} (")
        
        # Columns
        col_lines = []
        attrs = attrs_by_entity.get(entity_name, [])
        
        for attr in attrs:
            col_name = _quote_identifier(attr.attribute_name, dialect)
            col_type = _map_type(
                attr.data_type,
                dialect,
                attr.max_length,
                attr.precision,
                attr.scale
            )
            
            null_str = "NOT NULL" if attr.required or attr.pk else "NULL"
            
            col_def = f"    {col_name} {col_type} {null_str}"
            col_lines.append(col_def)
        
        # Primary key constraint
        if entity.primary_keys:
            pk_cols = ", ".join(_quote_identifier(pk, dialect) for pk in entity.primary_keys)
            pk_name = f"PK_{entity_name}"
            col_lines.append(f"    CONSTRAINT {_quote_identifier(pk_name, dialect)} PRIMARY KEY ({pk_cols})")
        
        lines.append(",\n".join(col_lines))
        lines.append(");")
        lines.append("")
    
    # Foreign keys (separate for dependency order)
    if include_fk and relationships:
        lines.append("")
        lines.append("-- ============================================================")
        lines.append("-- Foreign Key Constraints")
        lines.append("-- ============================================================")
        lines.append("")
        
        for rel in relationships:
            child_table = _quote_identifier(rel.child_entity, dialect)
            parent_table = _quote_identifier(rel.parent_entity, dialect)
            fk_col = _quote_identifier(rel.foreign_key, dialect)
            pk_col = _quote_identifier(rel.parent_key, dialect)
            
            q_schema = _quote_identifier(schema, dialect) if schema else ""
            child_full = f"{q_schema}.{child_table}" if schema else child_table
            parent_full = f"{q_schema}.{parent_table}" if schema else parent_table
            
            fk_name = f"FK_{rel.child_entity}_{rel.foreign_key}"
            
            lines.append(f"ALTER TABLE {child_full}")
            lines.append(f"    ADD CONSTRAINT {_quote_identifier(fk_name, dialect)}")
            lines.append(f"    FOREIGN KEY ({fk_col})")
            lines.append(f"    REFERENCES {parent_full} ({pk_col});")
            lines.append("")
    
    # Comments (for PostgreSQL and SQL Server)
    if include_comments and dialect in ("postgresql", "sqlserver"):
        lines.append("")
        lines.append("-- ============================================================")
        lines.append("-- Column Comments/Descriptions")
        lines.append("-- ============================================================")
        lines.append("")
        
        for attr in all_attributes:
            if not attr.description:
                continue
            
            desc = attr.description.replace("'", "''")[:500]
            
            if dialect == "postgresql":
                lines.append(f"COMMENT ON COLUMN {schema}.{_quote_identifier(attr.entity_name, dialect)}.{_quote_identifier(attr.attribute_name, dialect)} IS '{desc}';")
            elif dialect == "sqlserver":
                lines.append(f"EXEC sp_addextendedproperty")
                lines.append(f"    @name = N'MS_Description',")
                lines.append(f"    @value = N'{desc}',")
                lines.append(f"    @level0type = N'SCHEMA', @level0name = N'{schema}',")
                lines.append(f"    @level1type = N'TABLE', @level1name = N'{attr.entity_name}',")
                lines.append(f"    @level2type = N'COLUMN', @level2name = N'{attr.attribute_name}';")
                lines.append("")
    
    lines.append("")
    lines.append("-- End of DDL Script")
    
    return "\n".join(lines)


def generate_ddl_file(
    extractor: CDMExtractor,
    outdir: Path,
    domain: str,
    dialect: str = "sqlserver",
    schema: str = "dbo"
) -> Path:
    """
    Generate DDL file and save to output directory.
    
    Args:
        extractor: CDMExtractor with loaded Full CDM
        outdir: Base output directory
        domain: Domain name (for filename)
        dialect: SQL dialect
        schema: Schema name
    
    Returns:
        Path to generated DDL file
    """
    
    ddl_content = generate_ddl(extractor, dialect, schema)
    
    artifacts_dir = outdir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = domain.lower().replace(' ', '_')
    ddl_file = artifacts_dir / f"ddl_{domain_safe}_{timestamp}.sql"
    
    with open(ddl_file, 'w', encoding='utf-8') as f:
        f.write(ddl_content)
    
    return ddl_file