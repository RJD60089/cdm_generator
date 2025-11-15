"""
AWS Glue Catalog Converter
Consolidates columns across multiple Glue jobs, tracking which jobs each column appears in.

This replaces the legacy DDL converter for AWS Glue-specific data sources.
For actual SQL DDL files, a separate ddl_converter.py will be created in the future.
"""
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple


def convert_glue_to_json(file_path: str) -> str:
    """
    Convert AWS Glue catalog JSON to consolidated column format.
    
    Consolidates columns across all Glue jobs, showing which jobs each column appears in.
    This is designed to reduce redundancy and make it easier for LLMs to rationalize
    entities by seeing all column variations in one place.
    
    Args:
        file_path: Path to AWS Glue catalog JSON file
        
    Returns:
        JSON string with consolidated columns and their Glue job sources
        
    Output Format:
        {
          "DatabaseName": "/navitus/bpm/benefitsplanmanagement-analytics",
          "Columns": [
            {
              "Name": "detail_clientid",
              "Type": "int",
              "GJSources": [
                "source_navitus_bpm_account_event",
                "source_navitus_bpm_carrier_event",
                "source_navitus_bpm_subgroup_event"
              ]
            }
          ]
        }
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is not valid JSON or missing required fields
    """
    file = Path(file_path)
    
    if not file.exists():
        raise FileNotFoundError(f"Glue catalog file not found: {file_path}")
    
    # Load and validate JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            glue_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in Glue catalog file: {e}")
    
    if not isinstance(glue_data, list) or len(glue_data) == 0:
        raise ValueError("Glue catalog JSON must be a non-empty array of table definitions")
    
    # Extract database name (should be same for all tables)
    database_name = None
    if glue_data and len(glue_data) > 0:
        database_name = glue_data[0].get('DatabaseName', 'unknown')
    
    # Consolidate columns across all Glue jobs
    # Key: (column_name, column_type) -> list of glue job names
    column_sources: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    
    for table in glue_data:
        glue_job_name = table.get('Name')
        if not glue_job_name:
            continue
            
        columns = table.get('StorageDescriptor', {}).get('Columns', [])
        
        for col in columns:
            col_name = col.get('Name')
            col_type = col.get('Type')
            
            if col_name and col_type:
                key = (col_name, col_type)
                column_sources[key].append(glue_job_name)
    
    # Build output format - sort by column name for consistency
    consolidated_columns = []
    for (col_name, col_type), sources in sorted(column_sources.items()):
        consolidated_columns.append({
            "Name": col_name,
            "Type": col_type,
            "GJSources": sources
        })
    
    output = {
        "DatabaseName": database_name,
        "Columns": consolidated_columns
    }
    
    return json.dumps(output, indent=2)


def extract_tables_from_glue(glue_json_str: str) -> Dict[str, List[str]]:
    """
    Extract table names (Glue job names) from consolidated Glue JSON string.
    
    This is useful for understanding which source tables are available.
    
    Args:
        glue_json_str: JSON string from convert_glue_to_json()
        
    Returns:
        Dict mapping database name to list of unique Glue job names
        
    Example:
        {
          "/navitus/bpm/benefitsplanmanagement-analytics": [
            "source_navitus_bpm_account_event",
            "source_navitus_bpm_carrier_event",
            "source_navitus_bpm_subgroup_event"
          ]
        }
    """
    data = json.loads(glue_json_str)
    
    database_name = data.get("DatabaseName", "unknown")
    all_sources = set()
    
    for column in data.get("Columns", []):
        sources = column.get("GJSources", [])
        all_sources.update(sources)
    
    return {database_name: sorted(list(all_sources))}


# Backwards compatibility - keep old function names as aliases
convert_ddl_to_json = convert_glue_to_json
extract_tables_from_ddl = extract_tables_from_glue