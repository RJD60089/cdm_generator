# src/artifacts/excel/tab_cross_reference.py
"""
Cross-Reference Tab Generator

Shows source lineage for each attribute - maps CDM attributes back to 
original source fields (Guardrails, Glue, NCPDP, FHIR).
"""

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from typing import List, Dict, Any

from src.artifacts.common.cdm_extractor import CDMExtractor


# Styles
HEADER_FONT = Font(bold=True, color="FFFFFF")
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style='thin'),
    right=Side(style='thin'),
    top=Side(style='thin'),
    bottom=Side(style='thin')
)


def _extract_source_field(lineage: Dict, source_type: str) -> str:
    """Extract source field name from lineage data."""
    source_data = lineage.get(source_type, [])
    
    if not source_data:
        return ""
    
    # Handle list of mappings
    if isinstance(source_data, list):
        fields = []
        for mapping in source_data:
            if isinstance(mapping, dict):
                # CDM structure uses source_entity + source_attribute
                entity = mapping.get("source_entity", "")
                attr = mapping.get("source_attribute", "")
                
                if entity and attr:
                    fields.append(f"{entity}.{attr}")
                elif attr:
                    fields.append(attr)
                elif entity:
                    fields.append(entity)
            elif isinstance(mapping, str):
                fields.append(mapping)
        
        return "; ".join(fields) if fields else ""
    
    # Handle dict directly
    elif isinstance(source_data, dict):
        entity = source_data.get("source_entity", "")
        attr = source_data.get("source_attribute", "")
        
        if entity and attr:
            return f"{entity}.{attr}"
        return attr or entity or ""
    
    # Handle string directly
    elif isinstance(source_data, str):
        return source_data
    
    return ""


def create_cross_reference_tab(wb: Workbook, extractor: CDMExtractor) -> Worksheet:
    """
    Create Cross-Reference tab showing attribute-to-source mappings.
    
    Columns:
    - Entity
    - Attribute
    - Data Type
    - Guardrails Source
    - Glue Source
    - NCPDP Source
    - FHIR Source
    """
    
    ws = wb.create_sheet("Cross-Reference")
    
    # Headers
    headers = [
        "Entity",
        "Attribute", 
        "Data Type",
        "Guardrails Source",
        "Glue Source",
        "NCPDP Source",
        "FHIR Source"
    ]
    
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER
    
    # Data rows
    row = 2
    attributes = extractor.get_all_attributes()
    
    for attr in attributes:
        lineage = attr.source_lineage or {}
        
        data = [
            attr.entity_name,
            attr.attribute_name,
            attr.data_type,
            _extract_source_field(lineage, "guardrails"),
            _extract_source_field(lineage, "glue"),
            _extract_source_field(lineage, "ncpdp"),
            _extract_source_field(lineage, "fhir")
        ]
        
        for col, value in enumerate(data, 1):
            cell = ws.cell(row=row, column=col, value=value)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="center", wrap_text=True)
        
        row += 1
    
    # Column widths
    col_widths = {
        "A": 20,  # Entity
        "B": 25,  # Attribute
        "C": 15,  # Data Type
        "D": 35,  # Guardrails
        "E": 35,  # Glue
        "F": 35,  # NCPDP
        "G": 35,  # FHIR
    }
    
    for col, width in col_widths.items():
        ws.column_dimensions[col].width = width
    
    # Freeze header row
    ws.freeze_panes = "A2"
    
    # Add autofilter
    ws.auto_filter.ref = f"A1:G{row - 1}"
    
    return ws
