# src/artifacts/excel/tab_business_rules.py
"""Generate Business Rules tab for Excel CDM."""

from openpyxl import Workbook
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


def create_business_rules_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Business Rules tab.
    
    Columns:
    - Entity, Attribute, Rule Type, Rule Description, Source(s)
    """
    
    ws = wb.create_sheet("Business_Rules")
    
    # Headers
    headers = [
        "Entity", "Attribute", "Rule Type", 
        "Rule Description", "Source(s)"
    ]
    
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        ExcelStyles.apply_header_style(cell)
    
    # Data rows - collect all rules
    row_idx = 2
    
    for attr in extractor.get_all_attributes():
        # Business rules
        for rule in attr.business_rules:
            is_alt = row_idx % 2 == 0
            
            row_data = [
                attr.entity_name,
                attr.attribute_name,
                "Business",
                rule,
                "Various"  # Could extract from source_lineage
            ]
            
            for col, value in enumerate(row_data, 1):
                cell = ws.cell(row=row_idx, column=col, value=value)
                ExcelStyles.apply_body_style(cell, is_alt)
            
            row_idx += 1
        
        # Validation rules
        for rule in attr.validation_rules:
            is_alt = row_idx % 2 == 0
            
            row_data = [
                attr.entity_name,
                attr.attribute_name,
                "Validation",
                rule,
                "Various"
            ]
            
            for col, value in enumerate(row_data, 1):
                cell = ws.cell(row=row_idx, column=col, value=value)
                ExcelStyles.apply_body_style(cell, is_alt)
            
            row_idx += 1
    
    # Column widths
    widths = {
        "A": 25,  # Entity
        "B": 30,  # Attribute
        "C": 15,  # Rule Type
        "D": 80,  # Rule Description
        "E": 20   # Source(s)
    }
    ExcelStyles.set_column_widths(ws, widths)
    
    # Freeze header row
    ws.freeze_panes = "A2"
    
    # Auto-filter
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:E{row_idx - 1}"
