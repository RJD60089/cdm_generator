# src/artifacts/excel/tab_data_dictionary.py
"""Generate Data Dictionary tab for Excel CDM."""

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


def create_data_dictionary_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Data Dictionary tab with all attributes.
    
    Columns:
    - Entity, Attribute, Business Definition, Data Type, Size
    - Nullable, Is PK, Is FK, FK Reference, CDE Flag
    - Classification, PII, PHI, Authoritative Source
    """
    
    ws = wb.create_sheet("Data_Dictionary")
    
    # Headers
    headers = [
        "Entity", "Attribute", "Business Definition", "Data Type", "Size",
        "Nullable", "Is PK", "Is FK", "FK Reference", 
        "Classification", "PII", "PHI"
    ]
    
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        ExcelStyles.apply_header_style(cell)
    
    # Data rows
    attributes = extractor.get_all_attributes()
    
    for row_idx, attr in enumerate(attributes, 2):
        is_alt = row_idx % 2 == 0
        
        # Size display
        size = ""
        if attr.max_length:
            size = str(attr.max_length)
        elif attr.precision:
            size = f"{attr.precision},{attr.scale or 0}"
        
        row_data = [
            attr.entity_name,
            attr.attribute_name,
            attr.description or "",
            attr.data_type,
            size,
            "Y" if attr.nullable else "N",
            "Y" if attr.pk else "",
            "Y" if attr.fk_to else "",
            attr.fk_to or "",
            attr.classification or "",
            "Y" if attr.is_pii else "",
            "Y" if attr.is_phi else ""
        ]
        
        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col, value=value)
            ExcelStyles.apply_body_style(cell, is_alt)
            
            # Highlight PKs and FKs
            if col == 2:  # Attribute column
                if attr.pk:
                    ExcelStyles.apply_pk_style(cell)
                elif attr.fk_to:
                    ExcelStyles.apply_fk_style(cell)
    
    # Column widths
    widths = {
        "A": 25,  # Entity
        "B": 30,  # Attribute
        "C": 50,  # Description
        "D": 15,  # Data Type
        "E": 10,  # Size
        "F": 10,  # Nullable
        "G": 8,   # Is PK
        "H": 8,   # Is FK
        "I": 35,  # FK Reference
        "J": 15,  # Classification
        "K": 8,   # PII
        "L": 8    # PHI
    }
    ExcelStyles.set_column_widths(ws, widths)
    
    # Freeze header row
    ws.freeze_panes = "A2"
    
    # Auto-filter
    ws.auto_filter.ref = f"A1:L{len(attributes) + 1}"
