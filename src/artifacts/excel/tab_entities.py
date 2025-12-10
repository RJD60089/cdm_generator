# src/artifacts/excel/tab_entities.py
"""Generate Entities tab for Excel CDM."""

from openpyxl import Workbook
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


def create_entities_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Entities tab with entity overview.
    
    Columns:
    - Entity Name, Description, Classification
    - Primary Key(s), Attribute Count
    - Source Coverage (Guardrails, Glue, NCPDP, FHIR)
    """
    
    ws = wb.create_sheet("Entities")
    
    # Headers
    headers = [
        "Entity Name", "Description", "Classification",
        "Primary Key(s)", "Attribute Count",
        "Guardrails", "Glue", "NCPDP", "FHIR"
    ]
    
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        ExcelStyles.apply_header_style(cell)
    
    # Data rows
    entities = extractor.get_entities()
    
    for row_idx, entity in enumerate(entities, 2):
        is_alt = row_idx % 2 == 0
        
        # Format PKs as comma-separated
        pk_str = ", ".join(entity.primary_keys) if entity.primary_keys else ""
        
        row_data = [
            entity.name,
            entity.description,
            entity.classification,
            pk_str,
            entity.attribute_count,
            "✓" if entity.source_coverage.get("guardrails") else "",
            "✓" if entity.source_coverage.get("glue") else "",
            "✓" if entity.source_coverage.get("ncpdp") else "",
            "✓" if entity.source_coverage.get("fhir") else ""
        ]
        
        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col, value=value)
            ExcelStyles.apply_body_style(cell, is_alt)
            
            # Center the checkmarks
            if col >= 6:
                cell.alignment = ExcelStyles.CENTER_ALIGN
    
    # Column widths
    widths = {
        "A": 25,  # Entity Name
        "B": 60,  # Description
        "C": 15,  # Classification
        "D": 30,  # Primary Key(s)
        "E": 15,  # Attribute Count
        "F": 12,  # Guardrails
        "G": 10,  # Glue
        "H": 10,  # NCPDP
        "I": 10   # FHIR
    }
    ExcelStyles.set_column_widths(ws, widths)
    
    # Freeze header row
    ws.freeze_panes = "A2"
    
    # Auto-filter
    ws.auto_filter.ref = f"A1:I{len(entities) + 1}"
