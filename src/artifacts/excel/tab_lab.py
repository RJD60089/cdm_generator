# src/artifacts/excel/tab_lab.py
"""Generate Data_Dictionary_Lab tab for Excel CDM.

Workshop-ready version of the Data Dictionary with additional working
columns for business review and refinement during CDM workshops.
"""

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


def create_lab_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Data_Dictionary_Lab tab.

    Includes all Data Dictionary columns plus workshop columns:
      - Ancillary source references (per source, dynamic)
      - Add, Remove, Change, Reference Data (action column)
      - If Conditional: When is it required?
      - Fixed Values (if applicable - list all)
      - Notes
      - Comment
      - Finalized?
    """

    ws = wb.create_sheet("Data_Dictionary_Lab")

    attributes = extractor.get_all_attributes()

    # Detect which ancillary sources have data
    ancillary_sources = set()
    for a in attributes:
        for key in a.source_lineage:
            if key.startswith("ancillary") and a.source_lineage[key]:
                ancillary_sources.add(key)
    ancillary_sources = sorted(ancillary_sources)

    # Detect field codes
    has_field_codes = any(
        a.ncpdp_field_codes or a.edw_field_codes for a in attributes
    )

    # --- Headers ---
    headers = [
        "Entity",
        "Attribute",
        "Business Definition",
    ]

    # Ancillary source columns
    ancillary_col_names = []
    for anc_src in ancillary_sources:
        display_name = anc_src.replace("ancillary-", "").replace("-", " ").title()
        col_name = f"Ancillary {display_name}"
        headers.append(col_name)
        ancillary_col_names.append(col_name)

    # Workshop columns
    headers += [
        "Add, Remove, Change, Reference Data",
        "Data Type",
        "Size",
        "Nullable\n(if Not Nullable, then required)",
        "If Conditional:\nWhen is it required?",
        "Fixed Values\n(if applicable - list all)",
        "PII",
        "PHI",
        "Notes",
        "Is PK",
        "Is FK",
        "FK Reference",
        "Classification",
        "Rematch",
        "Comment",
        "Finalized?",
    ]

    if has_field_codes:
        # Insert NCPDP and EDW before workshop columns
        fc_insert = headers.index("Add, Remove, Change, Reference Data")
        headers.insert(fc_insert, "EDW")
        headers.insert(fc_insert, "NCPDP Field Code")

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        ExcelStyles.apply_header_style(cell)

    # --- Data rows ---
    for row_idx, attr in enumerate(attributes, 2):
        is_alt = row_idx % 2 == 0

        # Size display
        size = ""
        if attr.max_length:
            size = str(attr.max_length)
        elif attr.precision:
            size = f"{attr.precision},{attr.scale or 0}"

        # Rematch flag
        is_rematch = any(
            mapping.get("rematch") is True
            for mappings in attr.source_lineage.values()
            for mapping in (mappings if isinstance(mappings, list) else [])
        )

        row_data = [
            attr.entity_name,
            attr.attribute_name,
            attr.description or "",
        ]

        # Ancillary source references
        for anc_src in ancillary_sources:
            entries = attr.source_lineage.get(anc_src, [])
            if isinstance(entries, list) and entries:
                refs = []
                for e in entries:
                    src_entity = e.get("source_entity", "")
                    src_attr = e.get("source_attribute", "")
                    if src_entity and src_attr:
                        refs.append(f"{src_entity}.{src_attr}")
                    elif src_attr:
                        refs.append(src_attr)
                ref_str = "; ".join(refs)
            else:
                ref_str = ""
            row_data.append(ref_str)

        # Field codes (if present)
        if has_field_codes:
            row_data.append(
                "; ".join(attr.ncpdp_field_codes) if attr.ncpdp_field_codes else ""
            )
            row_data.append(
                "; ".join(attr.edw_field_codes) if attr.edw_field_codes else ""
            )

        # Workshop + standard columns
        row_data += [
            "",  # Add, Remove, Change, Reference Data
            attr.data_type,
            size,
            "Y" if attr.nullable else "N",
            "",  # If Conditional
            "",  # Fixed Values
            "Y" if attr.is_pii else "",
            "Y" if attr.is_phi else "",
            "",  # Notes
            "Y" if attr.pk else "",
            "Y" if attr.fk_to else "",
            attr.fk_to or "",
            attr.classification or "",
            "R" if is_rematch else "",
            "",  # Comment
            "",  # Finalized?
        ]

        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col, value=value)
            ExcelStyles.apply_body_style(cell, is_alt)

    # --- Column widths ---
    base_widths = [25, 30, 50]  # Entity, Attribute, Business Definition
    anc_widths = [30] * len(ancillary_sources)
    fc_widths = [20, 20] if has_field_codes else []
    workshop_widths = [
        25,  # Add/Remove/Change
        15,  # Data Type
        10,  # Size
        15,  # Nullable
        30,  # If Conditional
        30,  # Fixed Values
        8,   # PII
        8,   # PHI
        30,  # Notes
        8,   # Is PK
        8,   # Is FK
        35,  # FK Reference
        15,  # Classification
        10,  # Rematch
        30,  # Comment
        12,  # Finalized?
    ]

    all_widths = base_widths + anc_widths + fc_widths + workshop_widths
    widths_dict = {}
    for i, w in enumerate(all_widths):
        widths_dict[get_column_letter(i + 1)] = w
    ExcelStyles.set_column_widths(ws, widths_dict)

    # --- Freeze + filter ---
    ws.freeze_panes = "A2"
    last_col = get_column_letter(len(headers))
    ws.auto_filter.ref = f"A1:{last_col}{len(attributes) + 1}"
