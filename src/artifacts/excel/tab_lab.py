# src/artifacts/excel/tab_lab.py
"""
Generate Lab tabs for CDM Workshop use.

Two worksheets:
  - Data_Dictionary_Lab: Data Dictionary + workshop columns inserted
    immediately after "Business Definition". GREEN columns are Lab
    Decision / Note / Lab 3 Notes; ORANGE columns are attribute-level
    Lab 4 working fields.
  - Entities_Lab: Entity Name + Decision/Note/Lab 3 Notes (GREEN)
    and What-Is-This / Where-From / Forms / Where-Ends / Lab 4 Notes
    (ORANGE).
"""

from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


# --- Lab header colors ---
GREEN_FILL = PatternFill(start_color="548235", end_color="548235", fill_type="solid")
ORANGE_FILL = PatternFill(start_color="ED7D31", end_color="ED7D31", fill_type="solid")
LAB_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
LAB_HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)


# Green workshop columns - shared across both Lab tabs
GREEN_COLUMNS = [
    (
        "Decision",
        "CONFIRM / RENAME / ADD / REMOVE / MOVE / SPLIT-MERGE",
    ),
    (
        "Note",
        "(only for ADD, RENAME, MOVE, SPLIT/MERGE)",
    ),
    ("Lab 3 Notes", ""),
]


# Orange workshop columns for the Entities Lab tab
ENTITY_ORANGE_COLUMNS = [
    (
        "What Is This?",
        "A plain-language description of this entity, written so any business "
        "person could read it and understand it. One to three sentences. No "
        "technical language, no jargon.",
    ),
    (
        "Where Does It Come From?",
        "What business process creates this entity or causes it to exist? "
        "When does it show up in the business? What triggers its creation?",
    ),
    (
        "What Forms Does It Take?",
        "Are there different types or variations of this entity that behave "
        "differently or mean something different? For example: retail claims, "
        "mail order claims, compound claims, specialty claims.",
    ),
    (
        "Where Does It End?",
        "Where does this entity's responsibility stop and another CDM's "
        "begin? What does this entity NOT include? What belongs elsewhere?",
    ),
    ("Lab 4 Notes", ""),
]


# Orange workshop columns for the Data Dictionary Lab tab
ATTR_ORANGE_COLUMNS = [
    ("Add/Remove/Change/Reference Data", ""),
    ("If Conditional", ""),
    ("Fixed Values", ""),
    ("Comment", ""),
    ("Finalized", ""),
]


def _apply_lab_header(cell, fill: PatternFill) -> None:
    cell.fill = fill
    cell.font = LAB_HEADER_FONT
    cell.alignment = LAB_HEADER_ALIGN
    cell.border = ExcelStyles.THIN_BORDER


def _compose_header(label: str, note: str) -> str:
    """Combine the primary label and optional sub-note into a single header cell."""
    return f"{label}\n{note}" if note else label


# =============================================================================
# DATA DICTIONARY LAB
# =============================================================================

def create_data_dictionary_lab_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Data Dictionary Lab tab.

    Column order:
      1. Entity
      2. Attribute
      3. Business Definition
      4-6. GREEN — Decision, Note, Lab 3 Notes
      7-11. ORANGE — Add/Remove/Change/Reference Data, If Conditional,
            Fixed Values, Comment, Finalized
      remaining — rest of Data Dictionary columns (Data Type, Size, etc.)
    """

    ws = wb.create_sheet("Data_Dictionary_Lab")

    attributes = extractor.get_all_attributes()

    # Headers: lead with Entity / Attribute / Business Definition, then
    # Lab columns, then the rest of the DD columns.
    base_headers_after_def = [
        "Data Type", "Size", "Nullable", "Is PK", "Is FK", "FK Reference",
        "Classification", "PII", "PHI", "Rematch",
    ]

    # Build header row
    header_specs = (
        [("Entity", None), ("Attribute", None), ("Business Definition", None)]
        + [(_compose_header(l, n), "green") for l, n in GREEN_COLUMNS]
        + [(_compose_header(l, n), "orange") for l, n in ATTR_ORANGE_COLUMNS]
        + [(h, None) for h in base_headers_after_def]
    )

    for col_idx, (header, color) in enumerate(header_specs, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        if color == "green":
            _apply_lab_header(cell, GREEN_FILL)
        elif color == "orange":
            _apply_lab_header(cell, ORANGE_FILL)
        else:
            ExcelStyles.apply_header_style(cell)

    num_green = len(GREEN_COLUMNS)
    num_orange = len(ATTR_ORANGE_COLUMNS)
    blank_lab_cols = num_green + num_orange

    # Data rows
    for row_idx, attr in enumerate(attributes, 2):
        is_alt = row_idx % 2 == 0

        size = ""
        if attr.max_length:
            size = str(attr.max_length)
        elif attr.precision:
            size = f"{attr.precision},{attr.scale or 0}"

        is_rematch = any(
            mapping.get("rematch") is True
            for mappings in attr.source_lineage.values()
            for mapping in (mappings if isinstance(mappings, list) else [])
        )

        row_data = (
            [
                attr.entity_name,
                attr.attribute_name,
                attr.description or "",
            ]
            + [""] * blank_lab_cols
            + [
                attr.data_type,
                size,
                "Y" if attr.nullable else "N",
                "Y" if attr.pk else "",
                "Y" if attr.fk_to else "",
                attr.fk_to or "",
                attr.classification or "",
                "Y" if attr.is_pii else "",
                "Y" if attr.is_phi else "",
                "R" if is_rematch else "",
            ]
        )

        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            ExcelStyles.apply_body_style(cell, is_alt)
            if col_idx == 2:
                if attr.pk:
                    ExcelStyles.apply_pk_style(cell)
                elif attr.fk_to:
                    ExcelStyles.apply_fk_style(cell)

    # Column widths
    widths = {
        "A": 25,   # Entity
        "B": 30,   # Attribute
        "C": 50,   # Business Definition
    }
    # Green cols
    for i in range(num_green):
        widths[get_column_letter(4 + i)] = 22
    # Orange cols
    for i in range(num_orange):
        widths[get_column_letter(4 + num_green + i)] = 22
    # Remaining DD cols
    tail_widths = [15, 10, 10, 8, 8, 35, 15, 8, 8, 10]
    base_start = 4 + num_green + num_orange
    for i, w in enumerate(tail_widths):
        widths[get_column_letter(base_start + i)] = w
    ExcelStyles.set_column_widths(ws, widths)

    # Taller header for wrapped notes
    ws.row_dimensions[1].height = 60

    ws.freeze_panes = "D2"
    last_col = get_column_letter(len(header_specs))
    ws.auto_filter.ref = f"A1:{last_col}{len(attributes) + 1}"


# =============================================================================
# ENTITIES LAB
# =============================================================================

def create_entities_lab_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Entities Lab tab.

    Column order:
      1. Entity Name
      2-4. GREEN — Decision, Note, Lab 3 Notes
      5-9. ORANGE — What Is This?, Where Does It Come From?,
           What Forms Does It Take?, Where Does It End?, Lab 4 Notes
    """

    ws = wb.create_sheet("Entities_Lab")

    entities = extractor.get_entities()

    header_specs = (
        [("Entity Name", None)]
        + [(_compose_header(l, n), "green") for l, n in GREEN_COLUMNS]
        + [(_compose_header(l, n), "orange") for l, n in ENTITY_ORANGE_COLUMNS]
    )

    for col_idx, (header, color) in enumerate(header_specs, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        if color == "green":
            _apply_lab_header(cell, GREEN_FILL)
        elif color == "orange":
            _apply_lab_header(cell, ORANGE_FILL)
        else:
            ExcelStyles.apply_header_style(cell)

    num_lab_cols = len(GREEN_COLUMNS) + len(ENTITY_ORANGE_COLUMNS)

    for row_idx, entity in enumerate(entities, 2):
        is_alt = row_idx % 2 == 0
        row_data = [entity.name] + [""] * num_lab_cols
        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            ExcelStyles.apply_body_style(cell, is_alt)

    widths = {"A": 30}
    for i in range(len(GREEN_COLUMNS)):
        widths[get_column_letter(2 + i)] = 22
    for i in range(len(ENTITY_ORANGE_COLUMNS)):
        widths[get_column_letter(2 + len(GREEN_COLUMNS) + i)] = 35
    ExcelStyles.set_column_widths(ws, widths)

    ws.row_dimensions[1].height = 90

    ws.freeze_panes = "B2"
    last_col = get_column_letter(len(header_specs))
    ws.auto_filter.ref = f"A1:{last_col}{len(entities) + 1}"
