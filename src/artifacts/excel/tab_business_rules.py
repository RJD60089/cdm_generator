# src/artifacts/excel/tab_business_rules.py
"""Generate Business Rules tab for Excel CDM."""

import re
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font
from src.artifacts.common.cdm_extractor import CDMExtractor
from src.artifacts.common.styles import ExcelStyles


CONFLICT_FILL = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
CONFLICT_FONT = Font(bold=True, color="9C0006", size=10)


def _format_sources(sources):
    """Render a sources list as a short readable string."""
    if not sources:
        return ""
    return ", ".join(sorted({s for s in sources if s}))


def _normalize_rule_text(text: str) -> str:
    """Lowercase + collapse whitespace for near-duplicate comparison."""
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _detect_conflicts(rules_full):
    """
    Detect conflicting rules for a single attribute.

    Returns a dict: rule_text -> conflict_id (short marker) when the rule
    contradicts another rule on the same attribute (e.g., nullable vs
    non-nullable, or different sizes).
    """
    nullable_votes = []        # (rule_text, value_bool)
    size_votes = []            # (rule_text, int_size)
    required_votes = []        # (rule_text, bool)

    for r in rules_full:
        text = r.get("rule", "") or ""
        low = text.lower()

        # Nullability signals
        if "not null" in low or "non-null" in low or "non null" in low:
            nullable_votes.append((text, False))
        elif "nullable" in low or "may be null" in low or "can be null" in low:
            nullable_votes.append((text, True))

        # Required/optional signals
        if re.search(r"\brequired\b", low):
            required_votes.append((text, True))
        if re.search(r"\boptional\b", low):
            required_votes.append((text, False))

        # Size signals — e.g., "max length 10", "size 5", "varchar(10)"
        m = (
            re.search(r"(?:max(?:imum)?\s+length|size|length)\s*[:=]?\s*(\d+)", low)
            or re.search(r"\bvarchar\s*\((\d+)\)", low)
            or re.search(r"\bchar\s*\((\d+)\)", low)
        )
        if m:
            try:
                size_votes.append((text, int(m.group(1))))
            except ValueError:
                pass

    conflicts: dict = {}

    def _mark(group_votes, conflict_label):
        values = {v for _, v in group_votes}
        if len(values) > 1:
            for text, _ in group_votes:
                conflicts.setdefault(text, []).append(conflict_label)

    _mark(nullable_votes, "NULL")
    _mark(required_votes, "REQ")
    _mark(size_votes, "SIZE")

    return {t: "/".join(v) for t, v in conflicts.items()}


def create_business_rules_tab(wb: Workbook, extractor: CDMExtractor) -> None:
    """
    Create the Business Rules tab.

    Columns:
      Entity, Attribute, Rule Type, Rule Description, Source(s), Conflict ID
    """

    ws = wb.create_sheet("Business_Rules")

    headers = [
        "Entity", "Attribute", "Rule Type",
        "Rule Description", "Source(s)", "Conflict ID",
    ]
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        ExcelStyles.apply_header_style(cell)

    row_idx = 2

    for attr in extractor.get_all_attributes():
        # Combine business + validation rules to detect conflicts across both
        combined = (
            [("Business", r) for r in attr.business_rules_full]
            + [("Validation", r) for r in attr.validation_rules_full]
        )
        if not combined:
            continue

        conflicts = _detect_conflicts([r for _, r in combined])

        for rule_type, rule in combined:
            is_alt = row_idx % 2 == 0
            rule_text = rule.get("rule", "")
            conflict_id = conflicts.get(rule_text, "")

            row_data = [
                attr.entity_name,
                attr.attribute_name,
                rule_type,
                rule_text,
                _format_sources(rule.get("sources", [])),
                conflict_id,
            ]
            for col, value in enumerate(row_data, 1):
                cell = ws.cell(row=row_idx, column=col, value=value)
                ExcelStyles.apply_body_style(cell, is_alt)
                if conflict_id and col == 6:
                    cell.fill = CONFLICT_FILL
                    cell.font = CONFLICT_FONT
            row_idx += 1

    widths = {
        "A": 25,   # Entity
        "B": 30,   # Attribute
        "C": 15,   # Rule Type
        "D": 80,   # Rule Description
        "E": 25,   # Source(s)
        "F": 12,   # Conflict ID
    }
    ExcelStyles.set_column_widths(ws, widths)

    ws.freeze_panes = "A2"
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:F{row_idx - 1}"
