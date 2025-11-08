from __future__ import annotations
import os, re, docx2txt
from typing import Dict, Any

def _extract_text(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    if path.lower().endswith(".docx"):
        return docx2txt.process(path) or ""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def load_naming_rules(standard_path: str | None) -> Dict[str, Any]:
    txt = _extract_text(standard_path)

    def has(pat: str) -> bool:
        return re.search(pat, txt, flags=re.I) is not None

    rules = {
        "entities_casing": "PascalCase" if has(r"entities?.*pascal") or not txt else "PascalCase",
        "columns_casing": "snake_case"  if has(r"(fields?|columns?).*snake") or not txt else "snake_case",
        "table_form": "singular"        if has(r"tables?.*singular") or not txt else "singular",
        "pk_pattern": "{table}_id",
        "fk_pattern": "{ref}_id",
        "boolean_prefix": "is_",
        "suffix_hints": {
            "_code": "string", "_name": "string", "_description": "string",
            "_date": "date", "_datetime": "datetime", "_amount": "decimal"
        }
    }
    return rules

def naming_rules_snippet(rules: Dict[str, Any]) -> str:
    return (
        f"- Entities casing: {rules['entities_casing']}\n"
        f"- Columns casing: {rules['columns_casing']}\n"
        f"- Tables: {rules['table_form']}\n"
        f"- PK pattern: {rules['pk_pattern']}  (e.g., pharmacy_id)\n"
        f"- FK pattern: {rules['fk_pattern']}  (e.g., member_id)\n"
        f"- Boolean prefix: {rules['boolean_prefix']}\n"
        f"- Suffix hints: {', '.join(sorted(rules['suffix_hints'].keys()))}\n"
        f"- NOTE: Generate names to standard; do not “fix” post-hoc."
    )
