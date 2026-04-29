# src/config/config_gen_guardrails.py
"""
Guardrails Tab Triage — Step 0 module.

For each guardrail file configured in input_files.guardrails, ask the LLM
to look at every tab (sheet) and decide whether it should be INCLUDED in
CDM rationalization or EXCLUDED.

Why this exists:
  Guardrail files are workbooks that mix real data-element content with
  glossaries, examples, summaries, assumptions, and — critically —
  anti-lists like "not going to the cloud" that name items the team has
  already excluded from the CDM.  Sending those tabs to the rationalizer
  burns tokens and risks the LLM mistakenly importing excluded items.

Output:
  Each guardrail entry in config['input_files']['guardrails'] is upgraded
  from a plain string to an object:
      {
        "file": "<filename>",
        "include_sheets": ["..."],
        "exclude_sheets": ["..."],
        "triage_reasons": {"<sheet>": "<reason>", ...}
      }

  The downstream rationalizer reads include_sheets / exclude_sheets and
  filters before sending content to the LLM.

User overrides:
  After Step 0 saves the config, the user can hand-edit include_sheets /
  exclude_sheets to correct any AI mis-classifications. Re-running the
  triage step overwrites the AI-generated lists but preserves the rest
  of the config.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from . import config_utils
from .config_gen_core import ConfigGeneratorBase, prompt_user_choice


# How many data rows per tab to include in the triage prompt.  Small
# enough to keep the prompt cheap, big enough to expose data shape.
SAMPLE_ROWS_PER_TAB = 4

SYSTEM_PROMPT = (
    "You are a senior business analyst classifying spreadsheet tabs for "
    "a Common Data Model rationalization pipeline. Return ONLY valid JSON "
    "— no markdown, no commentary."
)


PROMPT_TEMPLATE = """Decide which tabs in this guardrail file should be included
in CDM rationalization.

CDM Domain: {domain}
CDM Description: {description}

For each tab below, return decision = "include" if the tab contains
business entity / attribute definitions that should be rationalized into
the CDM. Return decision = "exclude" if the tab is any of:

  - Glossary, abbreviations, terminology lookup
  - High-level summary, README, change log
  - Example / template / sample data (anything with "example" or
    "template" in the name)
  - Assumptions, scope notes, design rationale
  - FHIR reference / value-set / code-system tabs (anything with "FHIR"
    in the name). FHIR content is rationalized separately from FHIR IGs;
    including FHIR tabs from a guardrails file would re-rationalize the
    same concepts twice and inflate the prompt.
  - Anti-list — tabs that LIST items the team has decided NOT to model
    in the CDM. Common names: "not going to the cloud", "out of scope",
    "deprecated", "rejected", "excluded". Anti-lists must ALWAYS be
    excluded — items they name should NOT enter the CDM.
  - Pivot / chart / supporting analysis tabs

When in doubt, INCLUDE the tab — over-inclusion costs tokens, but
under-inclusion silently drops business data. Exceptions that are never
in doubt: anti-list tabs and FHIR-titled tabs — always exclude.

File: {filename}

Tabs (with first {n_rows} data rows of each):
{tab_summaries}

Return JSON in EXACTLY this shape:
{{
  "decisions": [
    {{"sheet": "<exact tab name>", "decision": "include"|"exclude",
      "reason": "<one short sentence>"}}
  ]
}}
"""


class GuardrailsConfigGenerator(ConfigGeneratorBase):
    """Triage tabs in each guardrail file via the LLM."""

    def _summarize_tabs(self, file_path: Path) -> List[Dict[str, Any]]:
        """Read the workbook, sample the first few rows of each tab."""
        try:
            xl = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"      ⚠️  Could not open {file_path.name}: {e}")
            return []

        out: List[Dict[str, Any]] = []
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=SAMPLE_ROWS_PER_TAB)
                # Detect empty tabs up front — no rows AND no columns
                # means truly empty; rows == 0 with header columns means
                # a "header-only" tab with no data.  Both are equally
                # useless for rationalization and get auto-excluded
                # without an LLM call.
                if len(df) == 0:
                    out.append({
                        "sheet": sheet_name,
                        "columns": [str(c) for c in df.columns],
                        "sample_rows": [],
                        "is_empty": True,
                    })
                    continue
                cols = [str(c) for c in df.columns]
                rows = []
                for _, row in df.iterrows():
                    rows.append({c: ("" if pd.isna(v) else str(v)[:120]) for c, v in zip(cols, row)})
                out.append({
                    "sheet": sheet_name,
                    "columns": cols,
                    "sample_rows": rows,
                })
            except Exception as e:
                # Some tabs (rich formatting, embedded objects) can't be sampled — keep the
                # name so the LLM can still rule on it from name alone.
                out.append({
                    "sheet": sheet_name,
                    "columns": [],
                    "sample_rows": [],
                    "read_error": str(e),
                })
        return out

    def _build_prompt(self, filename: str, tab_data: List[Dict[str, Any]],
                      domain: str, description: str) -> str:
        tab_summaries: List[str] = []
        for t in tab_data:
            block = [f"  - {t['sheet']}"]
            if t.get("columns"):
                block.append(f"      columns: {t['columns']}")
            if t.get("sample_rows"):
                block.append(f"      sample_rows: {json.dumps(t['sample_rows'])[:600]}")
            if t.get("read_error"):
                block.append(f"      (could not read sample: {t['read_error']})")
            tab_summaries.append("\n".join(block))

        return PROMPT_TEMPLATE.format(
            domain=domain or "(unknown)",
            description=description or "(none)",
            filename=filename,
            n_rows=SAMPLE_ROWS_PER_TAB,
            tab_summaries="\n\n".join(tab_summaries),
        )

    def _call_triage_llm(self, prompt: str) -> Optional[Dict[str, Any]]:
        if self.llm_client is None:
            raise ValueError("Guardrails triage requires an LLM client")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]
        response, _ = self.llm_client.chat(messages)
        try:
            return self.parse_ai_json_response(response)
        except json.JSONDecodeError as e:
            print(f"      ⚠️  Triage response was not valid JSON: {e}")
            return None

    def run_analysis(self, source_config: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        """
        Triage every guardrails entry. Returns an updates dict shaped as
        ``{"guardrails": [...new entries...]}``. The caller merges this
        into ``config['input_files']``.
        """
        guardrails_entries = (source_config.get("input_files") or {}).get("guardrails") or []
        if not guardrails_entries:
            print("\n   ℹ️  No guardrails files configured — nothing to triage.")
            return {}

        domain = (source_config.get("cdm") or {}).get("domain", "")
        description = (source_config.get("cdm") or {}).get("description", "")

        updated_entries: List[Any] = []
        for entry in guardrails_entries:
            # Normalise: accept plain string or dict form
            if isinstance(entry, str):
                filename = entry
                preserved: Dict[str, Any] = {"file": filename}
            elif isinstance(entry, dict):
                filename = entry.get("file") or ""
                preserved = dict(entry)
            else:
                print(f"      ⚠️  Skipping unexpected entry: {entry!r}")
                updated_entries.append(entry)
                continue

            if not filename:
                updated_entries.append(entry)
                continue

            file_path = config_utils.resolve_guardrail_file(self.cdm_name, filename)
            if not file_path.exists():
                print(f"      ⚠️  File not found, skipping: {file_path}")
                updated_entries.append(entry)
                continue

            print(f"\n   Triaging: {filename}")
            tab_data = self._summarize_tabs(file_path)
            print(f"      Tabs found: {len(tab_data)}")
            for t in tab_data:
                empty_marker = "  (EMPTY)" if t.get("is_empty") else ""
                print(f"        - {t['sheet']}{empty_marker}")

            # Pre-triage: empty tabs always exclude — no LLM call needed.
            # Empty by definition means no rationalizable content.
            empty_tabs = [t["sheet"] for t in tab_data if t.get("is_empty")]
            non_empty_tabs = [t for t in tab_data if not t.get("is_empty")]

            if dry_run:
                # Save the prompt; return entry unchanged
                prompt = self._build_prompt(filename, non_empty_tabs, domain, description)
                if self.config_dir:
                    prompts_dir = self.config_dir / "prompts"
                    prompts_dir.mkdir(parents=True, exist_ok=True)
                    safe = filename.replace(" ", "_").replace("/", "_")
                    (prompts_dir / f"guardrails_triage_{safe}.txt").write_text(prompt, encoding="utf-8")
                    print(f"      [dry run] prompt saved (empty tabs auto-excluded: {len(empty_tabs)})")
                updated_entries.append(entry)
                continue

            include: List[str] = []
            exclude: List[str] = list(empty_tabs)  # auto-exclude all empty tabs
            reasons: Dict[str, str] = {s: "Empty tab — no data rows" for s in empty_tabs}

            if non_empty_tabs:
                prompt = self._build_prompt(filename, non_empty_tabs, domain, description)
                decisions = self._call_triage_llm(prompt) or {}
                decision_list = decisions.get("decisions") or []

                seen_sheets = set(empty_tabs)
                for d in decision_list:
                    sheet = (d.get("sheet") or "").strip()
                    if not sheet:
                        continue
                    seen_sheets.add(sheet)
                    verdict = (d.get("decision") or "").strip().lower()
                    reasons[sheet] = (d.get("reason") or "").strip()
                    if verdict == "include":
                        include.append(sheet)
                    elif verdict == "exclude":
                        exclude.append(sheet)

                # Any non-empty tab we sampled but the LLM didn't rule on —
                # default to INCLUDE to fail safe (over-inclusion is recoverable;
                # silent drops aren't).
                for t in non_empty_tabs:
                    if t["sheet"] not in seen_sheets:
                        include.append(t["sheet"])
                        reasons[t["sheet"]] = "(no triage decision — defaulted to include)"

            print(f"      AI verdict: include={len(include)}, exclude={len(exclude)}")
            for s in include:
                print(f"         + {s}")
            for s in exclude:
                print(f"         - {s}  ({reasons.get(s, '')[:80]})")

            new_entry = dict(preserved)
            new_entry["file"] = filename
            new_entry["include_sheets"] = include
            new_entry["exclude_sheets"] = exclude
            new_entry["triage_reasons"] = reasons
            updated_entries.append(new_entry)

        return {"guardrails": updated_entries}
