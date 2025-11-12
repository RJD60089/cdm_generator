from __future__ import annotations
import json
from typing import Dict, Any
from pathlib import Path
from ..core.llm_client import LLMClient
from ..core.context_header import build_context_header
from ..core.standard_loader import naming_rules_snippet
from ..core.logging_utils import append_runlog
from ..core.ui import get_console   # ← add this import near the top
from ..core.json_sanitizer import parse_loose_json
import time             

def run_step(state: Dict[str, Any], prompt_template_path: str, llm: LLMClient, runlog_path: str) -> Dict[str, Any]:
    template = Path(prompt_template_path).read_text(encoding="utf-8")
    filled = template.replace("{{CONTEXT_HEADER}}", build_context_header(state)) \
                     .replace("{{NAMING_RULES_SNIPPET}}", naming_rules_snippet(state["naming_rules"]))

    messages = [
        {"role": "system", "content": "You are a precise CDM architect. Always return strict JSON as instructed."},
        {"role": "user", "content": filled},
    ]

    console = get_console()
    t0 = time.time()
    with console.status(f"[bold]Step 1[/bold]: contacting [cyan]{llm.model}[/cyan]…", spinner="dots"):
        raw, usage = llm.chat(messages)
    dt = time.time() - t0
    console.print(f":white_check_mark: Step 1 done in {dt:0.1f}s")

    append_runlog(runlog_path, {"step": 1, "prompt_chars": len(filled), "response_chars": len(raw)})

    try:
        obj = parse_loose_json(raw)
    except Exception:
        # One-shot repair prompt: ask the model to convert its own output to strict JSON
        repair_user = (
            "Convert the following content into STRICT JSON matching the schema from the prior instructions. "
            "Return ONLY JSON, no code fences, no commentary:\n\n" + raw[:8000]
        )
        repair_msgs = [
            {"role": "system", "content": "You are a strict JSON converter. Return only valid minified JSON."},
            {"role": "user", "content": repair_user},
        ]
        raw2, usage2 = llm.chat(repair_msgs, response_format={"type": "json_object"})
        obj = parse_loose_json(raw2)

    w = state["work"]
    w["assumptions"]        = sorted(set(w["assumptions"] + obj.get("assumptions", [])))
    w["decisions"]          = w["decisions"] + obj.get("decisions", [])
    w["open_questions"]     = w["open_questions"] + obj.get("open_questions", [])
    w["entities"]           = obj.get("entities", [])
    w["core_functional_map"]= obj.get("core_functional_map", [])
    w["reference_sets"]     = obj.get("reference_sets", [])
    if "confidence" in obj and obj["confidence"].get("tab") == "Entities":
        w["confidence"]["per_tab"]["Entities"] = obj["confidence"].get("score")
    return state
