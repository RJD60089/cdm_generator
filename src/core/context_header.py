from __future__ import annotations
from typing import Dict, Any
import json

def build_context_header(state: Dict[str, Any]) -> str:
    w = state["work"]
    header = {
        "domain": state["meta"]["domain"],
        "run_id": state["meta"]["run_id"],
        "timestamp": state["meta"]["timestamp"],
        "assumptions": w.get("assumptions", []),
        "decisions": w.get("decisions", []),
        "open_questions": w.get("open_questions", []),
    }
    return json.dumps(header, ensure_ascii=False, indent=2)
