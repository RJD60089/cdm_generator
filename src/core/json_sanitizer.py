# src/core/json_sanitizer.py
from __future__ import annotations
import json
import re

FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.IGNORECASE | re.DOTALL)

def strip_code_fences(text: str) -> str:
    m = FENCE_RE.search(text)
    return m.group(1) if m else text

def extract_first_json_object(text: str) -> str:
    # Greedy brace matcher to pull the first complete JSON object
    s = strip_code_fences(text).lstrip()
    if not s:
        raise ValueError("empty response")
    start = s.find("{")
    if start == -1:
        # maybe it's an array
        start = s.find("[")
        if start == -1:
            raise ValueError("no JSON delimiters found")
        open_ch, close_ch = "[", "]"
    else:
        open_ch, close_ch = "{", "}"
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(s[start:], start=start):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return s[start:i+1]
    raise ValueError("unterminated JSON")

def parse_loose_json(text: str):
    candidate = extract_first_json_object(text)
    return json.loads(candidate)
