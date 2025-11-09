from __future__ import annotations
import os, json, time

def append_runlog(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    record["ts"] = time.time()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
