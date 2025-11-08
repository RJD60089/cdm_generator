from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict
import json, time, hashlib

@dataclass
class RunMeta:
    domain: str
    run_id: str
    timestamp: str
    model: str
    temperature: float

@dataclass
class RunState:
    meta: RunMeta
    naming_rules: Dict[str, Any] = field(default_factory=dict)
    work: Dict[str, Any] = field(default_factory=lambda: {
        "assumptions": [], "decisions": [], "open_questions": [],
        "entities": [], "relationships": [], "attributes": [],
        "keys": [], "reference_sets": [], "quality_rules": [],
        "lineage_notes": [], "stewardship": [], "core_functional_map": [],
        "confidence": {"per_tab": {}, "overall": None}
    })

    def to_dict(self) -> Dict[str, Any]:
        return {"meta": asdict(self.meta), "naming_rules": self.naming_rules, "work": self.work}

    @staticmethod
    def new(domain: str, model: str, temperature: float) -> "RunState":
        ts = time.strftime("%Y-%m-%d_%H%M%S")
        rid = hashlib.sha1(f"{domain}|{ts}|{model}|{temperature}".encode()).hexdigest()[:10]
        meta = RunMeta(domain=domain, run_id=rid, timestamp=ts, model=model, temperature=temperature)
        return RunState(meta=meta)
