# src/core/llm_client.py
from __future__ import annotations
import os
from typing import List, Dict, Any, Optional
from openai import OpenAI, BadRequestError

def _is_openai_cloud(base_url: Optional[str]) -> bool:
    # If no base_url, or explicitly api.openai.com, we can use response_format=json_object
    return (not base_url) or "api.openai.com" in base_url

class LLMClient:
    def __init__(self, model: str | None = None, base_url: str | None = None, temperature: float = 0.2):
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL") or None
        self.client = OpenAI(base_url=self.base_url)
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-5")
        t = os.getenv("TEMP_DEFAULT", "")
        self.temperature = float(t) if t not in ("", None) else None
        self.max_tokens = int(os.getenv("MAX_TOKENS", "4096"))

    def chat(self, messages: List[Dict[str, str]], response_format: Dict[str, Any] | None = None) -> str:
        kwargs: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens,
        }
        if self.temperature is not None:
            kwargs["temperature"] = self.temperature

        # JSON mode for OpenAI cloud; many local servers ignore/404 on this, so only set when safe.
        if _is_openai_cloud(self.base_url):
            kwargs["response_format"] = {"type": "json_object"}

        if response_format:
            kwargs["response_format"] = response_format

        try:
            resp = self.client.chat.completions.create(**kwargs)
        except BadRequestError as e:
            # Some endpoints reject temperature or response_format; retry progressively simpler
            msg = str(e).lower()
            kwargs.pop("response_format", None)
            if "temperature" in msg:
                kwargs.pop("temperature", None)
            resp = self.client.chat.completions.create(**kwargs)

        finish = getattr(resp.choices[0], "finish_reason", None)
        usage  = getattr(resp, "usage", None)  # has prompt_tokens/completion_tokens/total_tokens on many backends
        # log if available (no crash if missing)
        from .logging_utils import append_runlog
        append_runlog(os.path.join("output","usage.log.jsonl"), {
            "model": self.model,
            "finish_reason": str(finish),
            "usage": getattr(usage, "__dict__", dict(usage)) if usage else None,
        })

        return resp.choices[0].message.content or ""
