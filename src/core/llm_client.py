# src/core/llm_client.py
"""
Enhanced LLM client with retry logic, error handling, and usage tracking.
Supports OpenAI API and compatible endpoints (Azure, local models).
"""
from __future__ import annotations
import os
import time
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from openai import (
    OpenAI,
    APIError,
    APIConnectionError,
    RateLimitError,
    APITimeoutError,
    InternalServerError,
    BadRequestError
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@dataclass
class TokenUsage:
    """Token usage tracking for LLM calls"""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    
    def to_dict(self) -> dict:
        return {
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'total_tokens': self.total_tokens
        }


def _is_openai_cloud(base_url: Optional[str]) -> bool:
    """Check if we're using OpenAI cloud API (supports json_object mode)."""
    return (not base_url) or "api.openai.com" in base_url


class LLMClient:
    """
    Enhanced LLM client with retry logic and robust error handling.
    
    Features:
    - Automatic retry on transient failures
    - Graceful fallback for unsupported parameters
    - Token usage tracking
    - Support for multiple endpoints (OpenAI, Azure, local)
    """
    
    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 64000,
        timeout: int = 1800,
        max_retries: int = 2
    ):
        """
        Initialize LLM client.
        
        Args:
            model: Model name (e.g., "gpt-5", "gpt-4")
            base_url: API base URL (None for OpenAI cloud)
            temperature: Sampling temperature (0-2)
            max_tokens: Maximum tokens in response
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for transient failures
        """
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL") or None
        self.client = OpenAI(
            base_url=self.base_url,
            timeout=timeout
        )
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-5")
        
        # Temperature: use provided, then env, then default
        env_temp = os.getenv("TEMP_DEFAULT", "")
        self.temperature = temperature if temperature is not None else (
            float(env_temp) if env_temp not in ("", None) else 0.2
        )
        
        self.max_tokens = max_tokens or int(os.getenv("MAX_TOKENS", "4096"))
        self.max_retries = max_retries
        
        # Track statistics
        self.total_calls = 0
        self.total_tokens_used = 0
        
        logger.info(
            f"LLM Client initialized: model={self.model}, "
            f"temperature={self.temperature}, base_url={self.base_url or 'OpenAI cloud'}"
        )
    
    @classmethod
    def from_env(cls) -> LLMClient:
        """
        Create LLM client from environment variables.
        
        Reads configuration from:
        - OPENAI_MODEL (default: "gpt-5")
        - OPENAI_BASE_URL (optional)
        - TEMP_DEFAULT (optional, default: 0.2)
        - MAX_TOKENS (optional, default: 4096)
        
        Returns:
            Configured LLMClient instance
        """
        model = os.getenv("OPENAI_MODEL", "gpt-5")
        base_url = os.getenv("OPENAI_BASE_URL")
        
        # Temperature from env
        env_temp = os.getenv("TEMP_DEFAULT", "")
        temperature = float(env_temp) if env_temp not in ("", None) else 0.2
        
        # Max tokens from env
        max_tokens = int(os.getenv("MAX_TOKENS", "4096"))
        
        return cls(
            model=model,
            base_url=base_url,
        )
    
    def call(self, prompt: str) -> str:
        """
        Simple call method for single prompt.
        Wraps chat() for convenience.
        
        Args:
            prompt: User prompt string
            
        Returns:
            Response text
        """
        messages = [{"role": "user", "content": prompt}]
        response, _ = self.chat(messages)
        return response
    
    @retry(
        retry=retry_if_exception_type((APIConnectionError, APITimeoutError, InternalServerError)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    def chat(
        self,
        messages: List[Dict[str, str]],
        response_format: Dict[str, Any] | None = None
    ) -> tuple[str, TokenUsage | None]:
        """
        Send chat completion request to LLM.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            response_format: Response format specification (e.g., {"type": "json_object"})
        
        Returns:
            Tuple of (response_text, token_usage)
            
        Raises:
            BadRequestError: For invalid requests (after fallback attempts)
            APIError: For API errors (after retries exhausted)
        """
        self.total_calls += 1
        start_time = time.time()
        
        # Build request kwargs
        kwargs: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
        }
        
        # JSON mode for OpenAI cloud; many local servers ignore/404 on this
        if _is_openai_cloud(self.base_url):
            kwargs["response_format"] = {"type": "json_object"}
        
        # Override with explicit response_format if provided
        if response_format:
            kwargs["response_format"] = response_format
        
        logger.debug(f"LLM call #{self.total_calls}: {len(messages)} messages, {kwargs.get('max_tokens')} max_tokens")
        
        # Print start message with timestamp
        start_timestamp = time.strftime("%H:%M:%S")
        print(f"  🤖 [{start_timestamp}] Calling LLM: {self.model}...")
        
        try:
            resp = self.client.chat.completions.create(**kwargs)
        except BadRequestError as e:
            # Some endpoints reject temperature or response_format
            # Try progressively simpler requests
            logger.warning(f"BadRequestError on LLM call: {e}. Attempting fallback...")
            print(f"  ⚠️  BadRequestError, attempting fallback...")
            
            msg = str(e).lower()
            
            # Remove response_format if it caused the error
            if "response_format" in msg or "json" in msg:
                logger.info("Removing response_format and retrying...")
                print(f"  🔄 Removing response_format and retrying...")
                kwargs.pop("response_format", None)
            
            # Remove temperature if it caused the error
            if "temperature" in msg:
                logger.info("Removing temperature and retrying...")
                print(f"  🔄 Removing temperature and retrying...")
                kwargs.pop("temperature", None)
            
            # Retry with simplified parameters
            try:
                resp = self.client.chat.completions.create(**kwargs)
                logger.info("Fallback request succeeded")
                print(f"  ✓ Fallback succeeded")
            except BadRequestError as e2:
                logger.error(f"Fallback also failed: {e2}")
                print(f"  ❌ Fallback failed: {e2}")
                raise
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Extract response
        content = resp.choices[0].message.content or ""
        finish_reason = getattr(resp.choices[0], "finish_reason", None)
        
        # Extract usage information
        usage_obj = getattr(resp, "usage", None)
        token_usage = None
        
        if usage_obj:
            prompt_tokens = getattr(usage_obj, "prompt_tokens", 0)
            completion_tokens = getattr(usage_obj, "completion_tokens", 0)
            total_tokens = getattr(usage_obj, "total_tokens", 0)
            
            token_usage = TokenUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens
            )
            
            self.total_tokens_used += total_tokens
            
            logger.debug(
                f"LLM response: {len(content)} chars, "
                f"{total_tokens} tokens, finish={finish_reason}"
            )
            
            # Print timing and token info to terminal
            end_timestamp = time.strftime("%H:%M:%S")
            print(f"  ✅ [{end_timestamp}] Completed in {duration:.1f}s")
            print(f"  📊 Tokens: {total_tokens:,} ({prompt_tokens:,} prompt + {completion_tokens:,} completion)")
        else:
            # No usage info available (some local models)
            end_timestamp = time.strftime("%H:%M:%S")
            print(f"  ✅ [{end_timestamp}] Completed in {duration:.1f}s")
        
        # Log usage to file
        self._log_usage(finish_reason, token_usage)
        
        return content, token_usage
    
    def _log_usage(self, finish_reason: str | None, token_usage: TokenUsage | None):
        """Log usage information to usage log file."""
        try:
            from .logging_utils import append_runlog
            
            record = {
                "model": self.model,
                "finish_reason": str(finish_reason),
                "usage": token_usage.to_dict() if token_usage else None,
            }
            
            append_runlog(os.path.join("output", "usage.log.jsonl"), record)
        except Exception as e:
            logger.warning(f"Failed to log usage: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get client usage statistics.
        
        Returns:
            Dict with total_calls and total_tokens_used
        """
        return {
            "total_calls": self.total_calls,
            "total_tokens_used": self.total_tokens_used,
            "model": self.model,
            "base_url": self.base_url or "OpenAI cloud"
        }