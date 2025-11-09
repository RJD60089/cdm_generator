# src/core/llm_client.py
"""
Enhanced LLM client with retry logic, error handling, and usage tracking.
Supports OpenAI API and compatible endpoints (Azure, local models).
"""
from __future__ import annotations
import os
import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI, BadRequestError, APIError, APIConnectionError, RateLimitError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from .run_state import TokenUsage

logger = logging.getLogger(__name__)


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
        max_tokens: int = 4096,
        timeout: int = 120,
        max_retries: int = 3
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
    
    @retry(
        retry=retry_if_exception_type((APIConnectionError, RateLimitError, APIError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def chat(
        self,
        messages: List[Dict[str, str]],
        response_format: Dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None
    ) -> tuple[str, TokenUsage | None]:
        """
        Send chat completion request to LLM.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            response_format: Response format specification (e.g., {"type": "json_object"})
            temperature: Override default temperature for this call
            max_tokens: Override default max_tokens for this call
        
        Returns:
            Tuple of (response_text, token_usage)
            
        Raises:
            BadRequestError: For invalid requests (after fallback attempts)
            APIError: For API errors (after retries exhausted)
        """
        self.total_calls += 1
        
        # Build request kwargs
        kwargs: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens or self.max_tokens,
        }
        
        # Use provided temperature or instance default
        temp = temperature if temperature is not None else self.temperature
        if temp is not None:
            kwargs["temperature"] = temp
        
        # JSON mode for OpenAI cloud; many local servers ignore/404 on this
        if _is_openai_cloud(self.base_url):
            kwargs["response_format"] = {"type": "json_object"}
        
        # Override with explicit response_format if provided
        if response_format:
            kwargs["response_format"] = response_format
        
        logger.debug(f"LLM call #{self.total_calls}: {len(messages)} messages, {kwargs.get('max_tokens')} max_tokens")
        
        try:
            resp = self.client.chat.completions.create(**kwargs)
        except BadRequestError as e:
            # Some endpoints reject temperature or response_format
            # Try progressively simpler requests
            logger.warning(f"BadRequestError on LLM call: {e}. Attempting fallback...")
            
            msg = str(e).lower()
            
            # Remove response_format if it caused the error
            if "response_format" in msg or "json" in msg:
                logger.info("Removing response_format and retrying...")
                kwargs.pop("response_format", None)
            
            # Remove temperature if it caused the error
            if "temperature" in msg:
                logger.info("Removing temperature and retrying...")
                kwargs.pop("temperature", None)
            
            # Retry with simplified parameters
            try:
                resp = self.client.chat.completions.create(**kwargs)
                logger.info("Fallback request succeeded")
            except BadRequestError as e2:
                logger.error(f"Fallback also failed: {e2}")
                raise
        
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


class MockLLMClient:
    """
    Mock LLM client for testing and dry-run mode.
    
    Returns predefined responses without making actual API calls.
    """
    
    def __init__(self, model: str = "mock-model", temperature: float = 0.0):
        self.model = model
        self.temperature = temperature
        self.total_calls = 0
        self.total_tokens_used = 0
        logger.info("MockLLMClient initialized (no actual API calls will be made)")
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        response_format: Dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None
    ) -> tuple[str, TokenUsage | None]:
        """Return mock response."""
        self.total_calls += 1
        
        # Generate a mock response based on the last message
        last_message = messages[-1]["content"] if messages else ""
        
        # Create a minimal valid JSON response
        mock_response = """{
            "assumptions": ["Mock assumption 1", "Mock assumption 2"],
            "decisions": ["Mock decision 1"],
            "open_questions": ["Mock question 1"],
            "entities": [
                {
                    "name": "MockEntity",
                    "definition": "A mock entity for testing",
                    "is_core": true,
                    "notes": "Generated by MockLLMClient"
                }
            ],
            "core_functional_map": [
                {
                    "component": "Mock Component",
                    "scope": "Testing",
                    "rationale": "For testing purposes"
                }
            ],
            "reference_sets": [],
            "confidence": {
                "tab": "Entities",
                "score": 8
            }
        }"""
        
        # Mock token usage
        token_usage = TokenUsage(
            prompt_tokens=len(last_message) // 4,  # Rough estimate
            completion_tokens=len(mock_response) // 4,
            total_tokens=(len(last_message) + len(mock_response)) // 4
        )
        
        self.total_tokens_used += token_usage.total_tokens
        
        logger.info(f"MockLLMClient returned mock response (call #{self.total_calls})")
        
        return mock_response, token_usage
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get mock statistics."""
        return {
            "total_calls": self.total_calls,
            "total_tokens_used": self.total_tokens_used,
            "model": self.model + " (mock)",
            "base_url": "mock"
        }
