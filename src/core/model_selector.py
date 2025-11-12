"""
Model Selection Module
Shared model configuration and selection for all CDM generation apps.
Supports GPT-5, GPT-4.1, and local models (70B, 33B, 8B).
"""
import os
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()

# Model configuration from .env
MODEL_OPTIONS = {
    "gpt-5": {
        "name": "gpt-5 (OpenAI - best reasoning)",
        "provider": "openai",
        "model": os.getenv("OPENAI_MODEL_5", "gpt-5"),
        "api_key": lambda: os.getenv("OPENAI_API_KEY"),
        "base_url": lambda: os.getenv("OPENAI_BASE_URL")
    },
    "gpt-4.1": {
        "name": "gpt-4.1 (OpenAI - large context)",
        "provider": "openai",
        "model": os.getenv("OPENAI_MODEL_4.1", "gpt-4.1"),
        "api_key": lambda: os.getenv("OPENAI_API_KEY"),
        "base_url": lambda: os.getenv("OPENAI_BASE_URL")
    },
    "local-70b": {
        "name": "local-70b (llama.cpp - Llama 3.3 70B)",
        "provider": "llamacpp",
        "model": os.getenv("LLAMACPP_MODEL_70B"),
        "api_key": lambda: os.getenv("LLAMACPP_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("LLAMACPP_BASE_URL")
    },
    "local-32b": {
        "name": "local-32b (unsloth/Qwen3-32B-GGUF Qwen3-32B-Q4_K_M.gguf)",
        "provider": "llamacpp",
        "model": os.getenv("LLAMACPP_MODEL_32B"),
        "api_key": lambda: os.getenv("LLAMACPP_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("LLAMACPP_BASE_URL")
    },
    "local-8b": {
        "name": "local-8b (vLLM - Llama 3.1 8B)",
        "provider": "vllm",
        "model": os.getenv("VLLM_MODEL_8B"),
        "api_key": lambda: os.getenv("VLLM_API_KEY", "dummy-key"),
        "base_url": lambda: os.getenv("VLLM_BASE_URL")
    }
}


def select_model() -> str:
    """
    Prompt user to select a model interactively.
    
    Returns:
        Selected model key (e.g., "gpt-5", "local-70b")
    """
    print("\nSelect model:")
    print("  1. gpt-5 (OpenAI - best reasoning) [DEFAULT]")
    print("  2. gpt-4.1 (OpenAI - large context)")
    print("  3. local-70b (llama.cpp - Llama 3.3 70B)")
    print("  4. local-32b (llama.cpp - QWEN3 32B)")
    print("  5. local-8b (vLLM - Llama 3.1 8B)")
    
    choice = input("Choice (1-5) [1]: ").strip()
    
    model_map = {
        "1": "gpt-5",
        "2": "gpt-4.1",
        "3": "local-70b",
        "4": "local-32b",
        "5": "local-8b",
        "": "gpt-5"  # Default
    }
    
    selected = model_map.get(choice, "gpt-5")
    config = MODEL_OPTIONS[selected]
    
    print(f"Selected: {config['name']}")
    return selected


def get_model_config(model_key: str) -> Dict:
    """
    Get configuration for a specific model.
    
    Args:
        model_key: Model identifier (e.g., "gpt-5", "local-70b")
        
    Returns:
        Dictionary with model configuration
        
    Raises:
        ValueError: If model_key is not recognized
    """
    if model_key not in MODEL_OPTIONS:
        raise ValueError(f"Unknown model: {model_key}. Valid options: {list(MODEL_OPTIONS.keys())}")
    
    return MODEL_OPTIONS[model_key]


def get_llm_client(model_key: Optional[str] = None, temperature: float = 0.2):
    """
    Create an LLM client for the specified model.
    
    Args:
        model_key: Model identifier (e.g., "gpt-5"). If None, prompts user to select.
        temperature: Temperature setting for generation (default 0.2)
        
    Returns:
        Configured LLMClient instance
    """
    from src.core.llm_client import LLMClient
    
    if model_key is None:
        model_key = select_model()
    
    config = get_model_config(model_key)
    
    # Get the actual values from lambdas
    api_key = config["api_key"]() if callable(config["api_key"]) else config["api_key"]
    base_url = config["base_url"]() if callable(config["base_url"]) else config["base_url"]
    model_name = config["model"]
    
    # Set environment variables for LLMClient
    if api_key:
        os.environ["OPENAI_API_KEY"] = api_key
    if base_url:
        os.environ["OPENAI_BASE_URL"] = base_url
    if model_name:
        os.environ["OPENAI_MODEL"] = model_name
    
    # Create client
    client = LLMClient(model=model_name, base_url=base_url, temperature=temperature)
    
    return client


def prompt_user(message: str, default: str = "N") -> bool:
    """
    Prompt user for yes/no input with default.
    
    Args:
        message: Prompt message
        default: Default value ("Y" or "N")
        
    Returns:
        True if yes, False if no
    """
    default_display = "Y/n" if default.upper() == "Y" else "y/N"
    response = input(f"{message} ({default_display}): ").strip().upper()
    
    if not response:
        response = default.upper()
    
    return response == "Y"


def estimate_cost(model_key: str, input_tokens: int, output_tokens: int) -> float:
    """
    Estimate cost for a model run (approximate, for planning purposes).
    
    Args:
        model_key: Model identifier
        input_tokens: Estimated input tokens
        output_tokens: Estimated output tokens
        
    Returns:
        Estimated cost in USD
    """
    # Rough pricing estimates (as of 2025, subject to change)
    pricing = {
        "gpt-5": {"input": 0.01, "output": 0.03},  # per 1K tokens (estimated)
        "gpt-4.1": {"input": 0.005, "output": 0.015},  # per 1K tokens (estimated)
        "local-70b": {"input": 0.0, "output": 0.0},  # Free (local)
        "local-32b": {"input": 0.0, "output": 0.0},  # Free (local)
        "local-8b": {"input": 0.0, "output": 0.0},  # Free (local)
    }
    
    if model_key not in pricing:
        return 0.0
    
    rates = pricing[model_key]
    cost = (input_tokens / 1000 * rates["input"]) + (output_tokens / 1000 * rates["output"])
    
    return cost