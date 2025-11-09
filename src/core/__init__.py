# src/core/__init__.py
"""Core utilities for CDM generation application."""

from .config import AppConfig, LLMConfig, PathConfig, CDMConfig
from .run_state import RunState, StepStatus, StepResult, TokenUsage
from .llm_client import LLMClient, MockLLMClient
from .pipeline import CDMPipeline, CDMStep, PipelineError, DryRunPipeline
from .prompt_builder import PromptBuilder, create_default_templates
from .validators import (
    validate_step_output,
    validate_naming_conventions,
    EntitySchema,
    AttributeSchema,
    RelationshipSchema
)
from .logging_utils import (
    setup_logging,
    append_runlog,
    log_step_start,
    log_step_complete,
    log_step_error,
    ProgressLogger
)

__all__ = [
    # Config
    'AppConfig', 'LLMConfig', 'PathConfig', 'CDMConfig',
    
    # State
    'RunState', 'StepStatus', 'StepResult', 'TokenUsage',
    
    # LLM
    'LLMClient', 'MockLLMClient',
    
    # Pipeline
    'CDMPipeline', 'CDMStep', 'PipelineError', 'DryRunPipeline',
    
    # Prompts
    'PromptBuilder', 'create_default_templates',
    
    # Validation
    'validate_step_output', 'validate_naming_conventions',
    'EntitySchema', 'AttributeSchema', 'RelationshipSchema',
    
    # Logging
    'setup_logging', 'append_runlog',
    'log_step_start', 'log_step_complete', 'log_step_error',
    'ProgressLogger'
]
