# src/core/pipeline.py
"""
Pipeline orchestration for CDM generation.
Manages execution of steps, validation, error handling, and state management.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import logging
from pathlib import Path

from .run_state import RunState, StepStatus, TokenUsage
from .llm_client import LLMClient
from .logging_utils import log_step_start, log_step_complete, log_step_error, ProgressLogger
from .config import AppConfig


logger = logging.getLogger(__name__)


class CDMStep(ABC):
    """
    Abstract base class for a CDM generation pipeline step.
    
    Each step should:
    - Execute LLM calls to generate CDM content
    - Validate its output
    - Update the run state
    - Handle errors gracefully
    """
    
    def __init__(self, step_num: int, name: str, description: str = ""):
        """
        Initialize a pipeline step.
        
        Args:
            step_num: Step number (1-5)
            name: Human-readable step name
            description: Detailed description of what this step does
        """
        self.step_num = step_num
        self.name = name
        self.description = description
        self.logger = logging.getLogger(f"{__name__}.Step{step_num}")
    
    @abstractmethod
    def execute(
        self,
        state: RunState,
        llm: LLMClient,
        config: AppConfig,
        runlog_path: str
    ) -> RunState:
        """
        Execute this step and return updated state.
        
        Args:
            state: Current run state
            llm: LLM client for making completion requests
            config: Application configuration
            runlog_path: Path to run log file
        
        Returns:
            Updated run state
        
        Raises:
            Exception: If step execution fails
        """
        pass
    
    @abstractmethod
    def validate_output(self, state: RunState) -> tuple[bool, str]:
        """
        Validate the output of this step.
        
        Args:
            state: Run state after step execution
        
        Returns:
            Tuple of (is_valid, error_message)
            If valid, error_message should be empty string
        """
        pass
    
    def get_prerequisites(self) -> List[int]:
        """
        Get list of step numbers that must complete before this step.
        
        Returns:
            List of prerequisite step numbers (empty if no prerequisites)
        """
        # By default, each step requires the previous step
        if self.step_num > 1:
            return [self.step_num - 1]
        return []
    
    def __str__(self) -> str:
        return f"Step {self.step_num}: {self.name}"


class CDMPipeline:
    """
    Orchestrates execution of CDM generation steps.
    
    Manages:
    - Step registration and ordering
    - State management across steps
    - Error handling and recovery
    - Progress tracking
    - Checkpointing
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize the pipeline.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.steps: Dict[int, CDMStep] = {}
        self.logger = logging.getLogger(__name__)
    
    def register_step(self, step: CDMStep):
        """
        Register a step with the pipeline.
        
        Args:
            step: CDMStep instance to register
        """
        if step.step_num in self.steps:
            self.logger.warning(f"Overwriting existing step {step.step_num}")
        
        self.steps[step.step_num] = step
        self.logger.debug(f"Registered: {step}")
    
    def run(
        self,
        initial_state: RunState,
        llm: LLMClient,
        runlog_path: str,
        checkpoint_path: Path | None = None
    ) -> RunState:
        """
        Run the pipeline with all registered steps.
        
        Args:
            initial_state: Starting run state
            llm: LLM client for steps to use
            runlog_path: Path to run log file
            checkpoint_path: Optional path for state checkpointing
        
        Returns:
            Final run state after all steps complete
        
        Raises:
            PipelineError: If a critical step fails
        """
        state = initial_state
        
        # Determine which steps to execute
        steps_to_run = self._get_steps_to_run(state)
        
        if not steps_to_run:
            self.logger.warning("No steps to run!")
            return state
        
        self.logger.info(f"Pipeline will execute steps: {steps_to_run}")
        
        # Execute each step
        for step_num in steps_to_run:
            if step_num not in self.steps:
                self.logger.error(f"Step {step_num} not registered with pipeline!")
                state.mark_step_skipped(
                    step_num,
                    f"Step{step_num}",
                    "Step not implemented"
                )
                continue
            
            step = self.steps[step_num]
            
            # Check prerequisites
            prereqs = step.get_prerequisites()
            if not self._check_prerequisites(state, prereqs):
                self.logger.error(
                    f"Prerequisites not met for {step}. "
                    f"Required: {prereqs}"
                )
                state.mark_step_skipped(
                    step_num,
                    step.name,
                    f"Prerequisites not met: {prereqs}"
                )
                continue
            
            # Execute the step
            try:
                log_step_start(self.logger, step.step_num, step.name)
                
                # Mark step as started
                step_result = state.mark_step_started(step.step_num, step.name)
                
                # Execute
                state = step.execute(state, llm, self.config, runlog_path)
                
                # Validate
                is_valid, error_msg = step.validate_output(state)
                
                if not is_valid:
                    self.logger.error(f"Validation failed: {error_msg}")
                    state.mark_step_failed(step.step_num, error_msg)
                    
                    if self._is_critical_step(step.step_num):
                        raise PipelineError(
                            f"Critical step {step.step_num} failed validation: {error_msg}"
                        )
                else:
                    # Mark as completed
                    state.mark_step_completed(
                        step.step_num,
                        validation_passed=True
                    )
                    
                    duration = step_result.duration_seconds or 0.0
                    log_step_complete(self.logger, step.step_num, duration, True)
                
                # Checkpoint state if configured
                if checkpoint_path:
                    self._checkpoint_state(state, checkpoint_path)
            
            except Exception as e:
                log_step_error(self.logger, step.step_num, e)
                state.mark_step_failed(step.step_num, str(e))
                
                if self._is_critical_step(step.step_num):
                    raise PipelineError(f"Critical step {step.step_num} failed") from e
                
                self.logger.warning(f"Continuing despite error in step {step.step_num}")
        
        return state
    
    def _get_steps_to_run(self, state: RunState) -> List[int]:
        """
        Determine which steps should be executed.
        
        Considers:
        - Configuration (steps_to_run)
        - Resume point (resume_from_step)
        - Already completed steps
        
        Args:
            state: Current run state
        
        Returns:
            Ordered list of step numbers to execute
        """
        # Start with configured steps
        steps = sorted(self.config.cdm.steps_to_run)
        
        # Handle resume
        if self.config.cdm.resume_from_step:
            resume_from = self.config.cdm.resume_from_step
            self.logger.info(f"Resuming from step {resume_from}")
            steps = [s for s in steps if s >= resume_from]
        
        # Filter out already completed steps (unless resuming)
        if not self.config.cdm.resume_from_step:
            steps = [
                s for s in steps
                if not state.is_step_completed(s)
            ]
        
        return steps
    
    def _check_prerequisites(self, state: RunState, prereqs: List[int]) -> bool:
        """
        Check if prerequisite steps have been completed.
        
        Args:
            state: Current run state
            prereqs: List of prerequisite step numbers
        
        Returns:
            True if all prerequisites are met
        """
        for prereq_num in prereqs:
            if not state.is_step_completed(prereq_num):
                return False
        return True
    
    def _is_critical_step(self, step_num: int) -> bool:
        """
        Determine if a step is critical (pipeline should stop if it fails).
        
        Args:
            step_num: Step number to check
        
        Returns:
            True if step is critical
        """
        # Steps 1 and 2 are critical - can't continue without them
        return step_num in [1, 2]
    
    def _checkpoint_state(self, state: RunState, checkpoint_path: Path):
        """
        Save current state to checkpoint file.
        
        Args:
            state: Current run state
            checkpoint_path: Path to save checkpoint
        """
        try:
            state.save_to_file(checkpoint_path)
            self.logger.debug(f"State checkpointed to {checkpoint_path}")
        except Exception as e:
            self.logger.warning(f"Failed to checkpoint state: {e}")
    
    def get_step(self, step_num: int) -> CDMStep | None:
        """
        Get a registered step by number.
        
        Args:
            step_num: Step number (1-5)
        
        Returns:
            CDMStep instance or None if not registered
        """
        return self.steps.get(step_num)
    
    def get_all_steps(self) -> List[CDMStep]:
        """
        Get all registered steps in order.
        
        Returns:
            List of CDMStep instances ordered by step_num
        """
        return [self.steps[num] for num in sorted(self.steps.keys())]


class PipelineError(Exception):
    """Exception raised when a critical pipeline error occurs."""
    pass


class DryRunPipeline(CDMPipeline):
    """
    Dry-run version of pipeline that simulates execution without LLM calls.
    
    Useful for:
    - Testing configuration
    - Validating inputs
    - Estimating costs
    """
    
    def run(
        self,
        initial_state: RunState,
        llm: LLMClient,
        runlog_path: str,
        checkpoint_path: Path | None = None
    ) -> RunState:
        """
        Simulate pipeline execution without making LLM calls.
        
        Args:
            initial_state: Starting run state
            llm: LLM client (not actually used)
            runlog_path: Path to run log file
            checkpoint_path: Optional checkpoint path
        
        Returns:
            Simulated run state
        """
        state = initial_state
        steps_to_run = self._get_steps_to_run(state)
        
        self.logger.info("[DRY RUN] Would execute steps: %s", steps_to_run)
        
        for step_num in steps_to_run:
            if step_num not in self.steps:
                self.logger.warning(f"[DRY RUN] Step {step_num} not registered")
                continue
            
            step = self.steps[step_num]
            self.logger.info(f"[DRY RUN] Would execute: {step}")
            
            # Simulate step execution
            state.mark_step_started(step.step_num, step.name)
            state.mark_step_completed(step.step_num, validation_passed=True)
        
        self.logger.info("[DRY RUN] Pipeline simulation complete")
        return state
