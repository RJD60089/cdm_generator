# src/core/run_state.py
"""
Enhanced run state management with step tracking, validation, and richer metadata.
Maintains all state throughout the CDM generation pipeline.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal
from enum import Enum
import json
import time
import hashlib
from pathlib import Path


class StepStatus(Enum):
    """Status of a pipeline step."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TokenUsage:
    """Token usage information from LLM calls."""
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "TokenUsage":
        return cls(
            prompt_tokens=data.get("prompt_tokens", 0),
            completion_tokens=data.get("completion_tokens", 0),
            total_tokens=data.get("total_tokens", 0)
        )


@dataclass
class StepResult:
    """Result and metadata from a pipeline step execution."""
    step_num: int
    step_name: str
    status: StepStatus
    started_at: str
    completed_at: str | None = None
    duration_seconds: float | None = None
    token_usage: TokenUsage | None = None
    validation_passed: bool = False
    error_message: str | None = None
    llm_calls_made: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_num": self.step_num,
            "step_name": self.step_name,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "token_usage": self.token_usage.to_dict() if self.token_usage else None,
            "validation_passed": self.validation_passed,
            "error_message": self.error_message,
            "llm_calls_made": self.llm_calls_made
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StepResult":
        token_usage = None
        if data.get("token_usage"):
            token_usage = TokenUsage.from_dict(data["token_usage"])
        
        return cls(
            step_num=data["step_num"],
            step_name=data["step_name"],
            status=StepStatus(data["status"]),
            started_at=data["started_at"],
            completed_at=data.get("completed_at"),
            duration_seconds=data.get("duration_seconds"),
            token_usage=token_usage,
            validation_passed=data.get("validation_passed", False),
            error_message=data.get("error_message"),
            llm_calls_made=data.get("llm_calls_made", 0)
        )


@dataclass
class RunMeta:
    """Metadata about the CDM generation run."""
    domain: str
    run_id: str
    timestamp: str
    model: str
    temperature: float
    business_model: Literal["passthrough", "spread"] = "passthrough"
    version: str = "2.0"  # App version for backwards compatibility
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunMeta":
        return cls(**data)


@dataclass
class RunState:
    """
    Complete state of a CDM generation run.
    
    Tracks metadata, naming rules, work products, step results, and inputs.
    Provides serialization, validation, and progress tracking.
    """
    meta: RunMeta
    naming_rules: Dict[str, Any] = field(default_factory=dict)
    work: Dict[str, Any] = field(default_factory=lambda: {
        "assumptions": [],
        "decisions": [],
        "open_questions": [],
        "entities": [],
        "relationships": [],
        "attributes": [],
        "keys": [],
        "reference_sets": [],
        "quality_rules": [],
        "lineage_notes": [],
        "stewardship": [],
        "core_functional_map": [],
        "confidence": {"per_tab": {}, "overall": None}
    })
    steps_completed: List[StepResult] = field(default_factory=list)
    inputs_used: Dict[str, str] = field(default_factory=dict)  # source_type: path
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entire state to dictionary for serialization."""
        return {
            "meta": self.meta.to_dict(),
            "naming_rules": self.naming_rules,
            "work": self.work,
            "steps_completed": [s.to_dict() for s in self.steps_completed],
            "inputs_used": self.inputs_used
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunState":
        """Reconstruct RunState from dictionary."""
        meta = RunMeta.from_dict(data["meta"])
        steps = [StepResult.from_dict(s) for s in data.get("steps_completed", [])]
        
        return cls(
            meta=meta,
            naming_rules=data.get("naming_rules", {}),
            work=data.get("work", cls._default_work()),
            steps_completed=steps,
            inputs_used=data.get("inputs_used", {})
        )
    
    @staticmethod
    def _default_work() -> Dict[str, Any]:
        """Return default work structure."""
        return {
            "assumptions": [],
            "decisions": [],
            "open_questions": [],
            "entities": [],
            "relationships": [],
            "attributes": [],
            "keys": [],
            "reference_sets": [],
            "quality_rules": [],
            "lineage_notes": [],
            "stewardship": [],
            "core_functional_map": [],
            "confidence": {"per_tab": {}, "overall": None}
        }
    
    @staticmethod
    def new(
        domain: str,
        model: str,
        temperature: float,
        business_model: Literal["passthrough", "spread"] = "passthrough"
    ) -> "RunState":
        """
        Create a new RunState for a CDM generation run.
        
        Args:
            domain: CDM domain name (e.g., "PlanBenefit")
            model: LLM model name
            temperature: LLM temperature setting
            business_model: PBM business model type
        
        Returns:
            New RunState instance
        """
        ts = time.strftime("%Y-%m-%d_%H%M%S")
        rid = hashlib.sha1(
            f"{domain}|{ts}|{model}|{temperature}|{business_model}".encode()
        ).hexdigest()[:10]
        
        meta = RunMeta(
            domain=domain,
            run_id=rid,
            timestamp=ts,
            model=model,
            temperature=temperature,
            business_model=business_model
        )
        
        return RunState(meta=meta)
    
    def mark_step_started(self, step_num: int, step_name: str) -> StepResult:
        """
        Mark a step as started and return the StepResult object.
        
        Args:
            step_num: Step number (1-5)
            step_name: Human-readable step name
        
        Returns:
            StepResult object that should be updated as step progresses
        """
        result = StepResult(
            step_num=step_num,
            step_name=step_name,
            status=StepStatus.IN_PROGRESS,
            started_at=time.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Remove any existing result for this step (in case of retry)
        self.steps_completed = [s for s in self.steps_completed if s.step_num != step_num]
        self.steps_completed.append(result)
        
        return result
    
    def mark_step_completed(
        self,
        step_num: int,
        validation_passed: bool = True,
        token_usage: TokenUsage | None = None,
        llm_calls_made: int = 0
    ):
        """
        Mark a step as completed.
        
        Args:
            step_num: Step number that completed
            validation_passed: Whether step output validation passed
            token_usage: Token usage from LLM calls
            llm_calls_made: Number of LLM calls made during step
        """
        for result in self.steps_completed:
            if result.step_num == step_num:
                result.status = StepStatus.COMPLETED
                result.completed_at = time.strftime("%Y-%m-%d %H:%M:%S")
                result.validation_passed = validation_passed
                result.token_usage = token_usage
                result.llm_calls_made = llm_calls_made
                
                # Calculate duration
                if result.started_at:
                    start = time.mktime(time.strptime(result.started_at, "%Y-%m-%d %H:%M:%S"))
                    end = time.mktime(time.strptime(result.completed_at, "%Y-%m-%d %H:%M:%S"))
                    result.duration_seconds = end - start
                break
    
    def mark_step_failed(self, step_num: int, error_message: str):
        """
        Mark a step as failed.
        
        Args:
            step_num: Step number that failed
            error_message: Error message describing the failure
        """
        for result in self.steps_completed:
            if result.step_num == step_num:
                result.status = StepStatus.FAILED
                result.completed_at = time.strftime("%Y-%m-%d %H:%M:%S")
                result.error_message = error_message
                
                # Calculate duration
                if result.started_at:
                    start = time.mktime(time.strptime(result.started_at, "%Y-%m-%d %H:%M:%S"))
                    end = time.mktime(time.strptime(result.completed_at, "%Y-%m-%d %H:%M:%S"))
                    result.duration_seconds = end - start
                break
    
    def mark_step_skipped(self, step_num: int, step_name: str, reason: str = ""):
        """
        Mark a step as skipped.
        
        Args:
            step_num: Step number to skip
            step_name: Human-readable step name
            reason: Reason for skipping
        """
        result = StepResult(
            step_num=step_num,
            step_name=step_name,
            status=StepStatus.SKIPPED,
            started_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            completed_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            error_message=reason if reason else None
        )
        
        # Remove any existing result for this step
        self.steps_completed = [s for s in self.steps_completed if s.step_num != step_num]
        self.steps_completed.append(result)
    
    def get_step_result(self, step_num: int) -> StepResult | None:
        """Get the result for a specific step number."""
        for result in self.steps_completed:
            if result.step_num == step_num:
                return result
        return None
    
    def is_step_completed(self, step_num: int) -> bool:
        """Check if a step has been completed successfully."""
        result = self.get_step_result(step_num)
        return result is not None and result.status == StepStatus.COMPLETED
    
    def get_completion_percentage(self) -> float:
        """
        Calculate completion percentage based on completed steps.
        
        Returns:
            Percentage (0-100)
        """
        total_steps = 5
        completed = len([
            s for s in self.steps_completed
            if s.status == StepStatus.COMPLETED
        ])
        return (completed / total_steps) * 100
    
    def get_total_tokens_used(self) -> int:
        """Calculate total tokens used across all steps."""
        total = 0
        for result in self.steps_completed:
            if result.token_usage:
                total += result.token_usage.total_tokens
        return total
    
    def get_total_duration(self) -> float:
        """Calculate total duration across all steps in seconds."""
        total = 0.0
        for result in self.steps_completed:
            if result.duration_seconds:
                total += result.duration_seconds
        return total
    
    def add_input_source(self, source_type: str, path: str):
        """
        Record an input source used in this run.
        
        Args:
            source_type: Type of input (e.g., "fhir", "guardrails", "ddl", "naming_standard")
            path: File path to the input
        """
        self.inputs_used[source_type] = path
    
    def save_to_file(self, filepath: Path):
        """
        Save state to JSON file.
        
        Args:
            filepath: Path to save the JSON file
        """
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load_from_file(cls, filepath: Path) -> "RunState":
        """
        Load state from JSON file.
        
        Args:
            filepath: Path to the JSON file
        
        Returns:
            RunState instance
        """
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def get_summary(self) -> str:
        """
        Get a human-readable summary of the run state.
        
        Returns:
            Formatted summary string
        """
        completed = len([s for s in self.steps_completed if s.status == StepStatus.COMPLETED])
        failed = len([s for s in self.steps_completed if s.status == StepStatus.FAILED])
        
        summary = [
            f"\nRun Summary: {self.meta.domain}",
            f"{'='*60}",
            f"Run ID: {self.meta.run_id}",
            f"Timestamp: {self.meta.timestamp}",
            f"Model: {self.meta.model} (temp={self.meta.temperature})",
            f"Business Model: {self.meta.business_model}",
            f"",
            f"Progress: {self.get_completion_percentage():.1f}% ({completed}/5 steps completed)",
            f"Failed Steps: {failed}",
            f"Total Tokens: {self.get_total_tokens_used():,}",
            f"Total Duration: {self.get_total_duration():.1f}s",
            f"",
            f"Entities Generated: {len(self.work.get('entities', []))}",
            f"Attributes Generated: {len(self.work.get('attributes', []))}",
            f"Relationships Generated: {len(self.work.get('relationships', []))}",
        ]
        
        if self.inputs_used:
            summary.append(f"\nInputs Used:")
            for source_type, path in self.inputs_used.items():
                summary.append(f"  - {source_type}: {path}")
        
        return "\n".join(summary)
