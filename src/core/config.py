# src/core/config.py
"""
Configuration management for CDM Generation Application.
Centralizes all settings, paths, and environment variables.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
import os
import json


@dataclass
class LLMConfig:
    """LLM service configuration."""
    model: str = "gpt-5"
    temperature: float = 0.2
    max_tokens: int = 4096
    base_url: str | None = None
    api_key: str | None = None
    timeout: int = 120  # seconds
    max_retries: int = 3
    
    @classmethod
    def from_env(cls) -> "LLMConfig":
        """Load LLM configuration from environment variables."""
        return cls(
            model=os.getenv("OPENAI_MODEL", "gpt-5"),
            temperature=float(os.getenv("TEMP_DEFAULT", "0.2")),
            max_tokens=int(os.getenv("MAX_TOKENS", "4096")),
            base_url=os.getenv("OPENAI_BASE_URL"),
            api_key=os.getenv("OPENAI_API_KEY"),
            timeout=int(os.getenv("LLM_TIMEOUT", "120")),
            max_retries=int(os.getenv("LLM_MAX_RETRIES", "3"))
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "base_url": self.base_url or "default",
            "timeout": self.timeout,
            "max_retries": self.max_retries
        }


@dataclass
class PathConfig:
    """Application path configuration."""
    output_dir: Path = Path("output")
    prompts_dir: Path = Path("prompts")
    standards_dir: Path = Path("standards")
    tests_dir: Path = Path("tests")
    
    # Input directories (for Step 2 preparation)
    fhir_input_dir: Path = Path("inputs/fhir")
    guardrails_dir: Path = Path("inputs/guardrails")
    ddl_input_dir: Path = Path("inputs/ddl")
    
    def ensure_dirs(self):
        """Create all necessary directories."""
        for dir_path in [
            self.output_dir,
            self.prompts_dir,
            self.standards_dir,
            self.fhir_input_dir,
            self.guardrails_dir,
            self.ddl_input_dir
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_args(cls, output_dir: str | None = None) -> "PathConfig":
        """Create PathConfig from command line arguments."""
        config = cls()
        if output_dir:
            config.output_dir = Path(output_dir)
        return config


@dataclass
class CDMConfig:
    """CDM domain-specific configuration."""
    domain: str
    business_model: Literal["passthrough", "spread"] = "passthrough"
    include_pbm_specific: bool = True
    
    # Standard file paths (can be None)
    naming_standard_path: Path | None = None
    
    # Input source paths (for Step 2)
    fhir_input_path: Path | None = None
    guardrails_path: Path | None = None
    ddl_input_path: Path | None = None
    
    # Processing options
    steps_to_run: list[int] = field(default_factory=lambda: [1, 2, 3, 4, 5])
    resume_from_step: int | None = None
    skip_excel_export: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "domain": self.domain,
            "business_model": self.business_model,
            "include_pbm_specific": self.include_pbm_specific,
            "naming_standard_path": str(self.naming_standard_path) if self.naming_standard_path else None,
            "steps_to_run": self.steps_to_run,
            "resume_from_step": self.resume_from_step,
            "skip_excel_export": self.skip_excel_export
        }


@dataclass
class AppConfig:
    """Main application configuration combining all settings."""
    llm: LLMConfig
    paths: PathConfig
    cdm: CDMConfig
    
    # Runtime settings
    verbose: bool = False
    dry_run: bool = False
    log_level: str = "INFO"
    
    @classmethod
    def from_env_and_args(
        cls,
        domain: str,
        standard: str | None = None,
        outdir: str | None = None,
        business_model: str = "passthrough",
        fhir_input: str | None = None,
        guardrails: str | None = None,
        ddl_input: str | None = None,
        resume_from_step: int | None = None,
        steps: str = "1-5",
        skip_excel: bool = False,
        verbose: bool = False,
        dry_run: bool = False
    ) -> "AppConfig":
        """
        Create AppConfig from environment variables and command line arguments.
        
        Args:
            domain: CDM domain name (e.g., "PlanBenefit", "Prescriber")
            standard: Path to naming standard file
            outdir: Output directory path
            business_model: "passthrough" or "spread"
            fhir_input: Path to FHIR input file
            guardrails: Path to guardrails file
            ddl_input: Path to DDL input file
            resume_from_step: Step number to resume from
            steps: Steps to run (e.g., "1-5" or "1,3,5")
            skip_excel: Skip Excel export
            verbose: Enable verbose logging
            dry_run: Dry run mode
        """
        llm_config = LLMConfig.from_env()
        path_config = PathConfig.from_args(outdir)
        
        # Parse steps argument
        steps_list = cls._parse_steps(steps)
        
        cdm_config = CDMConfig(
            domain=domain,
            business_model=business_model,  # type: ignore
            naming_standard_path=Path(standard) if standard else None,
            fhir_input_path=Path(fhir_input) if fhir_input else None,
            guardrails_path=Path(guardrails) if guardrails else None,
            ddl_input_path=Path(ddl_input) if ddl_input else None,
            steps_to_run=steps_list,
            resume_from_step=resume_from_step,
            skip_excel_export=skip_excel
        )
        
        log_level = "DEBUG" if verbose else "INFO"
        
        return cls(
            llm=llm_config,
            paths=path_config,
            cdm=cdm_config,
            verbose=verbose,
            dry_run=dry_run,
            log_level=log_level
        )
    
    @staticmethod
    def _parse_steps(steps_str: str) -> list[int]:
        """
        Parse steps string into list of step numbers.
        
        Examples:
            "1-5" -> [1, 2, 3, 4, 5]
            "1,3,5" -> [1, 3, 5]
            "1-3,5" -> [1, 2, 3, 5]
        """
        result = []
        for part in steps_str.split(","):
            part = part.strip()
            if "-" in part:
                start, end = map(int, part.split("-"))
                result.extend(range(start, end + 1))
            else:
                result.append(int(part))
        return sorted(set(result))  # Remove duplicates and sort
    
    def save_to_file(self, filepath: Path):
        """Save configuration to JSON file."""
        config_dict = {
            "llm": self.llm.to_dict(),
            "cdm": self.cdm.to_dict(),
            "verbose": self.verbose,
            "dry_run": self.dry_run,
            "log_level": self.log_level
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(config_dict, f, indent=2)
    
    def validate(self) -> list[str]:
        """
        Validate configuration and return list of errors.
        
        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        
        # Validate domain name
        if not self.cdm.domain or not self.cdm.domain.strip():
            errors.append("CDM domain name is required")
        
        # Validate steps
        if not self.cdm.steps_to_run:
            errors.append("At least one step must be specified")
        
        invalid_steps = [s for s in self.cdm.steps_to_run if s < 1 or s > 5]
        if invalid_steps:
            errors.append(f"Invalid step numbers: {invalid_steps} (must be 1-5)")
        
        # Validate resume_from_step
        if self.cdm.resume_from_step is not None:
            if self.cdm.resume_from_step < 1 or self.cdm.resume_from_step > 5:
                errors.append(f"resume_from_step must be 1-5, got {self.cdm.resume_from_step}")
        
        # Validate file paths exist if provided
        if self.cdm.naming_standard_path and not self.cdm.naming_standard_path.exists():
            errors.append(f"Naming standard file not found: {self.cdm.naming_standard_path}")
        
        if self.cdm.fhir_input_path and not self.cdm.fhir_input_path.exists():
            errors.append(f"FHIR input file not found: {self.cdm.fhir_input_path}")
        
        if self.cdm.guardrails_path and not self.cdm.guardrails_path.exists():
            errors.append(f"Guardrails file not found: {self.cdm.guardrails_path}")
        
        if self.cdm.ddl_input_path and not self.cdm.ddl_input_path.exists():
            errors.append(f"DDL input file not found: {self.cdm.ddl_input_path}")
        
        return errors
    
    def __str__(self) -> str:
        """Human-readable configuration summary."""
        return (
            f"CDM Generation Configuration\n"
            f"{'='*50}\n"
            f"Domain: {self.cdm.domain}\n"
            f"Business Model: {self.cdm.business_model}\n"
            f"LLM Model: {self.llm.model}\n"
            f"Temperature: {self.llm.temperature}\n"
            f"Steps to Run: {self.cdm.steps_to_run}\n"
            f"Output Directory: {self.paths.output_dir}\n"
            f"Verbose: {self.verbose}\n"
            f"Dry Run: {self.dry_run}\n"
        )
