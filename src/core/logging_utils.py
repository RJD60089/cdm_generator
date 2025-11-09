# src/core/logging_utils.py
"""
Enhanced logging utilities for CDM generation application.
Provides structured logging, run logs, and usage tracking.
"""
from __future__ import annotations
import os
import json
import time
import logging
from pathlib import Path
from typing import Any, Dict
from rich.console import Console
from rich.logging import RichHandler


def setup_logging(
    output_dir: Path,
    run_id: str,
    log_level: str = "INFO",
    verbose: bool = False
) -> logging.Logger:
    """
    Configure structured logging for the application.
    
    Sets up both file and console handlers with appropriate formatting.
    
    Args:
        output_dir: Directory for log files
        run_id: Unique run identifier for log file naming
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        verbose: Enable verbose output
    
    Returns:
        Configured logger instance
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / f"{run_id}.app.log"
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Set level
    level = logging.DEBUG if verbose else getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(level)
    
    # File handler - detailed logs
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)  # Always debug to file
    file_formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler - user-friendly output using Rich
    console_handler = RichHandler(
        rich_tracebacks=True,
        show_time=False,
        show_path=False,
        markup=True
    )
    console_handler.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Reduce noise from libraries
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized: level={log_level}, log_file={log_file}")
    
    return root_logger


def append_runlog(path: str, record: dict) -> None:
    """
    Append a record to a JSONL run log file.
    
    Each record is timestamped and written as a single JSON line.
    
    Args:
        path: Path to the JSONL log file
        record: Dictionary to log (will be serialized to JSON)
    """
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        record["ts"] = time.time()
        record["ts_human"] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to write to runlog {path}: {e}")


def log_step_start(logger: logging.Logger, step_num: int, step_name: str):
    """Log the start of a pipeline step with formatting."""
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"STEP {step_num}: {step_name}")
    logger.info("=" * 70)


def log_step_complete(
    logger: logging.Logger,
    step_num: int,
    duration: float,
    validation_passed: bool = True
):
    """Log the completion of a pipeline step."""
    status = "✓ PASSED" if validation_passed else "✗ FAILED"
    logger.info(f"Step {step_num} complete: {duration:.1f}s [{status}]")
    logger.info("-" * 70)


def log_step_error(logger: logging.Logger, step_num: int, error: Exception):
    """Log a step error with formatting."""
    logger.error(f"Step {step_num} failed with error: {error}")
    logger.exception("Full traceback:")


class ProgressLogger:
    """
    Helper class for logging progress within a step.
    
    Provides consistent formatting for progress updates.
    """
    
    def __init__(self, logger: logging.Logger, step_name: str):
        self.logger = logger
        self.step_name = step_name
        self.start_time = time.time()
    
    def log(self, message: str, level: str = "INFO"):
        """Log a progress message."""
        elapsed = time.time() - self.start_time
        prefix = f"[{elapsed:6.1f}s]"
        
        log_func = getattr(self.logger, level.lower(), self.logger.info)
        log_func(f"{prefix} {message}")
    
    def info(self, message: str):
        """Log an info message."""
        self.log(message, "INFO")
    
    def debug(self, message: str):
        """Log a debug message."""
        self.log(message, "DEBUG")
    
    def warning(self, message: str):
        """Log a warning message."""
        self.log(message, "WARNING")
    
    def error(self, message: str):
        """Log an error message."""
        self.log(message, "ERROR")


def create_run_directory(base_dir: Path, domain: str, run_id: str) -> Path:
    """
    Create a directory for this run's outputs.
    
    Args:
        base_dir: Base output directory
        domain: CDM domain name
        run_id: Unique run identifier
    
    Returns:
        Path to the created run directory
    """
    run_dir = base_dir / f"{domain}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def get_run_files(base_dir: Path, domain: str, timestamp: str, run_id: str) -> Dict[str, Path]:
    """
    Generate standardized file paths for a run.
    
    Args:
        base_dir: Base output directory
        domain: CDM domain name
        timestamp: Run timestamp
        run_id: Unique run identifier
    
    Returns:
        Dictionary mapping file types to paths
    """
    base_name = f"{domain}_{timestamp}_{run_id}"
    
    return {
        "work_json": base_dir / f"{base_name}.work.json",
        "runlog": base_dir / f"{base_name}.run.jsonl",
        "app_log": base_dir / f"{run_id}.app.log",
        "excel": base_dir / f"{base_name}_CDM.xlsx",
        "ddl": base_dir / f"{base_name}_DDL.sql",
        "config": base_dir / f"{base_name}.config.json",
    }


def summarize_run_logs(runlog_path: Path) -> Dict[str, Any]:
    """
    Summarize information from a JSONL run log.
    
    Args:
        runlog_path: Path to the JSONL run log file
    
    Returns:
        Dictionary with summary statistics
    """
    if not runlog_path.exists():
        return {}
    
    total_tokens = 0
    llm_calls = 0
    errors = 0
    
    with open(runlog_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line)
                
                if record.get("usage"):
                    usage = record["usage"]
                    if isinstance(usage, dict) and "total_tokens" in usage:
                        total_tokens += usage["total_tokens"]
                        llm_calls += 1
                
                if record.get("level") == "ERROR":
                    errors += 1
            except json.JSONDecodeError:
                continue
    
    return {
        "total_tokens": total_tokens,
        "llm_calls": llm_calls,
        "errors": errors
    }


def print_banner(console: Console, text: str, style: str = "bold blue"):
    """
    Print a formatted banner to console.
    
    Args:
        console: Rich console instance
        text: Banner text
        style: Rich style string
    """
    console.print()
    console.rule(f"[{style}]{text}[/{style}]")
    console.print()


def print_config_summary(console: Console, config: Any):
    """
    Print a formatted configuration summary.
    
    Args:
        console: Rich console instance
        config: AppConfig instance
    """
    from rich.table import Table
    
    table = Table(title="Configuration Summary", show_header=True, header_style="bold")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    
    table.add_row("Domain", config.cdm.domain)
    table.add_row("Business Model", config.cdm.business_model)
    table.add_row("LLM Model", config.llm.model)
    table.add_row("Temperature", str(config.llm.temperature))
    table.add_row("Steps to Run", str(config.cdm.steps_to_run))
    table.add_row("Output Directory", str(config.paths.output_dir))
    
    if config.cdm.naming_standard_path:
        table.add_row("Naming Standard", str(config.cdm.naming_standard_path))
    
    if config.dry_run:
        table.add_row("Mode", "[yellow]DRY RUN[/yellow]")
    
    console.print(table)
    console.print()


def print_run_summary(console: Console, state: Any):
    """
    Print a formatted run summary.
    
    Args:
        console: Rich console instance
        state: RunState instance
    """
    from rich.table import Table
    
    console.print()
    console.rule("[bold green]Run Complete[/bold green]")
    console.print()
    
    # Summary table
    table = Table(show_header=False)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    
    table.add_row("Domain", state.meta.domain)
    table.add_row("Run ID", state.meta.run_id)
    table.add_row("Completion", f"{state.get_completion_percentage():.1f}%")
    table.add_row("Total Tokens", f"{state.get_total_tokens_used():,}")
    table.add_row("Total Duration", f"{state.get_total_duration():.1f}s")
    table.add_row("Entities", str(len(state.work.get("entities", []))))
    table.add_row("Attributes", str(len(state.work.get("attributes", []))))
    
    console.print(table)
    console.print()
