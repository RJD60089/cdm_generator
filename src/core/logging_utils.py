"""
Logging utilities for CDM generation.
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def setup_logging(log_dir: str = "logs") -> Path:
    """
    Set up logging directory.
    
    Args:
        log_dir: Directory for log files
        
    Returns:
        Path to log directory
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    return log_path


def log_step(
    step_name: str,
    status: str,
    details: Dict[str, Any] | None = None,
    log_file: str = "logs/steps.log"
) -> None:
    """
    Log a step execution.
    
    Args:
        step_name: Name of the step
        status: Status (e.g., "started", "completed", "failed")
        details: Optional additional details
        log_file: Path to log file
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "step": step_name,
        "status": status,
        "details": details or {}
    }
    
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry) + '\n')


def append_runlog(log_file: str, data: Dict[str, Any]) -> None:
    """
    Append data to a JSONL run log file.
    
    Args:
        log_file: Path to log file
        data: Data to log
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        **data
    }
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')


def read_runlog(log_file: str) -> list[Dict[str, Any]]:
    """
    Read entries from a JSONL run log file.
    
    Args:
        log_file: Path to log file
        
    Returns:
        List of log entries
    """
    if not os.path.exists(log_file):
        return []
    
    entries = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    # Skip malformed lines
                    continue
    
    return entries
