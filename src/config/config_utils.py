"""
Shared utilities for CDM configuration management.

Provides common functions for:
- Finding config directories and files by CDM name
- Path normalization
- File searching
"""
import json
from pathlib import Path
from typing import Any, Optional, Dict, List


def get_project_root() -> Path:
    """Get project root directory (assumes src/config location)."""
    # This file lives in src/config, so go up 2 levels
    return Path(__file__).parent.parent.parent


def get_input_dir() -> Path:
    """Get input directory path."""
    return get_project_root() / "input"


def get_business_dir() -> Path:
    """Get business input directory path."""
    return get_input_dir() / "business"


def get_standards_fhir_dir() -> Path:
    """Get FHIR/IG standards directory."""
    return get_input_dir() / "strd_fhir_ig"


def get_standards_ncpdp_dir() -> Path:
    """Get NCPDP standards directory."""
    return get_input_dir() / "strd_ncpdp"


def safe_cdm_name(cdm_name: str) -> str:
    """Convert CDM name to safe directory/file format.
    
    Examples:
        'Plan and Benefit' -> 'plan_and_benefit'
        'plan' -> 'plan'
    """
    return cdm_name.lower().replace(' ', '_').replace('&', 'and')


def get_cdm_dir(cdm_name: str) -> Path:
    """Get CDM-specific directory.
    
    Args:
        cdm_name: CDM name (e.g., 'plan', 'formulary', 'Plan and Benefit')
        
    Returns:
        Path to input/business/cdm_{name}/
    """
    safe_name = safe_cdm_name(cdm_name)
    return get_business_dir() / f"cdm_{safe_name}"


def get_config_dir(cdm_name: str) -> Path:
    """Get config directory for a CDM.
    
    Args:
        cdm_name: CDM name (e.g., 'plan', 'formulary')
        
    Returns:
        Path to input/business/cdm_{name}/config/
    """
    return get_cdm_dir(cdm_name) / "config"


def find_latest_config(cdm_name: str) -> Optional[Path]:
    """Find latest timestamped config file for a CDM.
    
    Searches for config files matching pattern: config_{name}_*.json
    Returns the most recent timestamped version, or base config if no timestamped exists.
    
    Args:
        cdm_name: CDM name (e.g., 'plan', 'formulary')
        
    Returns:
        Path to config file, or None if not found
    """
    config_dir = get_config_dir(cdm_name)
    safe_name = safe_cdm_name(cdm_name)
    base_name = f"config_{safe_name}"
    
    if not config_dir.exists():
        return None
    
    # Find all timestamped configs (newest first)
    pattern = f"{base_name}_*.json"
    timestamped = sorted(config_dir.glob(pattern), reverse=True)
    
    if timestamped:
        return timestamped[0]
    
    # Fall back to base config
    base_config = config_dir / f"{base_name}.json"
    if base_config.exists():
        return base_config
    
    return None


def find_base_config(cdm_name: str) -> Optional[Path]:
    """Find base (non-timestamped) config template.
    
    Args:
        cdm_name: CDM name (e.g., 'plan', 'formulary')
        
    Returns:
        Path to base config file, or None if not found
    """
    config_dir = get_config_dir(cdm_name)
    safe_name = safe_cdm_name(cdm_name)
    base_config = config_dir / f"config_{safe_name}.json"
    
    if base_config.exists():
        return base_config
    
    return None


def load_json_file(filepath: Path) -> Dict:
    """Load and parse a JSON file.
    
    Args:
        filepath: Path to JSON file
        
    Returns:
        Parsed JSON data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json_file(filepath: Path, data: Any, indent: int = 2) -> None:
    """Save data to JSON file.
    
    Args:
        filepath: Path to save file
        data: Data to serialize (Dict or List)
        indent: JSON indent level (default 2)
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def find_file_recursive(base_dir: Path, filename: str) -> Optional[Path]:
    """Recursively search for exact filename in directory tree.
    
    Args:
        base_dir: Directory to search
        filename: Exact filename to find
        
    Returns:
        Path to file if found (first match), None otherwise
    """
    if not base_dir.exists():
        return None
    
    matches = list(base_dir.rglob(filename))
    
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        # Multiple matches - return first but log warning
        print(f"   ⚠️  Multiple matches for {filename}, using first")
        return matches[0]
    
    return None


def normalize_path(filepath: Path, project_root: Optional[Path] = None) -> str:
    """Convert absolute path to relative path within project.
    
    Args:
        filepath: Path to normalize
        project_root: Project root (defaults to detected root)
        
    Returns:
        Relative path string if within project, absolute otherwise
    """
    if project_root is None:
        project_root = get_project_root()
    
    filepath = Path(filepath)
    
    if filepath.is_absolute():
        try:
            return str(filepath.relative_to(project_root))
        except ValueError:
            # Path is outside project
            return str(filepath)
    
    return str(filepath)


def list_files_in_dir(directory: Path, pattern: str = "*.json") -> List[Path]:
    """List files matching pattern in directory.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern (default *.json)
        
    Returns:
        List of matching file paths
    """
    if not directory.exists():
        return []
    
    return sorted(directory.glob(pattern))