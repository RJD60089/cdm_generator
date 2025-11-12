"""
FHIR profile converter.
FHIR files are already JSON - pass through directly.
"""
import json
from pathlib import Path


def convert_fhir_to_json(file_path: str) -> str:
    """
    FHIR files are already JSON - just read and return.
    
    Args:
        file_path: Path to FHIR profile JSON file
        
    Returns:
        JSON string (file contents as-is)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    file = Path(file_path)
    
    if not file.exists():
        raise FileNotFoundError(f"FHIR file not found: {file_path}")
    
    # Read and validate it's proper JSON
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
        # Validate JSON
        json.loads(content)
        return content


def extract_fhir_elements(fhir_json_str: str) -> list:
    """
    Extract element definitions from FHIR JSON string.
    Helper function for analysis.
    
    Args:
        fhir_json_str: JSON string from convert_fhir_to_json()
        
    Returns:
        List of element definitions
    """
    data = json.loads(fhir_json_str)
    
    snapshot = data.get("snapshot", {})
    elements = snapshot.get("element", [])
    
    return elements