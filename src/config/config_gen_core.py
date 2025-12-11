"""
Core components for CDM configuration generation.

Provides:
- ConfigGeneratorBase: Base class with shared functionality
- Configuration validation and loading
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import config_utils


class ConfigGeneratorBase:
    """Base class for config generation modules."""
    
    def __init__(self, cdm_name: str, llm_client=None):
        """Initialize config generator.
        
        Args:
            cdm_name: CDM name (e.g., 'plan', 'formulary')
            llm_client: Optional LLM client for AI analysis
        """
        self.cdm_name = cdm_name
        self.safe_name = config_utils.safe_cdm_name(cdm_name)
        self.llm_client = llm_client
        
        # Standard paths
        self.project_root = config_utils.get_project_root()
        self.input_dir = config_utils.get_input_dir()
        self.cdm_dir = config_utils.get_cdm_dir(cdm_name)
        self.config_dir = config_utils.get_config_dir(cdm_name)
        
    def load_base_config(self) -> Optional[Dict]:
        """Load base config template.
        
        Returns:
            Config dict or None if not found
        """
        base_config = config_utils.find_base_config(self.cdm_name)
        if base_config:
            return config_utils.load_json_file(base_config)
        return None
    
    def load_latest_config(self) -> Optional[Dict]:
        """Load latest timestamped config.
        
        Returns:
            Config dict or None if not found
        """
        latest = config_utils.find_latest_config(self.cdm_name)
        if latest:
            return config_utils.load_json_file(latest)
        return None
    
    def validate_base_config(self, config: Dict) -> List[str]:
        """Validate base config has required fields.
        
        Args:
            config: Config dict to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Required top-level sections
        required_sections = ['cdm', 'input_files', 'thresholds']
        for section in required_sections:
            if section not in config:
                errors.append(f"Missing required section: {section}")
        
        if 'cdm' in config:
            # Required CDM fields
            cdm_required = ['domain', 'type', 'description']
            for field in cdm_required:
                if not config['cdm'].get(field):
                    errors.append(f"Missing CDM field: {field}")
            
            # Validate type
            cdm_type = config['cdm'].get('type', '').lower()
            if cdm_type and cdm_type not in ['core', 'functional']:
                errors.append(f"Invalid CDM type: {cdm_type} (must be 'core' or 'functional')")
            
            # Functional CDMs need core_dependency
            if cdm_type == 'functional' and not config['cdm'].get('core_dependency'):
                errors.append("Functional CDMs must specify core_dependency")
        
        return errors
    
    def generate_timestamp_filename(self, prefix: str = "config") -> str:
        """Generate timestamped config filename.
        
        Args:
            prefix: Filename prefix (default 'config')
            
        Returns:
            Filename like config_plan_20251211_143022.json
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}_{self.safe_name}_{timestamp}.json"
    
    def save_config(self, config: Dict, filename: Optional[str] = None) -> Path:
        """Save config to timestamped file.
        
        Args:
            config: Config dict to save
            filename: Optional filename (auto-generates if not provided)
            
        Returns:
            Path to saved file
        """
        if filename is None:
            filename = self.generate_timestamp_filename()
        
        filepath = self.config_dir / filename
        config_utils.save_json_file(filepath, config)
        
        return filepath
    
    def parse_ai_json_response(self, response_text: str) -> Dict:
        """Parse AI response text as JSON.
        
        Handles common issues like markdown code blocks.
        
        Args:
            response_text: Raw AI response text
            
        Returns:
            Parsed JSON dict
            
        Raises:
            json.JSONDecodeError: If parsing fails
        """
        text = response_text.strip()
        
        # Remove markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("```")
            if len(lines) >= 2:
                text = lines[1]
                # Remove language identifier (e.g., 'json')
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
        
        return json.loads(text)
    
    def call_llm(self, prompt: str) -> str:
        """Call LLM with prompt and return response text.
        
        Args:
            prompt: Prompt text
            
        Returns:
            Response text
            
        Raises:
            ValueError: If no LLM client configured
        """
        if self.llm_client is None:
            raise ValueError("No LLM client configured")
        
        messages = [{"role": "user", "content": prompt}]
        response_text, _ = self.llm_client.chat(messages)
        return response_text


def merge_config_sections(base: Dict, updates: Dict, sections: List[str]) -> Dict:
    """Merge specific sections from updates into base config.
    
    Args:
        base: Base config dict
        updates: Updates to merge
        sections: List of section keys to merge
        
    Returns:
        Merged config dict
    """
    result = base.copy()
    
    for section in sections:
        if section in updates:
            if section not in result:
                result[section] = {}
            
            if isinstance(updates[section], dict):
                result[section].update(updates[section])
            else:
                result[section] = updates[section]
    
    return result


def prompt_user_choice(message: str, default: str = "Y") -> bool:
    """Prompt user for Y/N choice.
    
    Args:
        message: Prompt message
        default: Default value ('Y' or 'N')
        
    Returns:
        True if yes, False if no
    """
    default_upper = default.upper()
    hint = "[Y/n]" if default_upper == "Y" else "[y/N]"
    
    response = input(f"{message} {hint}: ").strip().upper()
    
    if not response:
        return default_upper == "Y"
    
    return response in ["Y", "YES"]
