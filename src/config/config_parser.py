"""
Configuration parser for CDM generation application.
Loads and validates JSON config files.
"""
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass


@dataclass
class CDMConfig:
    """CDM metadata"""
    domain: str
    description: Optional[str] = None


@dataclass
class InputsConfig:
    """Input file paths - all support single file or list of files"""
    fhir: Optional[Union[str, List[str]]] = None
    guardrails: Optional[Union[str, List[str]]] = None
    ddl: Optional[Union[str, List[str]]] = None
    naming_standard: Optional[Union[str, List[str]]] = None
    
    def normalize(self):
        """Convert all inputs to lists for consistent processing"""
        self.fhir = self._to_list(self.fhir)
        self.guardrails = self._to_list(self.guardrails)
        self.ddl = self._to_list(self.ddl)
        self.naming_standard = self._to_list(self.naming_standard)
    
    @staticmethod
    def _to_list(value: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
        """Convert single string or list to list, or None"""
        if value is None:
            return None
        if isinstance(value, str):
            return [value]
        return value


@dataclass
class OutputConfig:
    """Output configuration"""
    directory: str
    filename: Optional[str] = None


@dataclass
class AppConfig:
    """Complete application configuration"""
    cdm: CDMConfig
    inputs: InputsConfig
    output: OutputConfig
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Validate required fields
        if not self.cdm.domain:
            errors.append("cdm.domain is required")
        
        if not self.output.directory:
            errors.append("output.directory is required")
        
        # Normalize inputs to lists
        self.inputs.normalize()
        
        # Validate file paths exist
        if self.inputs.fhir:
            for fhir_file in self.inputs.fhir:
                if not Path(fhir_file).exists():
                    errors.append(f"FHIR file not found: {fhir_file}")
        
        if self.inputs.guardrails:
            for gr_file in self.inputs.guardrails:
                if not Path(gr_file).exists():
                    errors.append(f"Guardrails file not found: {gr_file}")
        
        if self.inputs.ddl:
            for ddl_file in self.inputs.ddl:
                if not Path(ddl_file).exists():
                    errors.append(f"DDL file not found: {ddl_file}")
        
        if self.inputs.naming_standard:
            for ns_file in self.inputs.naming_standard:
                if not Path(ns_file).exists():
                    errors.append(f"Naming standard file not found: {ns_file}")
        
        return errors


def load_config(config_path: str) -> AppConfig:
    """
    Load and validate JSON configuration file.
    
    Args:
        config_path: Path to JSON config file
        
    Returns:
        AppConfig object
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config is not valid JSON
        ValueError: If config validation fails
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    # Load JSON
    with open(config_file, 'r') as f:
        data = json.load(f)
    
    # Parse into config objects
    try:
        cdm_config = CDMConfig(
            domain=data['cdm']['domain'],
            description=data['cdm'].get('description')
        )
        
        inputs_data = data.get('inputs', {})
        inputs_config = InputsConfig(
            fhir=inputs_data.get('fhir'),
            guardrails=inputs_data.get('guardrails'),
            ddl=inputs_data.get('ddl'),
            naming_standard=inputs_data.get('naming_standard')
        )
        
        output_data = data['output']
        output_config = OutputConfig(
            directory=output_data['directory'],
            filename=output_data.get('filename')
        )
        
        config = AppConfig(
            cdm=cdm_config,
            inputs=inputs_config,
            output=output_config
        )
        
    except KeyError as e:
        raise ValueError(f"Missing required config field: {e}")
    
    # Validate
    errors = config.validate()
    if errors:
        raise ValueError(f"Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
    
    return config


def create_default_output_filename(domain: str) -> str:
    """Create default output filename from domain name"""
    # Convert "Plan and Benefit" -> "Plan_and_Benefit_CDM.xlsx"
    safe_name = domain.replace(' ', '_').replace('/', '_')
    return f"{safe_name}_CDM.xlsx"