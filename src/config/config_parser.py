"""
Configuration parser for CDM generation application.
Loads and validates JSON config files.
"""
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass, field


@dataclass
class CDMConfig:
    """CDM metadata"""
    domain: str
    type: str = "Core"
    description: Optional[str] = None
    version: Optional[str] = None


@dataclass
class OutputConfig:
    """Output configuration"""
    directory: str
    filename: Optional[str] = None


@dataclass
class AppConfig:
    """Complete application configuration"""
    cdm: CDMConfig
    output: OutputConfig
    config_path: str = ""
    
    # Input files - structured per config_generator output
    fhir_igs: List[Dict[str, Any]] = field(default_factory=list)
    guardrails: List[str] = field(default_factory=list)
    glue: List[str] = field(default_factory=list)
    ddl: List[str] = field(default_factory=list)
    ncpdp_general_standards: List[Dict[str, Any]] = field(default_factory=list)
    ncpdp_script_standards: List[Dict[str, Any]] = field(default_factory=list)
    naming_standard: List[str] = field(default_factory=list)
    
    # Thresholds
    entity_threshold: float = 0.006
    attribute_threshold: float = 0.004
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self, check_files: bool = False) -> List[str]:
        """Validate configuration and return list of errors
        
        Args:
            check_files: If True, verify that referenced files exist
        """
        errors = []
        
        # Validate required fields
        if not self.cdm.domain:
            errors.append("cdm.domain is required")
        
        if not self.output.directory:
            errors.append("output.directory is required")
        
        # Optionally validate file paths exist
        if check_files:
            # Check FHIR files
            for fhir_ig in self.fhir_igs:
                file_path = fhir_ig.get('file', '')
                if file_path and not Path(file_path).exists():
                    errors.append(f"FHIR file not found: {file_path}")
            
            # Check guardrails files
            for gr_file in self.guardrails:
                if not Path(gr_file).exists():
                    errors.append(f"Guardrails file not found: {gr_file}")
            
            # Check glue files
            for glue_file in self.glue:
                if not Path(glue_file).exists():
                    errors.append(f"Glue file not found: {glue_file}")
            
            # Check DDL files
            for ddl_file in self.ddl:
                if not Path(ddl_file).exists():
                    errors.append(f"DDL file not found: {ddl_file}")
            
            # Check naming standard files
            for ns_file in self.naming_standard:
                if not Path(ns_file).exists():
                    errors.append(f"Naming standard file not found: {ns_file}")
        
        return errors
    
    def has_fhir(self) -> bool:
        """Check if FHIR IGs are configured"""
        return len(self.fhir_igs) > 0
    
    def has_ncpdp(self) -> bool:
        """Check if NCPDP standards are configured"""
        return len(self.ncpdp_general_standards) > 0 or len(self.ncpdp_script_standards) > 0
    
    def has_guardrails(self) -> bool:
        """Check if guardrails files are configured"""
        return len(self.guardrails) > 0
    
    def has_glue(self) -> bool:
        """Check if glue schemas are configured"""
        return len(self.glue) > 0
    
    def get_fhir_by_type(self, file_type: str) -> List[Dict[str, Any]]:
        """Get FHIR IGs filtered by file_type (StructureDefinition, ValueSet, CodeSystem)"""
        return [ig for ig in self.fhir_igs if ig.get('file_type') == file_type]
    
    def get_structure_definitions(self) -> List[Dict[str, Any]]:
        """Get FHIR StructureDefinitions"""
        return self.get_fhir_by_type('StructureDefinition')
    
    def get_value_sets(self) -> List[Dict[str, Any]]:
        """Get FHIR ValueSets"""
        return self.get_fhir_by_type('ValueSet')
    
    def get_code_systems(self) -> List[Dict[str, Any]]:
        """Get FHIR CodeSystems"""
        return self.get_fhir_by_type('CodeSystem')


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
        cdm_data = data['cdm']
        cdm_config = CDMConfig(
            domain=cdm_data['domain'],
            type=cdm_data.get('type', 'Core'),
            description=cdm_data.get('description'),
            version=cdm_data.get('version')
        )
        
        output_data = data['output']
        output_config = OutputConfig(
            directory=output_data['directory'],
            filename=output_data.get('filename')
        )
        
        # Parse input_files section
        input_files = data.get('input_files', {})
        
        config = AppConfig(
            cdm=cdm_config,
            output=output_config,
            config_path=str(config_path),
            fhir_igs=input_files.get('fhir_igs', []),
            guardrails=input_files.get('guardrails', []),
            glue=input_files.get('glue', []),
            ddl=input_files.get('ddl', []),
            ncpdp_general_standards=input_files.get('ncpdp_general_standards', []),
            ncpdp_script_standards=input_files.get('ncpdp_script_standards', []),
            naming_standard=input_files.get('naming_standard', []),
            entity_threshold=data.get('thresholds', {}).get('entity_threshold', 0.006),
            attribute_threshold=data.get('thresholds', {}).get('attribute_threshold', 0.004),
            metadata=data.get('metadata', {})
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