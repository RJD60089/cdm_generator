"""
CDM Configuration Package

Provides configuration management for CDM generation:
- config_utils: Shared utilities for finding/loading configs
- config_gen_core: Base classes for config generation
- config_gen_fhir: FHIR resource analysis
- config_gen_ncpdp: NCPDP standards analysis
- config_gen_glue: Glue file consolidation
- config_generator: Main coordinator
- config_parser: Config loading and validation (existing)

Usage:
    from src.config import load_config, find_latest_config
    
    # Find and load config by CDM name
    config_path = find_latest_config('plan')
    config = load_config(str(config_path))
    
    # Or use the generator
    from src.config.config_generator import ConfigGenerator
    gen = ConfigGenerator('plan', llm_client=llm)
    gen.run()
"""

# Re-export key functions from config_utils
from .config_utils import (
    find_latest_config,
    find_base_config,
    get_config_dir,
    get_cdm_dir,
    safe_cdm_name,
    load_json_file,
    save_json_file,
)

# Re-export from existing config_parser (backward compatibility)
from .config_parser import load_config, AppConfig, CDMConfig

# Re-export generators for direct use
from .config_generator import ConfigGenerator
from .config_gen_fhir import FHIRConfigGenerator
from .config_gen_ncpdp import NCPDPConfigGenerator
from .config_gen_glue import GlueConfigGenerator

__all__ = [
    # Utilities
    'find_latest_config',
    'find_base_config',
    'get_config_dir',
    'get_cdm_dir',
    'safe_cdm_name',
    'load_json_file',
    'save_json_file',
    # Parser (existing)
    'load_config',
    'AppConfig',
    'CDMConfig',
    # Generators
    'ConfigGenerator',
    'FHIRConfigGenerator',
    'NCPDPConfigGenerator',
    'GlueConfigGenerator',
]
