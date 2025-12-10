# src/cdm_full/__init__.py
"""
Full CDM Build and Post-Processing Package

Build Full CDM:
- run_build_full_cdm() - Main build function

Post-Processing:
- interactive_postprocessing() - Orchestrated post-process with prompts
- run_postprocessing() - Direct post-process execution

Post-Process Steps:
1. Sensitivity Analysis (PHI/PII flagging) - AI-driven
2. CDE Identification - AI-driven
"""

from src.cdm_full.run_postprocess import (
    run_postprocessing,
    interactive_postprocessing
)
from src.cdm_full.postprocess_sensitivity import run_sensitivity_postprocess
from src.cdm_full.postprocess_cde import run_cde_postprocess

__all__ = [
    'run_postprocessing',
    'interactive_postprocessing',
    'run_sensitivity_postprocess',
    'run_cde_postprocess'
]
