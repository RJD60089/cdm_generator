# src/cdm_builder/__init__.py
"""
CDM Builder Package

Step 3 module for building foundational CDM:
- Step 3a: Build Foundational CDM (AI-generated conceptual model)

Note: DDL and LucidChart generation moved to src/artifacts package (Step 7)
"""

from src.cdm_builder.build_foundational_cdm import run_step3a

__all__ = [
    'run_step3a'
]