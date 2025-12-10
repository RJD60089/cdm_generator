# src/artifacts/__init__.py
"""
Artifacts Generation Package (Step 7)

Generates deliverable artifacts from Full CDM:
- Excel CDM (10 tabs - primary deliverable)
- Word Review Document (8 sections + appendices)
- DDL generated during Word creation
- LucidChart CSV generated during Word creation

Requires Step 6 (Build Full CDM) to be completed first.
"""

from src.artifacts.run_artifacts import (
    run_artifact_generation,
    interactive_artifact_generation,
    find_full_cdm
)

# Excel generation
from src.artifacts.excel.generate_excel_cdm import generate_excel_cdm

# Word generation (includes DDL and LucidChart)
from src.artifacts.word.generate_word_cdm import generate_word_cdm

__all__ = [
    # Orchestration
    'run_artifact_generation',
    'interactive_artifact_generation',
    'find_full_cdm',
    # Excel
    'generate_excel_cdm',
    # Word
    'generate_word_cdm'
]