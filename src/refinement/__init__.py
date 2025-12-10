# src/refinement/__init__.py
"""
CDM Refinement Modules

Refinement steps that transform the Foundational CDM:
  - refine_consolidation: Entity consolidation (merge overlapping entities)
  - refine_pk_fk_validation: PK/FK validation and fixes
  - refine_naming: Naming standards compliance (future)
  - refine_cross_reference: Source traceability (future)
"""

from .refine_consolidation import run_consolidation_refinement
from .refine_pk_fk_validation import run_pk_fk_validation

__all__ = [
    'run_consolidation_refinement',
    'run_pk_fk_validation',
]