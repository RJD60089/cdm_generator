"""
Step modules for CDM generation pipeline.

Step 1: Input Rationalization (1a: FHIR, 1b: Guardrails, 1c: Glue)
Step 2: CDM Generation (2a-2e: Serial refinement)
Step 3: Relationships & Constraints (future)
Step 4: DDL Generation (future)
Step 5: Excel Generation (future)
"""

# Step 1: Rationalization
from .step1a_fhir import run_step1a
from .step1b_guardrails import run_step1b
from .step1c_glue import run_step1c

# Step 2: CDM Generation (implemented as stubs, being built out)
# from .step2a_fhir_foundation import run_step2a
# from .step2b_ncpdp_refinement import run_step2b
# from .step2c_guardrails_refinement import run_step2c
# from .step2d_ddl_refinement import run_step2d
# from .step2e_final_refinement import run_step2e

# Step 3-5: Future
# from .step3_relationships import run_step3
# from .step4_ddl_generation import run_step4
# from .step5_excel_generation import run_step5

__all__ = [
    # Step 1
    'run_step1a',
    'run_step1b',
    'run_step1c',
    # Step 2
    'run_step2a',
    'run_step2b',
    'run_step2c',
    'run_step2d',
    'run_step2e',
]