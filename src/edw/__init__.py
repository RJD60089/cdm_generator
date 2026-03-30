"""
EDW (Enterprise Data Warehouse) mapping preparation module.

Parses source-to-target XLS mapping files from the EDW ETL pipeline
into structured JSON catalogs for use in CDM generation.

Two-stage EDW pipeline:
    NI_ (Initial)   : Source OLTP (PBMCN01/SQLMGR) -> NI schema (change-detection staging)
    NP_ (Persistent): NI schema -> NP schema (SCD Type 2 full history)

Entry point:
    from src.edw import run_edw_mapping_prep
"""

from .edw_mapping_prep import run_edw_mapping_prep, EdwMappingPrep

__all__ = ["run_edw_mapping_prep", "EdwMappingPrep"]