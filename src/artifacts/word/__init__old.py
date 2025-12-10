# src/artifacts/word/__init__.py
"""Word CDM Document Generation Package."""

from src.artifacts.word.generate_word_cdm import generate_word_cdm
from src.artifacts.word.generate_ddl import generate_ddl, generate_ddl_file
from src.artifacts.word.generate_lucidchart_csv import (
    ddl_to_lucidchart,
    generate_lucidchart_files
)

__all__ = [
    'generate_word_cdm',
    'generate_ddl',
    'generate_ddl_file',
    'ddl_to_lucidchart',
    'generate_lucidchart_files'
]