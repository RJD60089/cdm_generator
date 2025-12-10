# src/artifacts/word/__init__.py
"""Word DDL Document and supporting file generation."""

from src.artifacts.word.generate_word_cdm import (
    generate_word_cdm,
    generate_word_ddl,
    generate_ddl_and_csv
)
from src.artifacts.word.generate_ddl import generate_ddl, generate_ddl_file
from src.artifacts.word.generate_lucidchart_csv import (
    ddl_to_lucidchart,
    generate_lucidchart_files
)

__all__ = [
    'generate_word_cdm',
    'generate_word_ddl',
    'generate_ddl_and_csv',
    'generate_ddl',
    'generate_ddl_file',
    'ddl_to_lucidchart',
    'generate_lucidchart_files'
]
