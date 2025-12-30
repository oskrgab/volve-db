"""
Volve Parquet Export Module

This module provides functionality to export SQLite database tables to Parquet
format, optimized for high-performance analytics and client-side consumption.

Public API:
- export_all_tables: Main entry point for full database export
- export_table_to_parquet: Export individual tables
- validate_row_count: Integrity check between SQLite and Parquet
- get_file_size_mb: Utility to check output file sizes

Example usage:
    from scripts.export import export_all_tables
    results = export_all_tables()
"""

from .export_constants import DEFAULT_COMPRESSION, PARQUET_OUTPUT_DIR
from .export_utils import get_file_size_mb, validate_row_count
from .parquet_export import export_all_tables, export_table_to_parquet

__all__ = [
    "export_all_tables",
    "export_table_to_parquet",
    "validate_row_count",
    "get_file_size_mb",
    "PARQUET_OUTPUT_DIR",
    "DEFAULT_COMPRESSION",
]
