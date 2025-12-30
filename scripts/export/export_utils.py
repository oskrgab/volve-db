"""
Export Utilities and Validation Functions

This module provides helper functions for the Parquet export process,
including database connectivity, row count validation, file size calculation,
and metadata README generation.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from scripts.export.export_constants import (
    METADATA_FILE_PATH,
    METADATA_FILE_SIZE_MB,
    METADATA_LAST_UPDATED,
    METADATA_ROWS,
    METADATA_TABLE_NAME,
)


def get_table_row_count(table_name: str, db_path: str) -> int:
    """
    Get the row count of a table in the SQLite database.

    Args:
        table_name: Name of the table to query
        db_path: Path to the SQLite database file

    Returns:
        Number of rows in the table
    """
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
    return int(count) if count is not None else 0


def get_parquet_row_count(parquet_path: Union[str, Path]) -> int:
    """
    Get the row count of a Parquet file using pyarrow.

    Args:
        parquet_path: Path to the Parquet file

    Returns:
        Number of rows in the Parquet file
    """
    metadata = pq.read_metadata(parquet_path)
    return metadata.num_rows


def validate_row_count(table_name: str, db_path: str, parquet_path: Union[str, Path]) -> bool:
    """
    Verify that the row counts in SQLite and Parquet match.

    Args:
        table_name: Name of the source SQLite table
        db_path: Path to the SQLite database
        parquet_path: Path to the exported Parquet file

    Returns:
        True if counts match, False otherwise
    """
    db_count = get_table_row_count(table_name, db_path)
    parquet_count = get_parquet_row_count(parquet_path)

    if db_count == parquet_count:
        return True
    else:
        print(f"   ✗ Row count mismatch for {table_name}: SQLite ({db_count}) != Parquet ({parquet_count})")
        return False


def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """
    Get the size of a file in Megabytes.

    Args:
        file_path: Path to the file

    Returns:
        File size in MB
    """
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)

def generate_metadata_readme(export_results: List[Dict], template_path: Optional[Union[str, Path]] = None) -> str:
    """
    Generate markdown content for the Parquet export README.
    
    If a template_path is provided, it replaces the metadata table placeholder.
    Otherwise, it generates a full basic README.

    Args:
        export_results: List of dictionaries containing metadata for each exported table
        template_path: Path to an existing README.md to use as a template

    Returns:
        Markdown string content
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate the table part
    table_lines = [
        "| Table Name | Rows | File Size (MB) | Last Updated |",
        "| :--- | :--- | :--- | :--- |"
    ]

    for res in export_results:
        table = res.get(METADATA_TABLE_NAME, "Unknown")
        rows = f"{res.get(METADATA_ROWS, 0):,}"
        size = f"{res.get(METADATA_FILE_SIZE_MB, 0.0):.4f}"
        updated = res.get(METADATA_LAST_UPDATED, timestamp)
        table_lines.append(f"| {table} | {rows} | {size} | {updated} |")

    table_content = "\n".join(table_lines)

    if template_path and os.path.exists(template_path):
        with open(template_path, "r") as f:
            content = f.read()
        
        # Placeholder markers
        start_marker = "<!-- START_METADATA_TABLE -->"
        end_marker = "<!-- END_METADATA_TABLE -->"
        
        if start_marker in content and end_marker in content:
            parts = content.split(start_marker)
            rest = parts[1].split(end_marker)
            return parts[0] + start_marker + "\n" + table_content + "\n" + end_marker + rest[1]

    # Default basic README if no template or placeholder found
    markdown = [
        "# Volve Parquet Exports",
        "",
        "This directory contains Parquet exports of the Volve production database tables.",
        "",
        "## Export Metadata",
        "",
        table_content,
        "",
        "## Usage",
        "",
        "See project root README.md for usage instructions.",
    ]
    return "\n".join(markdown)
