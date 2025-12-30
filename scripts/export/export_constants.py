"""
Parquet Export Constants and Configuration

This module defines reusable constants for the Parquet export pipeline,
including output paths, compression settings, and metadata keys.

Centralizing these values ensures consistency between the export script,
validation utilities, and documentation generators.
"""

import os
from pathlib import Path

# =============================================================================
# OUTPUT CONFIGURATION
# =============================================================================

# Directory where Parquet files will be saved (relative to project root)
PARQUET_OUTPUT_DIR = "parquet"

# Standard file extension for Parquet files
PARQUET_EXTENSION = ".parquet"

# README file name in the output directory
PARQUET_README_NAME = "README.md"


# =============================================================================
# COMPRESSION SETTINGS
# =============================================================================

# Supported compression codecs
COMPRESSION_SNAPPY = "snappy"
COMPRESSION_GZIP = "gzip"
COMPRESSION_NONE = None

# Default compression used if none specified
DEFAULT_COMPRESSION = COMPRESSION_SNAPPY


# =============================================================================
# METADATA DICTIONARY KEYS
# =============================================================================
# Used for passing export results between functions and generating READMEs

METADATA_TABLE_NAME = "table_name"
METADATA_ROWS = "rows"
METADATA_FILE_SIZE_MB = "file_size_mb"
METADATA_FILE_PATH = "file_path"
METADATA_LAST_UPDATED = "last_updated"
