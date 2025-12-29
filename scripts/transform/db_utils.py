"""
Database Utilities

Shared database connection and utility functions for the Volve database project.
"""

from pathlib import Path
from sqlalchemy import create_engine, MetaData


def create_database_engine(db_path: str, echo: bool = False):
    """
    Create SQLite database engine and metadata object.

    Args:
        db_path: Path to the SQLite database file
        echo: If True, print SQL statements (default: False)

    Returns:
        Tuple of (engine, metadata) objects
    """
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{db_path}", echo=echo)
    metadata = MetaData()

    return engine, metadata


def create_database_connection(db_path: str, echo: bool = False):
    """
    Create database engine without metadata.

    Args:
        db_path: Path to SQLite database file
        echo: If True, print SQL statements (default: False)

    Returns:
        SQLAlchemy engine object
    """
    engine = create_engine(f"sqlite:///{db_path}", echo=echo)
    return engine
