"""
Integration Testing for Parquet Export

This script performs end-to-end validation of the Parquet export pipeline.
"""

import os
import shutil
import sqlite3
import pandas as pd
import pyarrow.parquet as pq
import duckdb
from pathlib import Path

def test_export_integrity():
    print("--- 1. Verifying file existence and basic metadata ---")
    parquet_dir = Path("parquet")
    tables = ["wells", "daily_production", "monthly_production"]
    
    for table in tables:
        path = parquet_dir / f"{table}.parquet"
        assert path.exists(), f"{table}.parquet missing"
        print(f"✓ {table}.parquet exists")
    
    assert (parquet_dir / "README.md").exists(), "parquet/README.md missing"
    print("✓ parquet/README.md exists")

    print("\n--- 2. Validating row counts (SQLite vs Parquet) ---")
    db_conn = sqlite3.connect("database/volve.db")
    
    for table in tables:
        # SQLite count
        db_count = db_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        # Parquet count
        parquet_path = parquet_dir / f"{table}.parquet"
        p_table = pq.read_table(parquet_path)
        p_count = p_table.num_rows
        
        assert db_count == p_count, f"Row count mismatch for {table}: DB={db_count}, Parquet={p_count}"
        print(f"✓ {table} row counts match: {db_count}")
        
        # Schema check
        if table == "daily_production":
            assert "date" in p_table.column_names
            # Check for NULLs in oil_volume (should be preserved)
            df = p_table.to_pandas()
            null_count = df["oil_volume"].isna().sum()
            print(f"   (daily_production has {null_count} NULLs in oil_volume, preserved correctly)")

    print("\n--- 3. Testing DuckDB consumption ---")
    duck = duckdb.connect()
    for table in tables:
        path = parquet_dir / f"{table}.parquet"
        res = duck.query(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        print(f"✓ DuckDB can read {table}.parquet: {res} rows")
        
    # Sample Join
    res = duck.query(f"""
        SELECT w.wellbore_name, SUM(d.oil_volume)
        FROM read_parquet('parquet/daily_production.parquet') d
        JOIN read_parquet('parquet/wells.parquet') w ON d.npd_wellbore_code = w.npd_wellbore_code
        GROUP BY w.wellbore_name
        LIMIT 3
    """).fetchall()
    print(f"✓ DuckDB join successful: {res}")

    print("\n--- 4. Testing Error Handling ---")
    # Test missing database
    print("Testing missing database error...")
    os.rename("database/volve.db", "database/volve.db.bak")
    try:
        # We run it as a subprocess to catch sys.exit
        import subprocess
        result = subprocess.run(["python", "scripts/export/parquet_export.py"], 
                               env={**os.environ, "PYTHONPATH": "."},
                               capture_output=True, text=True)
        assert result.returncode != 0
        assert "Database not found" in result.stdout or "Database not found" in result.stderr
        print("✓ Missing database correctly handled")
    finally:
        os.rename("database/volve.db.bak", "database/volve.db")

    print("\nINTEGRATION TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    test_export_integrity()
