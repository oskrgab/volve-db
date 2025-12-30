"""
Volve Production Data Analysis - Remote Parquet Version

This script demonstrates querying parquet files directly from GitHub Pages
using DuckDB's HTTP reader capability, without requiring a local database.

This validates the complete data pipeline:
1. SQLite database → Parquet export
2. GitHub Actions → GitHub Pages deployment
3. DuckDB WASM → Remote parquet querying

Visualizations created:
1. Total daily field production (oil, water) over time - line plot
2. Cumulative oil production by well - horizontal bar chart
3. Overall field cumulative production (oil, water) - pie chart

All plots are arranged in a single figure with custom layout.
"""

import duckdb
from utils import (
    query_daily_field_totals,
    query_cumulative_oil_by_well,
    query_field_cumulative_totals,
    create_production_visualizations
)

# Configuration
PARQUET_BASE_URL = "https://volve-db.oscarcortez.me"
OUTPUT_PATH = "scripts/analysis/output/production_analysis_remote.png"

# Table sources for remote Parquet files
DAILY_PRODUCTION_TABLE = f"read_parquet('{PARQUET_BASE_URL}/daily_production.parquet')"
WELLS_TABLE = f"read_parquet('{PARQUET_BASE_URL}/wells.parquet')"


def main():
    """Run production analysis using remote Parquet files."""
    print("="*80)
    print("VOLVE PRODUCTION DATA ANALYSIS - REMOTE PARQUET")
    print("="*80)

    # Connect to DuckDB in-memory (no local database needed)
    print(f"\nData source: {PARQUET_BASE_URL}")
    conn = duckdb.connect(":memory:")

    # Query data
    print("\nQuerying remote parquet files...")
    print("  1. Daily field production totals")
    daily_totals = query_daily_field_totals(conn, DAILY_PRODUCTION_TABLE)
    print(f"     → {len(daily_totals)} days of production data")

    print("  2. Cumulative oil production by well")
    well_cumulatives = query_cumulative_oil_by_well(
        conn, DAILY_PRODUCTION_TABLE, WELLS_TABLE
    )
    print(f"     → {len(well_cumulatives)} wells")

    print("  3. Field cumulative totals")
    field_totals = query_field_cumulative_totals(conn, DAILY_PRODUCTION_TABLE)
    print(f"     → Oil: {field_totals['oil']:,.0f} sm3")
    print(f"     → Gas: {field_totals['gas']:,.0f} sm3")
    print(f"     → Water: {field_totals['water']:,.0f} sm3")

    conn.close()

    # Create visualizations
    create_production_visualizations(
        daily_totals=daily_totals,
        well_cumulatives=well_cumulatives,
        field_totals=field_totals,
        output_path=OUTPUT_PATH,
        title_suffix="Remote Parquet"
    )


if __name__ == "__main__":
    main()
