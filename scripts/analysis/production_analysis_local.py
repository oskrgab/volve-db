"""
Volve Production Data Analysis

This script demonstrates how to query the SQLite database using DuckDB and
visualize production data using matplotlib and polars.

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
DATABASE_PATH = "database/volve.db"
OUTPUT_PATH = "scripts/analysis/output/production_analysis_local.png"

# Table sources for local SQLite database
DAILY_PRODUCTION_TABLE = "daily_production"
WELLS_TABLE = "wells"


def main():
    """Run production analysis using local SQLite database."""
    print("="*80)
    print("VOLVE PRODUCTION DATA ANALYSIS")
    print("="*80)

    # Connect to database via DuckDB
    print(f"\nConnecting to database: {DATABASE_PATH}")
    conn = duckdb.connect(DATABASE_PATH, read_only=True)

    # Query data
    print("\nQuerying data...")
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
        title_suffix=""
    )


if __name__ == "__main__":
    main()
