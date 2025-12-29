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

from pathlib import Path
import duckdb
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Database path
DATABASE_PATH = "database/volve.db"


def query_daily_field_totals(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Query total daily production volumes across all wells.

    This aggregates production from all wells to get field-level daily totals.

    Args:
        conn: DuckDB connection to the SQLite database

    Returns:
        Polars DataFrame with columns: date, total_oil, total_gas, total_water
    """
    query = """
        SELECT
            date,
            SUM(oil_volume) as total_oil,
            SUM(gas_volume) as total_gas,
            SUM(water_volume) as total_water
        FROM daily_production
        WHERE oil_volume IS NOT NULL
          AND gas_volume IS NOT NULL
          AND water_volume IS NOT NULL
        GROUP BY date
        ORDER BY date
    """

    # Execute query and convert to polars DataFrame
    result = conn.execute(query).pl()

    return result


def query_cumulative_oil_by_well(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Query cumulative oil production by well, sorted from highest to lowest.

    Joins with wells table to get well names for better readability.

    Args:
        conn: DuckDB connection to the SQLite database

    Returns:
        Polars DataFrame with columns: wellbore_name, cumulative_oil
    """
    query = """
        SELECT
            w.wellbore_name,
            SUM(dp.oil_volume) as cumulative_oil
        FROM daily_production dp
        INNER JOIN wells w ON dp.npd_wellbore_code = w.npd_wellbore_code
        WHERE dp.oil_volume IS NOT NULL
        GROUP BY w.wellbore_name
        ORDER BY cumulative_oil DESC
    """

    result = conn.execute(query).pl()

    return result


def query_field_cumulative_totals(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Query total cumulative production volumes for the entire field.

    Args:
        conn: DuckDB connection to the SQLite database

    Returns:
        Dictionary with keys: oil, gas, water
    """
    query = """
        SELECT
            SUM(oil_volume) as total_oil,
            SUM(gas_volume) as total_gas,
            SUM(water_volume) as total_water
        FROM daily_production
        WHERE oil_volume IS NOT NULL
          AND gas_volume IS NOT NULL
          AND water_volume IS NOT NULL
    """

    result = conn.execute(query).fetchone()

    return {
        'oil': result[0],
        'gas': result[1],
        'water': result[2]
    }


def create_production_visualizations():
    """
    Create comprehensive production visualizations.

    Layout:
    - Top: Line plot of daily field production (full width)
    - Bottom left: Pie chart of field cumulative production
    - Bottom right: Horizontal bar chart of well oil cumulatives
    """
    print("="*80)
    print("VOLVE PRODUCTION DATA ANALYSIS")
    print("="*80)

    # Connect to database via DuckDB
    print(f"\nConnecting to database: {DATABASE_PATH}")
    conn = duckdb.connect(DATABASE_PATH, read_only=True)

    # Query data
    print("\nQuerying data...")
    print("  1. Daily field production totals")
    daily_totals = query_daily_field_totals(conn)
    print(f"     → {len(daily_totals)} days of production data")

    print("  2. Cumulative oil production by well")
    well_cumulatives = query_cumulative_oil_by_well(conn)
    print(f"     → {len(well_cumulatives)} wells")

    print("  3. Field cumulative totals")
    field_totals = query_field_cumulative_totals(conn)
    print(f"     → Oil: {field_totals['oil']:,.0f} sm3")
    print(f"     → Gas: {field_totals['gas']:,.0f} sm3")
    print(f"     → Water: {field_totals['water']:,.0f} sm3")

    conn.close()

    # Create figure with custom layout
    print("\nCreating visualizations...")
    fig = plt.figure(figsize=(16, 10))

    # Define grid layout: 2 rows, 2 columns
    # Top plot spans both columns (row 0, col 0-1)
    # Bottom left: pie chart (row 1, col 0)
    # Bottom right: bar chart (row 1, col 1)
    gs = fig.add_gridspec(2, 2, height_ratios=[2, 1], hspace=0.3, wspace=0.3)

    # =========================================================================
    # PLOT 1: Daily field production over time (line plot)
    # =========================================================================
    ax1 = fig.add_subplot(gs[0, :])  # Spans both columns

    # Convert date column to datetime for better plotting
    dates = daily_totals['date'].to_list()

    # Plot each production type
    ax1.plot(dates, daily_totals['total_oil'],
             label='Oil', color='green', linewidth=1.5, alpha=0.8)
    ax1.plot(dates, daily_totals['total_water'],
             label='Water', color='blue', linewidth=1.5, alpha=0.8)

    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('Daily Production (sm3)', fontsize=12)
    ax1.set_title('Volve Field - Daily Production Over Time',
                  fontsize=14, fontweight='bold', pad=20)
    ax1.legend(loc='upper right', fontsize=11)
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.set_ylim(bottom=0)

    # Format x-axis dates
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=0, ha='center')

    # =========================================================================
    # PLOT 2: Field cumulative production breakdown (pie chart)
    # =========================================================================
    ax2 = fig.add_subplot(gs[1, 0])

    # Prepare data for pie chart
    labels = ['Oil', 'Water']
    values = [field_totals['oil'], field_totals['water']]
    colors = ['green', 'blue']

    # Create pie chart with percentage labels
    wedges, texts, autotexts = ax2.pie(
        values,
        labels=labels,
        colors=colors,
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 11}
    )

    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(10)

    ax2.set_title('Field Cumulative Production\n(Oil & Water, sm3)',
                  fontsize=12, fontweight='bold', pad=15)

    # =========================================================================
    # PLOT 3: Cumulative oil by well (horizontal bar chart)
    # =========================================================================
    ax3 = fig.add_subplot(gs[1, 1])

    # Take top 15 wells for readability
    top_wells = well_cumulatives.head(15)

    # Create horizontal bar chart
    y_positions = range(len(top_wells))
    ax3.barh(y_positions, top_wells['cumulative_oil'],
             color='green', alpha=0.7, edgecolor='black', linewidth=0.5)

    # Set well names as y-tick labels
    ax3.set_yticks(y_positions)
    ax3.set_yticklabels(top_wells['wellbore_name'], fontsize=9)

    ax3.set_xlabel('Cumulative Oil Production (sm3)', fontsize=11)
    ax3.set_title('Top 15 Wells by Oil Production',
                  fontsize=12, fontweight='bold', pad=15)
    ax3.grid(True, axis='x', alpha=0.3, linestyle='--')

    # Invert y-axis so highest producer is at top
    ax3.invert_yaxis()

    # Add value labels on bars
    for i, (well_name, cum_oil) in enumerate(
        zip(top_wells['wellbore_name'], top_wells['cumulative_oil'])
    ):
        ax3.text(cum_oil, i, f' {cum_oil:,.0f}',
                va='center', ha='left', fontsize=8)

    # Overall figure title
    fig.suptitle('Volve Field Production Analysis',
                 fontsize=16, fontweight='bold', y=0.98)

    # Save figure
    output_path = "scripts/analysis/output/production_analysis.png"
    Path("scripts/analysis/output").mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')


    # Display
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    create_production_visualizations()
