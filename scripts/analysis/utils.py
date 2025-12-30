"""
Shared utilities for Volve production data analysis.

This module contains reusable functions for querying production data
and creating visualizations, supporting both local SQLite and remote
Parquet data sources.
"""

from pathlib import Path
import duckdb
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def query_daily_field_totals(
    conn: duckdb.DuckDBPyConnection,
    daily_production_table: str
) -> pl.DataFrame:
    """
    Query total daily production volumes across all wells.

    This aggregates production from all wells to get field-level daily totals.

    Args:
        conn: DuckDB connection
        daily_production_table: Table source (e.g., "daily_production" or
                               "read_parquet('url/daily_production.parquet')")

    Returns:
        Polars DataFrame with columns: date, total_oil, total_gas, total_water
    """
    query = f"""
        SELECT
            date,
            SUM(oil_volume) as total_oil,
            SUM(gas_volume) as total_gas,
            SUM(water_volume) as total_water
        FROM {daily_production_table}
        WHERE oil_volume IS NOT NULL
          AND gas_volume IS NOT NULL
          AND water_volume IS NOT NULL
        GROUP BY date
        ORDER BY date
    """

    result = conn.execute(query).pl()
    return result


def query_cumulative_oil_by_well(
    conn: duckdb.DuckDBPyConnection,
    daily_production_table: str,
    wells_table: str
) -> pl.DataFrame:
    """
    Query cumulative oil production by well, sorted from highest to lowest.

    Joins wells and daily_production tables to get well names.

    Args:
        conn: DuckDB connection
        daily_production_table: Daily production table source
        wells_table: Wells table source

    Returns:
        Polars DataFrame with columns: wellbore_name, cumulative_oil
    """
    query = f"""
        SELECT
            w.wellbore_name,
            SUM(dp.oil_volume) as cumulative_oil
        FROM {daily_production_table} dp
        INNER JOIN {wells_table} w
            ON dp.npd_wellbore_code = w.npd_wellbore_code
        WHERE dp.oil_volume IS NOT NULL
        GROUP BY w.wellbore_name
        ORDER BY cumulative_oil DESC
    """

    result = conn.execute(query).pl()
    return result


def query_field_cumulative_totals(
    conn: duckdb.DuckDBPyConnection,
    daily_production_table: str
) -> dict:
    """
    Query total cumulative production volumes for the entire field.

    Args:
        conn: DuckDB connection
        daily_production_table: Daily production table source

    Returns:
        Dictionary with keys: oil, gas, water
    """
    query = f"""
        SELECT
            SUM(oil_volume) as total_oil,
            SUM(gas_volume) as total_gas,
            SUM(water_volume) as total_water
        FROM {daily_production_table}
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


def create_production_visualizations(
    daily_totals: pl.DataFrame,
    well_cumulatives: pl.DataFrame,
    field_totals: dict,
    output_path: str,
    title_suffix: str = ""
):
    """
    Create comprehensive production visualizations.

    Layout:
    - Top: Line plot of daily field production (full width)
    - Bottom left: Pie chart of field cumulative production
    - Bottom right: Horizontal bar chart of well oil cumulatives

    Args:
        daily_totals: DataFrame with date, total_oil, total_gas, total_water
        well_cumulatives: DataFrame with wellbore_name, cumulative_oil
        field_totals: Dictionary with oil, gas, water totals
        output_path: Path where to save the figure
        title_suffix: Optional suffix to add to plot titles (e.g., "Remote Parquet")
    """
    print("\nCreating visualizations...")
    fig = plt.figure(figsize=(16, 10))

    # Define grid layout: 2 rows, 2 columns
    gs = fig.add_gridspec(2, 2, height_ratios=[2, 1], hspace=0.3, wspace=0.3)

    # =========================================================================
    # PLOT 1: Daily field production over time (line plot)
    # =========================================================================
    ax1 = fig.add_subplot(gs[0, :])  # Spans both columns

    dates = daily_totals['date'].to_list()

    ax1.plot(dates, daily_totals['total_oil'],
             label='Oil', color='green', linewidth=1.5, alpha=0.8)
    ax1.plot(dates, daily_totals['total_water'],
             label='Water', color='blue', linewidth=1.5, alpha=0.8)

    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('Daily Production (sm3)', fontsize=12)

    title = 'Volve Field - Daily Production Over Time'
    if title_suffix:
        title += f' ({title_suffix})'
    ax1.set_title(title, fontsize=14, fontweight='bold', pad=20)

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

    labels = ['Oil', 'Water']
    values = [field_totals['oil'], field_totals['water']]
    colors = ['green', 'blue']

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

    y_positions = range(len(top_wells))
    ax3.barh(y_positions, top_wells['cumulative_oil'],
             color='green', alpha=0.7, edgecolor='black', linewidth=0.5)

    ax3.set_yticks(y_positions)
    ax3.set_yticklabels(top_wells['wellbore_name'], fontsize=9)

    ax3.set_xlabel('Cumulative Oil Production (sm3)', fontsize=11)
    ax3.set_title('Top 15 Wells by Oil Production',
                  fontsize=12, fontweight='bold', pad=15)
    ax3.grid(True, axis='x', alpha=0.3, linestyle='--')

    ax3.invert_yaxis()

    # Add value labels on bars
    for i, (well_name, cum_oil) in enumerate(
        zip(top_wells['wellbore_name'], top_wells['cumulative_oil'])
    ):
        ax3.text(cum_oil, i, f' {cum_oil:,.0f}',
                va='center', ha='left', fontsize=8)

    # Overall figure title
    main_title = 'Volve Field Production Analysis'
    if title_suffix:
        main_title += f' ({title_suffix})'
    fig.suptitle(main_title, fontsize=16, fontweight='bold', y=0.98)

    # Save figure
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Figure saved to: {output_path}")

    # Display
    plt.tight_layout()
    plt.show()
