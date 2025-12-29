"""
Load Volve Production Data into SQLite Database

This script orchestrates the complete ETL pipeline to load production data
from Excel into the SQLite database using pandas and SQLAlchemy Core.

Pipeline:
1. Extract unique wells from source data → load wells table
2. Transform and load daily production data → load daily_production table
3. Transform and load monthly production data → load monthly_production table
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

from constants import (
    # Database config
    DATABASE_PATH,
    SOURCE_EXCEL_PATH,
    SHEET_DAILY,
    SHEET_MONTHLY,
    # Table names
    TABLE_WELLS,
    TABLE_DAILY_PRODUCTION,
    TABLE_MONTHLY_PRODUCTION,
    # Wells table columns
    WELLS_NPD_WELLBORE_CODE,
    WELLS_WELLBORE_CODE,
    WELLS_WELLBORE_NAME,
    WELLS_NPD_FIELD_CODE,
    WELLS_NPD_FIELD_NAME,
    WELLS_NPD_FACILITY_CODE,
    WELLS_NPD_FACILITY_NAME,
    # Daily production columns
    DAILY_DATE,
    DAILY_NPD_WELLBORE_CODE,
    DAILY_ON_STREAM_HRS,
    DAILY_AVG_DOWNHOLE_PRESSURE,
    DAILY_AVG_DP_TUBING,
    DAILY_AVG_ANNULUS_PRESS,
    DAILY_AVG_WHP_P,
    DAILY_AVG_DOWNHOLE_TEMPERATURE,
    DAILY_AVG_WHT_P,
    DAILY_AVG_CHOKE_SIZE_P,
    DAILY_AVG_CHOKE_UOM,
    DAILY_DP_CHOKE_SIZE,
    DAILY_BORE_OIL_VOL,
    DAILY_BORE_GAS_VOL,
    DAILY_BORE_WAT_VOL,
    DAILY_BORE_WI_VOL,
    DAILY_FLOW_KIND,
    DAILY_WELL_TYPE,
    # Monthly production columns
    MONTHLY_DATE,
    MONTHLY_NPD_WELLBORE_CODE,
    MONTHLY_ON_STREAM,
    MONTHLY_OIL_VOL,
    MONTHLY_GAS_VOL,
    MONTHLY_WATER_VOL,
    MONTHLY_GAS_INJECTION,
    MONTHLY_WATER_INJECTION,
    # Source column names
    SOURCE_DAILY_DATEPRD,
    SOURCE_DAILY_WELL_BORE_CODE,
    SOURCE_DAILY_NPD_WELL_BORE_CODE,
    SOURCE_DAILY_NPD_WELL_BORE_NAME,
    SOURCE_DAILY_NPD_FIELD_CODE,
    SOURCE_DAILY_NPD_FIELD_NAME,
    SOURCE_DAILY_NPD_FACILITY_CODE,
    SOURCE_DAILY_NPD_FACILITY_NAME,
    SOURCE_DAILY_ON_STREAM_HRS,
    SOURCE_DAILY_AVG_DOWNHOLE_PRESSURE,
    SOURCE_DAILY_AVG_DOWNHOLE_TEMPERATURE,
    SOURCE_DAILY_AVG_DP_TUBING,
    SOURCE_DAILY_AVG_ANNULUS_PRESS,
    SOURCE_DAILY_AVG_CHOKE_SIZE_P,
    SOURCE_DAILY_AVG_CHOKE_UOM,
    SOURCE_DAILY_AVG_WHP_P,
    SOURCE_DAILY_AVG_WHT_P,
    SOURCE_DAILY_DP_CHOKE_SIZE,
    SOURCE_DAILY_BORE_OIL_VOL,
    SOURCE_DAILY_BORE_GAS_VOL,
    SOURCE_DAILY_BORE_WAT_VOL,
    SOURCE_DAILY_BORE_WI_VOL,
    SOURCE_DAILY_FLOW_KIND,
    SOURCE_DAILY_WELL_TYPE,
    SOURCE_MONTHLY_WELLBORE_NAME,
    SOURCE_MONTHLY_NPD_CODE,
    SOURCE_MONTHLY_YEAR,
    SOURCE_MONTHLY_MONTH,
    SOURCE_MONTHLY_ON_STREAM,
    SOURCE_MONTHLY_OIL,
    SOURCE_MONTHLY_GAS,
    SOURCE_MONTHLY_WATER,
    SOURCE_MONTHLY_GI,
    SOURCE_MONTHLY_WI,
)


def create_database_connection(db_path: str = DATABASE_PATH):
    """
    Create database engine and connection.

    Args:
        db_path: Path to SQLite database file

    Returns:
        SQLAlchemy engine object
    """
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    return engine


def load_wells_table(daily_df: pd.DataFrame, engine) -> int:
    """
    Extract unique wells from daily production data and load into wells table.

    Args:
        daily_df: DataFrame containing daily production data
        engine: SQLAlchemy engine

    Returns:
        Number of wells loaded
    """
    print("\n" + "="*80)
    print("STEP 1: LOADING WELLS TABLE")
    print("="*80)

    # Extract unique wells with their attributes
    wells_df = daily_df[[
        SOURCE_DAILY_NPD_WELL_BORE_CODE,
        SOURCE_DAILY_WELL_BORE_CODE,
        SOURCE_DAILY_NPD_WELL_BORE_NAME,
        SOURCE_DAILY_NPD_FIELD_CODE,
        SOURCE_DAILY_NPD_FIELD_NAME,
        SOURCE_DAILY_NPD_FACILITY_CODE,
        SOURCE_DAILY_NPD_FACILITY_NAME,
    ]].drop_duplicates()

    # Rename columns to match database schema
    wells_df = wells_df.rename(columns={
        SOURCE_DAILY_NPD_WELL_BORE_CODE: WELLS_NPD_WELLBORE_CODE,
        SOURCE_DAILY_WELL_BORE_CODE: WELLS_WELLBORE_CODE,
        SOURCE_DAILY_NPD_WELL_BORE_NAME: WELLS_WELLBORE_NAME,
        SOURCE_DAILY_NPD_FIELD_CODE: WELLS_NPD_FIELD_CODE,
        SOURCE_DAILY_NPD_FIELD_NAME: WELLS_NPD_FIELD_NAME,
        SOURCE_DAILY_NPD_FACILITY_CODE: WELLS_NPD_FACILITY_CODE,
        SOURCE_DAILY_NPD_FACILITY_NAME: WELLS_NPD_FACILITY_NAME,
    })

    print(f"Found {len(wells_df)} unique wells")
    print("\nSample wells:")
    print(wells_df[[WELLS_NPD_WELLBORE_CODE, WELLS_WELLBORE_NAME]].head())

    # Load to database
    wells_df.to_sql(
        TABLE_WELLS,
        engine,
        if_exists='append',
        index=False,
    )

    print(f"\n✓ Loaded {len(wells_df)} wells into {TABLE_WELLS} table")

    return len(wells_df)


def load_daily_production_table(daily_df: pd.DataFrame, engine) -> int:
    """
    Transform and load daily production data into database.

    Args:
        daily_df: DataFrame containing daily production data
        engine: SQLAlchemy engine

    Returns:
        Number of records loaded
    """
    print("\n" + "="*80)
    print("STEP 2: LOADING DAILY PRODUCTION TABLE")
    print("="*80)

    # Select and rename columns to match database schema
    column_mapping = {
        SOURCE_DAILY_DATEPRD: DAILY_DATE,
        SOURCE_DAILY_NPD_WELL_BORE_CODE: DAILY_NPD_WELLBORE_CODE,
        SOURCE_DAILY_ON_STREAM_HRS: DAILY_ON_STREAM_HRS,
        SOURCE_DAILY_AVG_DOWNHOLE_PRESSURE: DAILY_AVG_DOWNHOLE_PRESSURE,
        SOURCE_DAILY_AVG_DP_TUBING: DAILY_AVG_DP_TUBING,
        SOURCE_DAILY_AVG_ANNULUS_PRESS: DAILY_AVG_ANNULUS_PRESS,
        SOURCE_DAILY_AVG_WHP_P: DAILY_AVG_WHP_P,
        SOURCE_DAILY_AVG_DOWNHOLE_TEMPERATURE: DAILY_AVG_DOWNHOLE_TEMPERATURE,
        SOURCE_DAILY_AVG_WHT_P: DAILY_AVG_WHT_P,
        SOURCE_DAILY_AVG_CHOKE_SIZE_P: DAILY_AVG_CHOKE_SIZE_P,
        SOURCE_DAILY_AVG_CHOKE_UOM: DAILY_AVG_CHOKE_UOM,
        SOURCE_DAILY_DP_CHOKE_SIZE: DAILY_DP_CHOKE_SIZE,
        SOURCE_DAILY_BORE_OIL_VOL: DAILY_BORE_OIL_VOL,
        SOURCE_DAILY_BORE_GAS_VOL: DAILY_BORE_GAS_VOL,
        SOURCE_DAILY_BORE_WAT_VOL: DAILY_BORE_WAT_VOL,
        SOURCE_DAILY_BORE_WI_VOL: DAILY_BORE_WI_VOL,
        SOURCE_DAILY_FLOW_KIND: DAILY_FLOW_KIND,
        SOURCE_DAILY_WELL_TYPE: DAILY_WELL_TYPE,
    }

    # Create transformed dataframe
    production_df = daily_df[list(column_mapping.keys())].copy()
    production_df = production_df.rename(columns=column_mapping)

    # Ensure date is in proper format
    production_df[DAILY_DATE] = pd.to_datetime(production_df[DAILY_DATE])

    print(f"Transforming {len(production_df):,} daily production records")
    print(f"Date range: {production_df[DAILY_DATE].min()} to {production_df[DAILY_DATE].max()}")

    # Data quality check
    null_counts = production_df.isnull().sum()
    significant_nulls = null_counts[null_counts > 0].sort_values(ascending=False).head(5)
    print("\nTop 5 columns with missing values:")
    for col, count in significant_nulls.items():
        pct = (count / len(production_df)) * 100
        print(f"  {col}: {count:,} ({pct:.1f}%)")

    # Load to database
    production_df.to_sql(
        TABLE_DAILY_PRODUCTION,
        engine,
        if_exists='append',
        index=False,
    )

    print(f"\n✓ Loaded {len(production_df):,} records into {TABLE_DAILY_PRODUCTION} table")

    return len(production_df)


def load_monthly_production_table(monthly_df: pd.DataFrame, engine) -> int:
    """
    Transform and load monthly production data into database.

    This requires significant transformation:
    1. Remove header row (row 0 contains units, not data)
    2. Convert Year + Month → Date (first day of month)
    3. Convert text columns to numeric

    Args:
        monthly_df: DataFrame containing monthly production data
        engine: SQLAlchemy engine

    Returns:
        Number of records loaded
    """
    print("\n" + "="*80)
    print("STEP 3: LOADING MONTHLY PRODUCTION TABLE")
    print("="*80)

    # Remove header row (row 0 contains "Sm3" units)
    print("Removing header row with units...")
    monthly_df = monthly_df[1:].copy()

    # Convert numeric columns from text to float
    numeric_columns = [
        SOURCE_MONTHLY_NPD_CODE,
        SOURCE_MONTHLY_YEAR,
        SOURCE_MONTHLY_MONTH,
        SOURCE_MONTHLY_OIL,
        SOURCE_MONTHLY_GAS,
        SOURCE_MONTHLY_WATER,
        SOURCE_MONTHLY_GI,
        SOURCE_MONTHLY_WI,
    ]

    print("Converting text columns to numeric...")
    for col in numeric_columns:
        if col in monthly_df.columns:
            monthly_df[col] = pd.to_numeric(monthly_df[col], errors='coerce')

    # Handle on_stream column (may need conversion depending on format)
    if SOURCE_MONTHLY_ON_STREAM in monthly_df.columns:
        monthly_df[SOURCE_MONTHLY_ON_STREAM] = pd.to_numeric(
            monthly_df[SOURCE_MONTHLY_ON_STREAM],
            errors='coerce'
        )

    # Create date column from Year + Month (first day of month)
    print("Creating date column from Year + Month...")
    monthly_df[MONTHLY_DATE] = pd.to_datetime(
        monthly_df[[SOURCE_MONTHLY_YEAR, SOURCE_MONTHLY_MONTH]].rename(
            columns={SOURCE_MONTHLY_YEAR: 'year', SOURCE_MONTHLY_MONTH: 'month'}
        ).assign(day=1)
    )

    # Select and rename columns
    column_mapping = {
        MONTHLY_DATE: MONTHLY_DATE,  # Already created
        SOURCE_MONTHLY_NPD_CODE: MONTHLY_NPD_WELLBORE_CODE,
        SOURCE_MONTHLY_ON_STREAM: MONTHLY_ON_STREAM,
        SOURCE_MONTHLY_OIL: MONTHLY_OIL_VOL,
        SOURCE_MONTHLY_GAS: MONTHLY_GAS_VOL,
        SOURCE_MONTHLY_WATER: MONTHLY_WATER_VOL,
        SOURCE_MONTHLY_GI: MONTHLY_GAS_INJECTION,
        SOURCE_MONTHLY_WI: MONTHLY_WATER_INJECTION,
    }

    # Create transformed dataframe
    production_df = monthly_df[[
        MONTHLY_DATE,
        SOURCE_MONTHLY_NPD_CODE,
        SOURCE_MONTHLY_ON_STREAM,
        SOURCE_MONTHLY_OIL,
        SOURCE_MONTHLY_GAS,
        SOURCE_MONTHLY_WATER,
        SOURCE_MONTHLY_GI,
        SOURCE_MONTHLY_WI,
    ]].copy()

    production_df = production_df.rename(columns=column_mapping)

    # Remove any rows where npd_wellbore_code is null (these are invalid)
    before_count = len(production_df)
    production_df = production_df[production_df[MONTHLY_NPD_WELLBORE_CODE].notna()]
    removed_count = before_count - len(production_df)
    if removed_count > 0:
        print(f"Removed {removed_count} invalid records (null wellbore code)")

    print(f"\nTransforming {len(production_df):,} monthly production records")
    print(f"Date range: {production_df[MONTHLY_DATE].min()} to {production_df[MONTHLY_DATE].max()}")

    # Data quality check
    null_counts = production_df.isnull().sum()
    significant_nulls = null_counts[null_counts > 0].sort_values(ascending=False)
    print("\nColumns with missing values:")
    for col, count in significant_nulls.items():
        pct = (count / len(production_df)) * 100
        print(f"  {col}: {count:,} ({pct:.1f}%)")

    # Load to database
    production_df.to_sql(
        TABLE_MONTHLY_PRODUCTION,
        engine,
        if_exists='append',
        index=False,
    )

    print(f"\n✓ Loaded {len(production_df):,} records into {TABLE_MONTHLY_PRODUCTION} table")

    return len(production_df)


def validate_data_integrity(engine) -> bool:
    """
    Validate referential integrity and data quality after loading.

    Args:
        engine: SQLAlchemy engine

    Returns:
        True if all validations pass
    """
    print("\n" + "="*80)
    print("DATA VALIDATION")
    print("="*80)

    all_passed = True

    with engine.connect() as conn:
        # Check 1: Verify all tables have data
        print("\n1. Checking table row counts...")
        for table in [TABLE_WELLS, TABLE_DAILY_PRODUCTION, TABLE_MONTHLY_PRODUCTION]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"   {table}: {count:,} rows")
            if count == 0:
                print(f"   ✗ ERROR: {table} is empty!")
                all_passed = False

        # Check 2: Verify foreign key integrity for daily_production
        print("\n2. Checking foreign key integrity (daily_production)...")
        result = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {TABLE_DAILY_PRODUCTION} d
            LEFT JOIN {TABLE_WELLS} w ON d.{DAILY_NPD_WELLBORE_CODE} = w.{WELLS_NPD_WELLBORE_CODE}
            WHERE w.{WELLS_NPD_WELLBORE_CODE} IS NULL
        """))
        orphan_count = result.scalar()
        if orphan_count > 0:
            print(f"   ✗ ERROR: {orphan_count} daily records reference non-existent wells!")
            all_passed = False
        else:
            print(f"   ✓ All daily production records have valid well references")

        # Check 3: Verify foreign key integrity for monthly_production
        print("\n3. Checking foreign key integrity (monthly_production)...")
        result = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {TABLE_MONTHLY_PRODUCTION} m
            LEFT JOIN {TABLE_WELLS} w ON m.{MONTHLY_NPD_WELLBORE_CODE} = w.{WELLS_NPD_WELLBORE_CODE}
            WHERE w.{WELLS_NPD_WELLBORE_CODE} IS NULL
        """))
        orphan_count = result.scalar()
        if orphan_count > 0:
            print(f"   ✗ ERROR: {orphan_count} monthly records reference non-existent wells!")
            all_passed = False
        else:
            print(f"   ✓ All monthly production records have valid well references")

        # Check 4: Verify date ranges
        print("\n4. Checking date ranges...")
        result = conn.execute(text(f"""
            SELECT MIN({DAILY_DATE}), MAX({DAILY_DATE})
            FROM {TABLE_DAILY_PRODUCTION}
        """))
        daily_min, daily_max = result.fetchone()
        print(f"   Daily production: {daily_min} to {daily_max}")

        result = conn.execute(text(f"""
            SELECT MIN({MONTHLY_DATE}), MAX({MONTHLY_DATE})
            FROM {TABLE_MONTHLY_PRODUCTION}
        """))
        monthly_min, monthly_max = result.fetchone()
        print(f"   Monthly production: {monthly_min} to {monthly_max}")

        # Check 5: Verify no duplicate primary keys
        print("\n5. Checking for duplicate primary keys...")
        result = conn.execute(text(f"""
            SELECT {DAILY_DATE}, {DAILY_NPD_WELLBORE_CODE}, COUNT(*) as cnt
            FROM {TABLE_DAILY_PRODUCTION}
            GROUP BY {DAILY_DATE}, {DAILY_NPD_WELLBORE_CODE}
            HAVING cnt > 1
        """))
        duplicates = result.fetchall()
        if duplicates:
            print(f"   ✗ ERROR: Found {len(duplicates)} duplicate date-well combinations in daily_production!")
            all_passed = False
        else:
            print(f"   ✓ No duplicate primary keys in daily_production")

        result = conn.execute(text(f"""
            SELECT {MONTHLY_DATE}, {MONTHLY_NPD_WELLBORE_CODE}, COUNT(*) as cnt
            FROM {TABLE_MONTHLY_PRODUCTION}
            GROUP BY {MONTHLY_DATE}, {MONTHLY_NPD_WELLBORE_CODE}
            HAVING cnt > 1
        """))
        duplicates = result.fetchall()
        if duplicates:
            print(f"   ✗ ERROR: Found {len(duplicates)} duplicate date-well combinations in monthly_production!")
            all_passed = False
        else:
            print(f"   ✓ No duplicate primary keys in monthly_production")

    return all_passed


def main():
    """
    Main ETL pipeline orchestration.
    """
    print("="*80)
    print("VOLVE PRODUCTION DATA ETL PIPELINE")
    print("="*80)
    print(f"\nSource: {SOURCE_EXCEL_PATH}")
    print(f"Target: {DATABASE_PATH}")

    # Check if source file exists
    if not Path(SOURCE_EXCEL_PATH).exists():
        print(f"\n✗ ERROR: Source file not found: {SOURCE_EXCEL_PATH}")
        print("Please ensure the Volve dataset is downloaded and placed in the data/ directory")
        return

    # Check if database exists
    if not Path(DATABASE_PATH).exists():
        print(f"\n✗ ERROR: Database not found: {DATABASE_PATH}")
        print("Please run create_tables.py first to create the database schema")
        return

    # Create database connection
    engine = create_database_connection()

    # Load Excel file
    print(f"\nLoading Excel file...")
    print(f"  Sheet 1: {SHEET_DAILY}")
    print(f"  Sheet 2: {SHEET_MONTHLY}")

    daily_df = pd.read_excel(SOURCE_EXCEL_PATH, sheet_name=SHEET_DAILY)
    monthly_df = pd.read_excel(SOURCE_EXCEL_PATH, sheet_name=SHEET_MONTHLY)

    print(f"\n✓ Loaded {len(daily_df):,} daily records")
    print(f"✓ Loaded {len(monthly_df):,} monthly records (including header row)")

    # Execute ETL pipeline
    try:
        wells_count = load_wells_table(daily_df, engine)
        daily_count = load_daily_production_table(daily_df, engine)
        monthly_count = load_monthly_production_table(monthly_df, engine)

        # Validate data
        validation_passed = validate_data_integrity(engine)

        # Summary
        print("\n" + "="*80)
        print("ETL PIPELINE COMPLETE")
        print("="*80)
        print(f"\nRecords loaded:")
        print(f"  Wells: {wells_count}")
        print(f"  Daily production: {daily_count:,}")
        print(f"  Monthly production: {monthly_count:,}")
        print(f"\nValidation: {'✓ PASSED' if validation_passed else '✗ FAILED'}")
        print(f"\nDatabase ready at: {DATABASE_PATH}")
        print("="*80)

    except Exception as e:
        print(f"\n✗ ERROR during ETL pipeline:")
        print(f"  {str(e)}")
        raise


if __name__ == "__main__":
    main()
