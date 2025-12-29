"""
Explore Volve Production Data Excel File

This script analyzes the structure and content of the Volve production data Excel file.
It examines both sheets (daily and monthly production data) to understand:
- Column names and data types
- Number of records
- Date ranges
- Missing values
- Basic statistics for numerical columns
"""

import pandas as pd
from pathlib import Path


def analyze_sheet(df: pd.DataFrame, sheet_name: str) -> None:
    """
    Analyze and print detailed information about an Excel sheet.

    Args:
        df: DataFrame containing the sheet data
        sheet_name: Name of the sheet being analyzed
    """
    print(f"\n{'='*80}")
    print(f"SHEET: {sheet_name}")
    print(f"{'='*80}\n")

    # Basic information
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns\n")

    # Column information
    print("COLUMNS AND DATA TYPES:")
    print("-" * 80)
    for idx, (col_name, dtype) in enumerate(df.dtypes.items(), 1):
        non_null_count = df[col_name].notna().sum()
        null_count = df[col_name].isna().sum()
        print(f"{idx:2d}. {col_name:40s} | {str(dtype):15s} | Non-null: {non_null_count:,} | Null: {null_count:,}")

    # Date range (if date columns exist)
    print("\n" + "-" * 80)
    print("DATE RANGE:")
    print("-" * 80)
    date_columns = df.select_dtypes(include=['datetime64']).columns
    if len(date_columns) == 0:
        # Try to find columns that might be dates
        potential_date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if potential_date_cols:
            print(f"Found potential date columns: {potential_date_cols}")
            print("(These are not yet parsed as datetime objects)")
            for col in potential_date_cols[:1]:  # Check first date column
                print(f"\nFirst few values in '{col}':")
                print(df[col].head())
    else:
        for col in date_columns:
            min_date = df[col].min()
            max_date = df[col].max()
            print(f"{col}: {min_date} to {max_date}")

    # Numerical columns summary
    print("\n" + "-" * 80)
    print("NUMERICAL COLUMNS SUMMARY:")
    print("-" * 80)
    numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns
    if len(numerical_cols) > 0:
        print(df[numerical_cols].describe())
    else:
        print("No numerical columns found")

    # Sample data
    print("\n" + "-" * 80)
    print("FIRST 5 ROWS:")
    print("-" * 80)
    print(df.head())

    # Missing values summary
    print("\n" + "-" * 80)
    print("MISSING VALUES SUMMARY:")
    print("-" * 80)
    missing_data = df.isna().sum()
    missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
    if len(missing_data) > 0:
        print(f"\nColumns with missing values:")
        for col, count in missing_data.items():
            percentage = (count / len(df)) * 100
            print(f"  {col:40s}: {count:6,} ({percentage:5.1f}%)")
    else:
        print("No missing values found!")


def main():
    """Main function to analyze the Volve production data Excel file."""

    # Path to the Excel file
    excel_path = Path("data/production/Volve production data.xlsx")

    # Check if file exists
    if not excel_path.exists():
        print(f"Error: File not found at {excel_path}")
        print("Please ensure the Volve dataset is downloaded and placed in the data/ directory")
        return

    print("Loading Excel file...")
    print(f"File: {excel_path}")

    # Load all sheets
    excel_file = pd.ExcelFile(excel_path)

    print(f"\nFound {len(excel_file.sheet_names)} sheet(s):")
    for idx, sheet_name in enumerate(excel_file.sheet_names, 1):
        print(f"  {idx}. {sheet_name}")

    # Analyze each sheet
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        analyze_sheet(df, sheet_name)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
