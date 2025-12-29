# Data Transformation Scripts

This directory contains scripts for transforming raw Volve production data from Excel format into a structured SQLite database.

## Files

### Core Scripts

- **`constants.py`** - Centralized constants and column definitions
  - Database table and column names
  - Source Excel column mappings
  - File paths and configuration
  - Reused across all transformation scripts

- **`create_tables.py`** - Database schema creation using SQLAlchemy Core
  - Defines three tables: wells, daily_production, monthly_production
  - Creates proper relationships and indexes
  - Educational SQL output (echo=True)
  - Run this FIRST to create the database structure

### Documentation

- **`SCHEMA_DOCUMENTATION.md`** - Comprehensive database schema documentation
  - Table structures and relationships
  - Data types and constraints
  - Query patterns and examples
  - Transformation pipeline details

- **`PRODUCTION_DATA_FINDINGS.md`** - Analysis of source Excel data
  - Located in `scripts/explore/`
  - Data quality observations
  - Transformation requirements

## Usage

### 1. Create Database Schema

```bash
# From project root
uv run python scripts/transform/create_tables.py
```

This creates `database/volve.db` with three empty tables:
- `wells` (master/dimension table)
- `daily_production` (time-series fact table)
- `monthly_production` (time-series fact table)

### 2. Load Data (Coming Next)

Future scripts will:
1. Extract unique wells from source data → populate `wells` table
2. Transform daily production data → populate `daily_production` table
3. Clean and transform monthly data → populate `monthly_production` table

## Database Schema Overview

```
wells (master table)
├── npd_wellbore_code (PK)
├── wellbore_code
├── wellbore_name
├── npd_field_code
├── npd_field_name
├── npd_facility_code
└── npd_facility_name

daily_production (time-series)
├── date (PK)
├── npd_wellbore_code (PK, FK → wells)
├── [17 operational metrics]
└── [4 production volumes]

monthly_production (time-series)
├── date (PK)
├── npd_wellbore_code (PK, FK → wells)
├── on_stream_hours
├── [3 production volumes in Sm3]
└── [2 injection volumes in Sm3]
```

## Design Principles

### SQLAlchemy Core (Not ORM)
All scripts use **SQLAlchemy Core** to stay close to raw SQL:
```python
# GOOD: Explicit SQL using Core
from sqlalchemy import text, Table, Column, Integer

query = text("SELECT * FROM wells WHERE npd_wellbore_code = :code")
result = conn.execute(query, {"code": 7405})
```

### Educational Focus
- SQL statements are logged (echo=True)
- Clear comments explaining each step
- Type hints for readability
- Follows project CLAUDE.md guidelines

### Constants-Based Design
Column names defined once in `constants.py`:
```python
from constants import WELLS_NPD_WELLBORE_CODE, DAILY_DATE

# Use constants instead of strings
wells_table.c[WELLS_NPD_WELLBORE_CODE]
```

Benefits:
- Avoid typos
- IDE autocomplete
- Easy refactoring
- Self-documenting code

## Database File Location

**Path**: `database/volve.db`

- Created automatically by `create_tables.py`
- SQLite format (single file)
- Portable and version-controllable
- ~5-10 MB when fully loaded

## Next Steps

1. ✅ Schema created
2. ⏳ Implement data loading scripts:
   - `load_wells.py` - Extract and load unique wells
   - `load_daily_production.py` - Load daily data
   - `load_monthly_production.py` - Load monthly data
3. ⏳ Data validation scripts
4. ⏳ Create SQL query examples

## Dependencies

Defined in `pyproject.toml`:
- `sqlalchemy` - Database toolkit (Core only)
- `pandas` - Excel file reading
- `openpyxl` - Excel support for pandas

Install with:
```bash
uv sync
```

## Troubleshooting

### "Table already exists" Error
```bash
# Delete the database and recreate
rm database/volve.db
uv run python scripts/transform/create_tables.py
```

### Import Errors
Make sure you run scripts from project root:
```bash
# ✅ Correct
uv run python scripts/transform/create_tables.py

# ❌ Wrong
cd scripts/transform
uv run python create_tables.py  # Can't find constants.py
```

### SQLite Browser
To inspect the database visually:
- Install [DB Browser for SQLite](https://sqlitebrowser.org/)
- Open `database/volve.db`
- View tables, schema, and run queries

## Educational Notes

This transformation pipeline demonstrates:

1. **Star Schema Design**: Dimension table (wells) + fact tables (production)
2. **Referential Integrity**: Foreign key constraints
3. **Composite Keys**: Multi-column primary keys for time-series
4. **Indexing Strategy**: Date and wellbore indexes for performance
5. **Explicit SQL**: SQLAlchemy Core keeps SQL visible
6. **Constants Pattern**: Centralized definitions reduce errors

Compare to typical ORM approach:
```python
# ORM (hides SQL)
session.query(Well).filter(Well.code == "F-1").all()

# Core (shows SQL)
query = text("SELECT * FROM wells WHERE wellbore_code = :code")
conn.execute(query, {"code": "F-1"})
```

The Core approach is more educational for learning SQL and database concepts.
