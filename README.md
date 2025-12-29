# Volve Production Database

A reproducible ETL pipeline that transforms production data from the Equinor Volve oil field dataset into a structured SQLite database for educational purposes and SQL learning.

## Overview

This project demonstrates the advantages of structured databases over Excel-only data storage by converting raw Volve production data (Excel format) into a queryable SQLite database. The focus is on **education** - teaching SQL, data engineering concepts, and database design to students and professionals.

## Features

✅ **Star schema design** with dimension and fact tables
✅ **SQLAlchemy Core** for transparent SQL operations (not ORM)
✅ **Reproducible ETL pipeline** from Excel to SQLite
✅ **Data validation** with referential integrity checks
✅ **Beginner-friendly** code with extensive documentation
✅ **Ready for analysis** with pandas, polars, or duckdb

## Database Contents

- **7 wells** from the Volve field
- **15,634 daily production records** (2007-2016)
- **526 monthly production records** (2007-2016)
- **Database size**: 3.2 MB

### Schema Overview

- **wells**: Master dimension table (7 wellbores)
- **daily_production**: Daily measurements (15,634 records, 2007-2016)
- **monthly_production**: Monthly aggregates (526 records, 2007-2016)

All tables are related via `npd_wellbore_code` foreign keys.

## Quick Start

### 1. Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### 2. Download Volve Dataset

Download the Volve production data Excel file from [Equinor Data Portal](https://data.equinor.com/dataset/Volve) and place it at:

```
data/production/Volve production data.xlsx
```

### 3. Create Database

```bash
# Create database schema
uv run python scripts/transform/create_tables.py

# Load data from Excel
uv run python scripts/transform/load_data.py
```

### 4. Query the Database

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('database/volve.db')

# Get production summary
df = pd.read_sql("""
    SELECT
        w.wellbore_name,
        SUM(d.oil_volume) as total_oil,
        SUM(d.gas_volume) as total_gas
    FROM wells w
    JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
    WHERE d.oil_volume IS NOT NULL
    GROUP BY w.wellbore_name
    ORDER BY total_oil DESC
""", conn)

print(df)
conn.close()
```

## Project Structure

```
volve-db/
├── data/
│   └── production/
│       └── Volve production data.xlsx    # Source data (download)
├── database/
│   └── volve.db                          # SQLite database (generated)
├── scripts/
│   ├── explore/
│   │   ├── analyze_production_data.py    # Data exploration
│   │   └── PRODUCTION_DATA_FINDINGS.md   # Analysis results
│   └── transform/
│       ├── constants.py                  # Reusable column definitions
│       ├── create_tables.py              # Schema creation
│       ├── load_data.py                  # ETL pipeline
│       ├── SCHEMA_DOCUMENTATION.md       # Database schema docs
│       ├── USAGE_EXAMPLES.md             # Query examples
│       └── README.md                     # Transformation guide
├── CLAUDE.md                             # Development guidelines
├── pyproject.toml                        # Dependencies (uv)
└── README.md                             # This file
```

## Example Queries

### Top Producing Wells

```sql
SELECT
    w.wellbore_name,
    ROUND(SUM(d.oil_volume), 2) as total_oil
FROM wells w
JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
WHERE d.oil_volume IS NOT NULL
GROUP BY w.wellbore_name
ORDER BY total_oil DESC;
```

**See [USAGE_EXAMPLES.md](scripts/transform/USAGE_EXAMPLES.md) for comprehensive query examples including:**
- Production summaries by well
- Time-series analysis
- Well performance comparison
- Pandas, Polars, and DuckDB examples
- Visualization with matplotlib

## Documentation

- **[SCHEMA_DOCUMENTATION.md](scripts/transform/SCHEMA_DOCUMENTATION.md)** - Complete database schema with ER diagrams
- **[USAGE_EXAMPLES.md](scripts/transform/USAGE_EXAMPLES.md)** - SQL queries and analysis examples
- **[PRODUCTION_DATA_FINDINGS.md](scripts/explore/PRODUCTION_DATA_FINDINGS.md)** - Source data analysis
- **[CLAUDE.md](CLAUDE.md)** - Development guidelines and principles

## Educational Focus

This project is designed for **learning SQL and data engineering**, not as a production application:

### Why SQLAlchemy Core (Not ORM)?

```python
# ✅ Good: Explicit SQL with Core
from sqlalchemy import text

query = text("""
    SELECT wellbore_name, SUM(oil_volume) as total
    FROM wells w
    JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
    GROUP BY wellbore_name
""")

result = conn.execute(query)

# ❌ Avoid: ORM hides the SQL
session.query(Well).join(DailyProduction)...
```

**Why?** Students can see and learn SQL directly. The ORM abstracts away the very concepts we're trying to teach.

### Design Principles

1. **Minimal dependencies** - Only essential packages
2. **Explicit SQL** - Show the SQL, don't hide it
3. **Educational code** - Readable, well-commented, type-hinted
4. **Reproducible** - Anyone can recreate the database
5. **Beginner-friendly** - Clear variable names, comprehensive docs

## Analysis Examples

### Using Pandas

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('database/volve.db')

# Monthly production trends
df = pd.read_sql("""
    SELECT
        strftime('%Y-%m', date) as month,
        SUM(oil_volume) as oil,
        SUM(gas_volume) as gas
    FROM daily_production
    WHERE oil_volume IS NOT NULL
    GROUP BY month
    ORDER BY month
""", conn)

df['month'] = pd.to_datetime(df['month'])
df = df.set_index('month')

# Plot
df.plot(figsize=(12, 6), title='Volve Field Production')

conn.close()
```

### Using Polars (Fast)

```python
import polars as pl

df = pl.read_database(
    "SELECT * FROM daily_production",
    connection="sqlite:///database/volve.db"
)

summary = (
    df.group_by('npd_wellbore_code')
    .agg([
        pl.col('oil_volume').sum(),
        pl.col('gas_volume').sum()
    ])
)

print(summary)
```

### Using DuckDB (SQL Analytics)

```python
import duckdb

conn = duckdb.connect()
conn.execute("ATTACH 'database/volve.db' AS volve (TYPE SQLITE)")

result = conn.execute("""
    SELECT
        w.wellbore_name,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d.oil_volume) as median_oil
    FROM volve.wells w
    JOIN volve.daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
    GROUP BY w.wellbore_name
""").fetchdf()

print(result)
```

## Tech Stack

- **Python 3.12+**
- **uv** - Fast package manager
- **SQLAlchemy** - Database toolkit (Core only)
- **pandas** - Data manipulation and Excel reading
- **SQLite** - Lightweight, portable database

Optional for analysis:
- **matplotlib** - Plotting
- **polars** or **duckdb** - Fast analytics

## Database Schema

The database uses a **star schema** design:

```
wells (dimension)
├── 7 unique wellbores
└── Static attributes: codes, names, field, facility

daily_production (fact)
├── 15,634 daily measurements
├── Operational metrics: pressure, temperature, choke
└── Production volumes: oil, gas, water

monthly_production (fact)
├── 526 monthly aggregates
└── Production volumes in Sm3
```

**See [SCHEMA_DOCUMENTATION.md](scripts/transform/SCHEMA_DOCUMENTATION.md) for:**
- Complete table schemas with all columns
- ER diagrams and relationship details
- Data types, constraints, and indexes
- ETL pipeline documentation

## Data Quality

### Daily Production
- **Completeness**: ~60% (operational metrics have ~40% missing values)
- **Missing sensors**: Not all wells had downhole pressure/temperature monitoring
- **Water injection**: Only populated for injection wells (expected)

### Monthly Production
- **Completeness**: ~60% (production volumes have ~40% missing values)
- **Gas injection**: 99.8% missing (rarely used)
- **Non-producing periods**: NULL values indicate shut-in months

## Contributing

This is an educational project. Contributions that improve clarity, add documentation, or enhance the learning experience are welcome.

**Please maintain**:
- SQLAlchemy Core approach (not ORM)
- Explicit SQL visibility
- Beginner-friendly code style
- Comprehensive comments and docstrings

## Roadmap

- [ ] Add example analysis notebooks
- [ ] Create visualization examples
- [ ] Add data validation tests
- [ ] Production decline curve analysis
- [ ] Water breakthrough analysis
- [ ] Well performance comparison
- [ ] Time-series forecasting examples

## License

This project processes data from the Equinor Volve dataset. Please refer to the original license terms:

https://cdn.equinor.com/files/h61q9gi9/global/de6532f6134b9a953f6c41bac47a0c055a3712d3.pdf

## Acknowledgments

- **Equinor** for releasing the Volve field dataset as open data
- The dataset is provided for research and educational purposes

---

**Educational Note**: This project demonstrates database design principles, ETL pipelines, and SQL concepts. It's intentionally kept simple to focus on learning rather than production-grade features.
