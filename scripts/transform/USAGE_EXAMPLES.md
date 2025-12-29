# Volve Database Usage Examples

**Database**: `database/volve.db`
**Last Updated**: 2025-12-28

## Quick Start

The database has been loaded with production data from the Volve field:
- **7 wells** in the wells master table
- **15,634 daily production records** (2007-2016)
- **526 monthly production records** (2007-2016)

## Using SQLAlchemy Core

### Basic Query with Joins

```python
from sqlalchemy import create_engine, text

# Create connection
engine = create_engine("sqlite:///database/volve.db")

# Query with explicit SQL
query = text("""
    SELECT
        w.wellbore_name,
        d.date,
        d.oil_volume,
        d.gas_volume,
        d.water_volume
    FROM wells w
    JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
    WHERE w.wellbore_name = '15/9-F-12'
    ORDER BY d.date
    LIMIT 10
""")

with engine.connect() as conn:
    result = conn.execute(query)
    for row in result:
        print(f"{row.wellbore_name} | {row.date} | Oil: {row.oil_volume}")
```

### Using Constants for Column Names

```python
from sqlalchemy import create_engine, text
from scripts.transform.constants import (
    TABLE_WELLS,
    TABLE_DAILY_PRODUCTION,
    WELLS_WELLBORE_NAME,
    WELLS_NPD_WELLBORE_CODE,
    DAILY_DATE,
    DAILY_OIL_VOL,
    DAILY_NPD_WELLBORE_CODE,
)

engine = create_engine("sqlite:///database/volve.db")

# Build query using constants
query = text(f"""
    SELECT
        w.{WELLS_WELLBORE_NAME},
        d.{DAILY_DATE},
        d.{DAILY_OIL_VOL}
    FROM {TABLE_WELLS} w
    JOIN {TABLE_DAILY_PRODUCTION} d
        ON w.{WELLS_NPD_WELLBORE_CODE} = d.{DAILY_NPD_WELLBORE_CODE}
    WHERE d.{DAILY_OIL_VOL} IS NOT NULL
    ORDER BY d.{DAILY_DATE}
    LIMIT 5
""")

with engine.connect() as conn:
    result = conn.execute(query)
    for row in result:
        print(row)
```

## Using Pandas for Analysis

### Load Data into DataFrame

```python
import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('database/volve.db')

# Read data into pandas
df = pd.read_sql("""
    SELECT
        w.wellbore_name,
        d.date,
        d.oil_volume,
        d.gas_volume,
        d.water_volume
    FROM daily_production d
    JOIN wells w ON d.npd_wellbore_code = w.npd_wellbore_code
    WHERE d.oil_volume IS NOT NULL
    ORDER BY d.date
""", conn)

print(df.head())
print(f"\nTotal records: {len(df):,}")

conn.close()
```

### Time-Series Analysis

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('database/volve.db')

# Get monthly totals for a specific well
df = pd.read_sql("""
    SELECT
        strftime('%Y-%m', date) as year_month,
        SUM(oil_volume) as total_oil,
        SUM(gas_volume) as total_gas,
        SUM(water_volume) as total_water
    FROM daily_production
    WHERE npd_wellbore_code = 5599  -- 15/9-F-12
    GROUP BY year_month
    ORDER BY year_month
""", conn)

df['year_month'] = pd.to_datetime(df['year_month'])
df = df.set_index('year_month')

print(df.head(10))

conn.close()
```

## Common Query Patterns

### 1. Get All Wells

```sql
SELECT * FROM wells ORDER BY wellbore_name;
```

Result:
```
npd_wellbore_code | wellbore_name
5351              | 15/9-F-14
5599              | 15/9-F-12
5693              | 15/9-F-4
5769              | 15/9-F-5
7078              | 15/9-F-11
7289              | 15/9-F-15 D
7405              | 15/9-F-1 C
```

### 2. Production Summary by Well

```sql
SELECT
    w.wellbore_name,
    COUNT(DISTINCT d.date) as days_producing,
    ROUND(SUM(d.oil_volume), 2) as total_oil,
    ROUND(SUM(d.gas_volume), 2) as total_gas,
    ROUND(AVG(d.oil_volume), 2) as avg_daily_oil
FROM wells w
LEFT JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
GROUP BY w.wellbore_name
ORDER BY total_oil DESC;
```

### 3. Find Top Producing Days

```sql
SELECT
    w.wellbore_name,
    d.date,
    d.oil_volume,
    d.gas_volume
FROM daily_production d
JOIN wells w ON d.npd_wellbore_code = w.npd_wellbore_code
WHERE d.oil_volume IS NOT NULL
ORDER BY d.oil_volume DESC
LIMIT 10;
```

### 4. Water Injection Wells

```sql
SELECT DISTINCT
    w.wellbore_name,
    d.well_type
FROM wells w
JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
WHERE d.well_type = 'WI'
ORDER BY w.wellbore_name;
```

### 5. Production by Year

```sql
SELECT
    strftime('%Y', d.date) as year,
    ROUND(SUM(d.oil_volume), 2) as total_oil,
    ROUND(SUM(d.gas_volume), 2) as total_gas,
    ROUND(SUM(d.water_volume), 2) as total_water
FROM daily_production d
GROUP BY year
ORDER BY year;
```

### 6. Well Performance Comparison (2014)

```sql
SELECT
    w.wellbore_name,
    COUNT(*) as days_active,
    ROUND(SUM(d.oil_volume), 2) as oil_2014,
    ROUND(SUM(d.gas_volume), 2) as gas_2014
FROM wells w
JOIN daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
WHERE strftime('%Y', d.date) = '2014'
    AND d.oil_volume IS NOT NULL
GROUP BY w.wellbore_name
ORDER BY oil_2014 DESC;
```

### 7. Monthly Production Trends

```sql
SELECT
    w.wellbore_name,
    m.date,
    m.oil_volume_sm3,
    m.gas_volume_sm3,
    m.water_volume_sm3
FROM monthly_production m
JOIN wells w ON m.npd_wellbore_code = w.npd_wellbore_code
WHERE m.oil_volume_sm3 IS NOT NULL
ORDER BY m.date, w.wellbore_name;
```

### 8. Compare Daily Totals vs Monthly Aggregates

```sql
-- Daily sum for a specific month
SELECT
    'Daily Sum' as source,
    SUM(d.oil_volume) as oil,
    SUM(d.gas_volume) as gas
FROM daily_production d
WHERE npd_wellbore_code = 5599  -- 15/9-F-12
    AND strftime('%Y-%m', d.date) = '2014-04'

UNION ALL

-- Monthly aggregate for the same month
SELECT
    'Monthly Agg' as source,
    m.oil_volume_sm3 as oil,
    m.gas_volume_sm3 as gas
FROM monthly_production m
WHERE npd_wellbore_code = 5599
    AND strftime('%Y-%m', m.date) = '2014-04';
```

## Using Polars for High-Performance Analysis

```python
import polars as pl

# Read directly from SQLite
df = pl.read_database(
    "SELECT * FROM daily_production WHERE oil_volume IS NOT NULL",
    connection="sqlite:///database/volve.db"
)

# Fast aggregations
summary = (
    df.group_by('npd_wellbore_code')
    .agg([
        pl.col('oil_volume').sum().alias('total_oil'),
        pl.col('gas_volume').sum().alias('total_gas'),
        pl.col('date').count().alias('days_producing')
    ])
    .sort('total_oil', descending=True)
)

print(summary)
```

## Using DuckDB for SQL-like Analysis

```python
import duckdb

# Create in-memory database and attach SQLite
conn = duckdb.connect()
conn.execute("ATTACH 'database/volve.db' AS volve (TYPE SQLITE)")

# Query with DuckDB's fast analytics engine
result = conn.execute("""
    SELECT
        w.wellbore_name,
        SUM(d.oil_volume) as total_oil,
        AVG(d.oil_volume) as avg_oil,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d.oil_volume) as median_oil
    FROM volve.wells w
    JOIN volve.daily_production d ON w.npd_wellbore_code = d.npd_wellbore_code
    WHERE d.oil_volume IS NOT NULL
    GROUP BY w.wellbore_name
    ORDER BY total_oil DESC
""").fetchdf()

print(result)

conn.close()
```

## Visualization Example with Matplotlib

```python
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

conn = sqlite3.connect('database/volve.db')

# Get production over time for top well
df = pd.read_sql("""
    SELECT
        date,
        oil_volume,
        gas_volume,
        water_volume
    FROM daily_production
    WHERE npd_wellbore_code = 5599  -- 15/9-F-12 (top producer)
        AND oil_volume IS NOT NULL
    ORDER BY date
""", conn)

conn.close()

# Convert date to datetime
df['date'] = pd.to_datetime(df['date'])

# Create plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(df['date'], df['oil_volume'], label='Oil', linewidth=1)
ax.plot(df['date'], df['water_volume'], label='Water', linewidth=1)
ax.set_xlabel('Date')
ax.set_ylabel('Volume')
ax.set_title('Production History - Well 15/9-F-12')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('production_history.png', dpi=150)
plt.show()
```

## Tips for Efficient Queries

1. **Always filter on indexed columns** when possible:
   - `date` (indexed in both production tables)
   - `npd_wellbore_code` (indexed in both production tables)

2. **Use date ranges** instead of full table scans:
   ```sql
   WHERE date BETWEEN '2014-01-01' AND '2014-12-31'
   ```

3. **Aggregate in SQL** before loading into Python:
   ```sql
   -- Good: Aggregate in database
   SELECT year, SUM(oil_volume) FROM ...

   -- Avoid: Load all data then aggregate in pandas
   ```

4. **Use EXPLAIN QUERY PLAN** to optimize:
   ```sql
   EXPLAIN QUERY PLAN
   SELECT * FROM daily_production WHERE date > '2014-01-01';
   ```

5. **Leverage foreign key joins**:
   ```sql
   -- Fast: Uses FK index
   JOIN wells w ON d.npd_wellbore_code = w.npd_wellbore_code
   ```

## Next Steps

- Create analysis notebooks in `notebooks/`
- Build visualization scripts in `scripts/analysis/`
- Explore production decline curves
- Analyze water breakthrough
- Compare well performance
- Time-series forecasting

---

For more information, see:
- `scripts/transform/SCHEMA_DOCUMENTATION.md` - Database schema details
- `scripts/transform/README.md` - Transformation pipeline documentation
