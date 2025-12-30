# Volve Parquet Exports Usage Guide

This directory contains Parquet exports of the Volve production database. These files are optimized for columnar access and are ideal for analytical queries.

## Export Metadata

<!-- START_METADATA_TABLE -->
| Table Name | Rows | File Size (MB) | Last Updated |
| :--- | :--- | :--- | :--- |
| wells | 7 | 0.0048 | 2025-12-30 06:10:58 |
| daily_production | 15,634 | 0.8341 | 2025-12-30 06:10:58 |
| monthly_production | 526 | 0.0174 | 2025-12-30 06:10:58 |
<!-- END_METADATA_TABLE -->

## Usage with DuckDB-WASM (Browser)

DuckDB-WASM allows you to run high-performance SQL queries directly in your browser on Parquet files hosted on GitHub Pages.

```javascript
import * as duckdb from '@duckdb/duckdb-wasm';

// 1. Initialize DuckDB-WASM
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

// 2. Query Parquet file from GitHub Pages URL
const conn = await db.connect();
const url = 'https://<username>.github.io/volve-db/parquet/daily_production.parquet';

await conn.query(`
    CREATE VIEW daily_prod AS 
    SELECT * FROM read_parquet('${url}');
`);

// 3. Execute analysis
const result = await conn.query(`
    SELECT 
        date, 
        SUM(oil_volume) as total_oil 
    FROM daily_prod 
    GROUP BY date 
    ORDER BY date;
`);

console.table(result.toArray());
```

## Usage with DuckDB Python

DuckDB can query Parquet files directly over HTTPS, making it perfect for remote analysis without downloading large datasets.

```python
import duckdb

# Query directly from URL
url = "https://<username>.github.io/volve-db/parquet/daily_production.parquet"

# Join remote Parquet files
wells_url = "https://<username>.github.io/volve-db/parquet/wells.parquet"

query = f"""
    SELECT 
        w.wellbore_name,
        SUM(d.oil_volume) as total_oil
    FROM read_parquet('{url}') d
    JOIN read_parquet('{wells_url}') w ON d.npd_wellbore_code = w.npd_wellbore_code
    GROUP BY w.wellbore_name
    ORDER BY total_oil DESC
"""

df = duckdb.query(query).to_df()
print(df)
```

## Usage with Pandas / Polars

You can also load these files directly into Pandas or Polars DataFrames.

### Pandas
```python
import pandas as pd

url = "https://<username>.github.io/volve-db/parquet/daily_production.parquet"
df = pd.read_parquet(url)
print(df.head())
```

### Polars
```python
import polars as pl

url = "https://<username>.github.io/volve-db/parquet/daily_production.parquet"
df = pl.read_parquet(url)
print(df.head())
```

## Schema Information

The schemas in these Parquet files exactly match the SQLite database schema. For detailed column definitions and descriptions, please refer to:
- [SCHEMA_DOCUMENTATION.md](../scripts/transform/SCHEMA_DOCUMENTATION.md)

## Automatic Updates

These files are automatically updated whenever the source data or transformation scripts change in the `main` branch, via the **Build Parquet Exports** GitHub Action.
