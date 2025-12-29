# Volve Production Data - Analysis Findings

**File**: `data/production/Volve production data.xlsx`
**Analysis Date**: 2025-12-28

## Overview

The Excel file contains **2 sheets** with production data from the Volve field:
1. **Daily Production Data** - High granularity daily measurements
2. **Monthly Production Data** - Aggregated monthly production volumes

---

## Sheet 1: Daily Production Data

### Summary
- **Records**: 15,634 rows
- **Columns**: 24 columns
- **Date Range**: September 1, 2007 to December 1, 2016 (~9.25 years)
- **Grain**: Daily production records per wellbore

### Column Categories

#### Identification & Metadata (7 columns)
- `DATEPRD` (datetime) - Production date
- `WELL_BORE_CODE` (text) - Well bore identifier (e.g., "NO 15/9-F-1 C")
- `NPD_WELL_BORE_CODE` (int) - Norwegian Petroleum Directorate well code
- `NPD_WELL_BORE_NAME` (text) - NPD well name
- `NPD_FIELD_CODE` (int) - Field code (all records = 3420717, Volve field)
- `NPD_FIELD_NAME` (text) - Field name
- `NPD_FACILITY_CODE` (int) - Facility code
- `NPD_FACILITY_NAME` (text) - Facility name

#### Operational Metrics (10 columns)
- `ON_STREAM_HRS` (float) - Hours the well was producing (1.8% missing)
- `AVG_DOWNHOLE_PRESSURE` (float) - Average downhole pressure (42.6% missing)
- `AVG_DOWNHOLE_TEMPERATURE` (float) - Average downhole temperature (42.6% missing)
- `AVG_DP_TUBING` (float) - Average differential pressure in tubing (42.6% missing)
- `AVG_ANNULUS_PRESS` (float) - Average annulus pressure (49.5% missing)
- `AVG_CHOKE_SIZE_P` (float) - Average choke size percentage (43.0% missing)
- `AVG_CHOKE_UOM` (text) - Choke unit of measurement (41.4% missing)
- `AVG_WHP_P` (float) - Average wellhead pressure (41.4% missing)
- `AVG_WHT_P` (float) - Average wellhead temperature (41.5% missing)
- `DP_CHOKE_SIZE` (float) - Differential pressure choke size (1.9% missing)

#### Production Volumes (4 columns)
- `BORE_OIL_VOL` (float) - Oil volume from bore (41.4% missing)
- `BORE_GAS_VOL` (float) - Gas volume from bore (41.4% missing)
- `BORE_WAT_VOL` (float) - Water volume from bore (41.4% missing)
- `BORE_WI_VOL` (float) - Water injection volume (63.5% missing)

#### Classification (2 columns)
- `FLOW_KIND` (text) - Type of flow (e.g., "production")
- `WELL_TYPE` (text) - Well type (e.g., "OP" for oil producer, "WI" for water injector)

### Data Quality Observations

#### Missing Data Patterns
1. **High missingness** (~40-50%): Downhole sensors, pressure/temperature measurements
   - Suggests not all wells had downhole monitoring equipment
   - Or sensors were not always operational

2. **Water injection volume** (63.5% missing):
   - Expected, as only water injection wells (WELL_TYPE = "WI") would have this data

3. **Minimal missingness** (~2%): Operational hours, choke size
   - Core operational parameters are well-recorded

#### Negative Values
- `BORE_WAT_VOL` has minimum of -457.84
- Likely indicates measurement corrections or adjustments

#### Date Coverage
- Spans full production lifecycle: 2007-2016
- Represents ~9 years of operations

---

## Sheet 2: Monthly Production Data

### Summary
- **Records**: 527 rows (526 valid + 1 header row)
- **Columns**: 10 columns
- **Date Range**: 2007-2016 (monthly aggregations)
- **Grain**: Monthly production totals per wellbore

### Column Structure

#### Identification (4 columns)
- `Wellbore name` (text) - Well name
- `NPDCode` (float) - NPD well code
- `Year` (float) - Production year
- `Month` (float) - Production month (1-12)

#### Production Metrics (6 columns)
- `On Stream` (text) - Time on stream (format unclear, stored as text)
- `Oil` (text) - Oil production volume (units in first row: "Sm3")
- `Gas` (text) - Gas production volume (units: "Sm3")
- `Water` (text) - Water production volume (units: "Sm3")
- `GI` (text) - Gas injection (99.8% missing - rarely used)
- `WI` (text) - Water injection (61.7% missing)

### Data Quality Observations

#### Header Row Issue
- **Row 1** contains unit labels ("Sm3") instead of data
- Should be removed during transformation
- Explains why numeric columns are stored as text

#### Missing Data
1. **Gas Injection** (99.8%): Almost never used in Volve operations
2. **Water Injection** (61.7%): Only relevant for injection wells
3. **Oil/Gas/Water** (40.8%): Significant missing production data
   - May represent non-producing months or shut-in periods

#### Data Type Issues
- Production volumes stored as **text** instead of numeric
- Needs conversion to float during database transformation
- Unit standardization required (Sm3 = Standard cubic meters)

#### Date Representation
- Dates split into separate `Year` and `Month` columns
- No exact date (day component missing)
- Will need to construct date column (e.g., first day of month)

---

## Key Differences Between Sheets

| Aspect | Daily Production | Monthly Production |
|--------|-----------------|-------------------|
| **Granularity** | Daily | Monthly |
| **Records** | 15,634 | 526 |
| **Time Span** | 2007-09-01 to 2016-12-01 | 2007-2016 |
| **Data Quality** | Clean datetime, numeric types | Text types, header row contamination |
| **Completeness** | ~60% complete | ~60% complete |
| **Detail Level** | Operational metrics (pressure, temp) | Production totals only |

---

## Database Schema Recommendations

### Approach 1: Single Table with Temporal Grain
- Create one `production` table with a `granularity` column
- Less normalized, but simpler queries

### Approach 2: Separate Tables (Recommended)
```sql
-- Recommended structure

CREATE TABLE daily_production (
    date DATE NOT NULL,
    wellbore_code TEXT NOT NULL,
    npd_wellbore_code INTEGER NOT NULL,
    wellbore_name TEXT NOT NULL,

    -- Operational metrics
    on_stream_hours REAL,
    avg_downhole_pressure REAL,
    avg_downhole_temperature REAL,
    avg_choke_size_pct REAL,
    avg_wellhead_pressure REAL,
    avg_wellhead_temperature REAL,

    -- Production volumes
    oil_volume REAL,
    gas_volume REAL,
    water_volume REAL,
    water_injection_volume REAL,

    -- Classification
    flow_kind TEXT,
    well_type TEXT,

    PRIMARY KEY (date, wellbore_code)
);

CREATE TABLE monthly_production (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    wellbore_name TEXT NOT NULL,
    npd_code INTEGER NOT NULL,

    on_stream_hours REAL,
    oil_volume_sm3 REAL,
    gas_volume_sm3 REAL,
    water_volume_sm3 REAL,
    gas_injection_sm3 REAL,
    water_injection_sm3 REAL,

    PRIMARY KEY (year, month, wellbore_name)
);
```

### Transformation Steps Required

#### Daily Production
1. ✅ Date column already parsed correctly
2. ✅ Numeric columns in correct types
3. ⚠️ Handle missing values explicitly (NULL vs 0)
4. ⚠️ Investigate negative water volumes
5. ✅ Standardize column names (lowercase, underscores)

#### Monthly Production
1. ❌ **Remove header row** (row 0 with "Sm3" units)
2. ❌ **Convert text columns to numeric**: Oil, Gas, Water, WI, On Stream
3. ❌ **Create proper date column** from Year + Month
4. ⚠️ Handle missing values (NULL for non-producing months)
5. ✅ Standardize column names

---

## Next Steps

1. **Data Cleaning**:
   - Remove header row from monthly sheet
   - Convert monthly production volumes to numeric types
   - Standardize column names across both sheets

2. **Schema Design**:
   - Create database schema with appropriate constraints
   - Add indexes on date and wellbore columns for query performance
   - Consider foreign keys to wellbore dimension table

3. **Data Validation**:
   - Verify daily totals roughly match monthly aggregations
   - Check for duplicate records
   - Validate date continuity

4. **Documentation**:
   - Document unit conversions (Sm3 for gas, bbl for oil if needed)
   - Note missing data patterns for users
   - Explain difference between production and injection wells
