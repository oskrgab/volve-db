# Volve Dataset License

This project uses production data from the **Volve field dataset** released by Equinor.

## About the Volve Dataset

The Volve field dataset contains comprehensive production and operational data from the Volve field in the North Sea (2007-2016). Equinor released this dataset to support research, education, and technology development.

## Data Processing

The original Volve dataset provides production data in Excel format. For this project, the data has been **transformed into a structured SQLite database** with the following characteristics:

- **Star schema design** with dimension and fact tables
- **Referential integrity** with proper foreign key relationships
- **Data validation** and quality checks
- **Parquet exports** for high-performance analytics

This transformation creates a queryable database suitable for SQL learning, data engineering education, and analytical applications.

## License Terms

The original Volve dataset is released by Equinor under specific license terms.

### Official License Document

The complete terms and conditions for the Volve data license are available here:

**[Equinor HRS Terms and Conditions for Licence to Data - Volve](https://www.equinor.com/content/dam/statoil/documents/what-we-do/Equinor-HRS-Terms-and-conditions-for-licence-to-data-Volve.pdf)**

### Key License Characteristics

- **License Type**: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)
- **Source**: Equinor Data Portal - Volve Dataset
- **Permitted Use**: Research, education, and non-commercial purposes
- **Requirement**: Attribution to Equinor required

### What This Means

✅ **You CAN**:
- Use the data for educational purposes
- Use the data for research and academic work
- Create derivative works (like this database)
- Share the data and derivatives under the same license

❌ **You CANNOT**:
- Use the data for commercial purposes without permission
- Remove attribution to Equinor
- Apply more restrictive license terms to derivatives

## Attribution

When using this project or the data it contains, provide appropriate credit to Equinor:

> "This project uses data from the Volve field dataset released by Equinor under CC BY-NC-SA 4.0 license."

For academic citations:
```
Equinor (2018). Volve field dataset.
Available at: https://data.equinor.com/dataset/Volve
License: CC BY-NC-SA 4.0
```

## Scope of This License

### What This License Covers

- ✅ **Source data files** in `data/production/`
- ✅ **Generated database** in `database/volve.db`
- ✅ **Parquet exports** in `parquet/`
- ✅ **Any derivative data** created from the Volve dataset

### What This License Does NOT Cover

- ❌ **Software code** in this repository (licensed separately - see main LICENSE file if present)
- ❌ **Documentation files** created specifically for this project
- ❌ **ETL scripts and transformation logic** (project-specific code)

**Important**: The Volve dataset license applies specifically to the **data content**, not to the software implementation. Users are free to use their own datasets with this codebase for commercial applications.

## Compliance Requirements

If you use this project:

1. **Acknowledge Equinor** as the source of the original dataset
2. **Include license notice** when distributing the data or derivatives
3. **Respect non-commercial terms** - do not use for commercial purposes without proper licensing
4. **Share derivatives** under the same CC BY-NC-SA 4.0 license
5. **Review the full terms** in the official PDF document linked above

## Commercial Use

For commercial applications:
- Contact Equinor for commercial licensing options
- Alternatively, use this codebase with your own licensed datasets
- The software/code can be licensed independently from the Volve data

## References

- **Equinor Data Village**: https://www.equinor.com/energy/volve-data-sharing
- **Volve Dataset Portal**: https://data.equinor.com/dataset/Volve
- **Official License PDF**: https://www.equinor.com/content/dam/statoil/documents/what-we-do/Equinor-HRS-Terms-and-conditions-for-licence-to-data-Volve.pdf
- **CC BY-NC-SA 4.0 License**: https://creativecommons.org/licenses/by-nc-sa/4.0/

## Questions?

For questions about the Volve dataset license, contact Equinor directly through their data portal or licensing team.

---

**Last Updated**: January 2026

**Disclaimer**: This document summarizes the Volve dataset license for convenience. The official terms in the Equinor PDF document take precedence in case of any discrepancies.
