# Session-Scoped UDTF Registration

## Introduction

Session-scoped UDTF registration allows you to register and use User-Defined Table Functions (UDTFs) within a single Databricks Spark session without requiring Unity Catalog registration. This approach is ideal for:

- **Development and Testing**: Quickly test UDTFs before committing to Unity Catalog registration
- **Prototyping**: Experiment with different UDTF configurations and queries
- **Quick Setup**: Use UDTFs in environments where custom dependencies can be installed via `%pip`
- **Temporary Analysis**: Run ad-hoc queries without permanent catalog registration

Session-scoped UDTFs are registered using PySpark Connect and are only available within the current Spark session. They are automatically cleaned up when the session ends.

## Overview

This documentation covers the complete workflow for using session-scoped UDTFs:

1. **[Installation](./installation.md)**: Set up dependencies and verify your environment
2. **[Registration](./registration.md)**: Register UDTFs for session-scoped use
3. **[Querying](./querying.md)**: Query UDTFs using SQL with various parameter styles
4. **[Filtering](./filtering.md)**: Filter data using WHERE clauses with predicate pushdown
5. **[Joining](./joining.md)**: Join data from different UDTFs based on `external_id` and `space`
6. **[Time Series](./time_series.md)**: Work with template-generated time series UDTFs (same template-based generation as Data Model UDTFs)
7. **[Troubleshooting](./troubleshooting.md)**: Common issues and solutions

## Quick Links

### Examples

- [Basic Registration](../../examples/session_scoped/basic_registration.ipynb): Install, generate, register, and query UDTFs
- [Querying Data](../../examples/session_scoped/querying_data.ipynb): Query single/multiple UDTFs, named vs positional parameters, time series
- [Filtering Queries](../../examples/session_scoped/filtering_queries.ipynb): Equality, range, NULL handling, multiple conditions
- [Joining UDTFs](../../examples/session_scoped/joining_udtfs.ipynb): Joins on external_id, space+external_id, CROSS JOIN LATERAL with time series

### Related Documentation

- [Catalog-Based UDTF Registration](../catalog_based/index.md): For production deployments with Unity Catalog
- Technical Plan: CDF Databricks Integration (UDTF-Based)


