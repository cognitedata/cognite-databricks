# SQL-Native Time Series UDTF

This guide describes the SQL-native Time Series UDTF that supports aggregate pushdown while remaining fully compatible with Unity Catalog.

> **Status: Alpha**
> The SQL-native time series UDTF is experimental and not fully tested. Expect changes.

## Overview

The SQL-native UDTF uses **pushdown hints** for:
- start and end time filtering
- aggregate selection
- granularity grouping

These hints are required to avoid raw datapoint retrieval.

## Detailed vs SQL-Native UDTFs

**`time_series_datapoints_detailed_udtf`**:
- Flexible: supports raw datapoints or aggregates (aggregates/granularity optional).
- Returns status metadata (`status_code`, `status_symbol`) for datapoints.
- Best for debugging or when you need status info.

**`time_series_sql_udtf`** (this guide):
- Requires aggregate + granularity hints (pushdown enforced).
- Returns aggregated datapoints only (no status columns).
- Best for SQL-native analytics and pushdown performance.

## UDTF Name

`time_series_sql_udtf`

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `instance_ids` | STRING | Yes | Comma-separated instance IDs in format `"space1:ext_id1,space2:ext_id2"` |
| `start_hint` | STRING/TIMESTAMP | Yes | Start time (ISO 8601, SQL TIMESTAMP, or relative time) |
| `end_hint` | STRING/TIMESTAMP | Yes | End time (ISO 8601, SQL TIMESTAMP, or relative time) |
| `aggregate_hint` | STRING | Yes | SQL aggregate hint (`avg`, `sum`, `min`, `max`, `count`) |
| `granularity_hint` | STRING | Yes | Granularity hint (e.g., `14d`, `1h`, `30s`) |

## Aggregate Mappings

| SQL Aggregate | CDF Aggregate |
|-------------|---------------|
| `AVG()` | `average` |
| `SUM()` | `sum` |
| `MIN()` | `min` |
| `MAX()` | `max` |
| `COUNT()` | `count` |

## Example: Direct SQL Usage

```sql
SELECT 
    date_add('2025-01-01', CAST(FLOOR(datediff(timestamp, '2025-01-01') / 14) * 14 AS INT)) AS period_start_date,
    external_id,
    value AS average_value
FROM main.cdf_models.time_series_sql_udtf(
    client_id        => SECRET('cdf_scope', 'client_id'),
    client_secret    => SECRET('cdf_scope', 'client_secret'),
    tenant_id        => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster      => SECRET('cdf_scope', 'cdf_cluster'),
    project          => SECRET('cdf_scope', 'project'),
    instance_ids     => 'space1:ts1,space1:ts2',
    start_hint       => '2025-01-01',
    end_hint         => '2025-12-31',
    aggregate_hint   => 'average',
    granularity_hint => '14d'
)
ORDER BY period_start_date DESC, external_id;
```

## Table Function Syntax (Notebook vs SQL Warehouse)

Databricks SQL UDTFs are table functions, and the invocation syntax differs by environment:

**Notebook `%sql` (cluster-backed):**
```sql
SELECT *
FROM main.cdf_models.time_series_sql_udtf(
    client_id        => SECRET('cdf_scope', 'client_id'),
    client_secret    => SECRET('cdf_scope', 'client_secret'),
    tenant_id        => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster      => SECRET('cdf_scope', 'cdf_cluster'),
    project          => SECRET('cdf_scope', 'project'),
    instance_ids     => 'space1:ts1,space1:ts2',
    start_hint       => '2025-01-01',
    end_hint         => '2025-12-31',
    aggregate_hint   => 'average',
    granularity_hint => '14d'
) AS t;
```

**SQL Warehouse (Databricks SQL):**
```sql
SELECT *
FROM TABLE(
  main.cdf_models.time_series_sql_udtf(
      client_id        => SECRET('cdf_scope', 'client_id'),
      client_secret    => SECRET('cdf_scope', 'client_secret'),
      tenant_id        => SECRET('cdf_scope', 'tenant_id'),
      cdf_cluster      => SECRET('cdf_scope', 'cdf_cluster'),
      project          => SECRET('cdf_scope', 'project'),
      instance_ids     => 'space1:ts1,space1:ts2',
      start_hint       => '2025-01-01',
      end_hint         => '2025-12-31',
      aggregate_hint   => 'average',
      granularity_hint => '14d'
  )
);
```

## Example: Extract Hints Programmatically

```python
from cognite.databricks.sql_analyzer import SQLQueryAnalyzer

sql = """
SELECT AVG(value) AS avg_value
FROM time_series
WHERE timestamp >= '2025-01-01' AND timestamp <= '2025-12-31'
GROUP BY date_add('2025-01-01', CAST(FLOOR(datediff(timestamp, '2025-01-01') / 14) * 14 AS INT))
"""

hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

print(hints)
```

## Notes

- Unity Catalog requires all parameters to be provided explicitly in SQL.
- The UDTF enforces aggregate and granularity hints to avoid raw datapoint retrieval.
- If you need a persistent View, generate one with `generate_time_series_sql_view()`.
