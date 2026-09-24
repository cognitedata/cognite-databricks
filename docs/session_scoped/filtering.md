# Filtering

## WHERE Clauses in SQL

UDTFs support filtering via WHERE clauses in SQL. The filters are pushed down to the CDF API call, improving performance:

```sql
-- Filter by external_id
SELECT * FROM small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE external_id = 'my-boat-123'
LIMIT 10;
```

## Predicate Pushdown

Predicate pushdown means filter conditions are sent to CDF (`instances/list` or
`instances/aggregate`) instead of only being applied in Spark after a full page fetch.

**Pushed when bound as UDTF parameters** (or via `DataModelQueryRewriter` in cognite-databricks):

- Equality / `IN` on view properties → `equals` / `in`
- Array properties → `containsAny`
- `IS NOT NULL` / `IS NULL` via `_exists` / `_not_exists` → `exists` / `not.exists`
- Instance `space` / `external_id` via `instance_space` / `external_id` params → identity paths
- Ranges via `_gt` / `_gte` / `_lt` / `_lte` → `range`
- `LIMIT n` via `_row_limit` (not with `ORDER BY`) → list API `limit` + early pagination stop
- `COUNT(*)` / `MIN` / `MAX` via `_query_mode='aggregate'` → `instances/aggregate`

**Spark-only unless rewritten:** bare `WHERE` on a Unity Catalog view that still passes
`prop => NULL` for every argument; `ORDER BY ... LIMIT`; `OFFSET`; joins; `COUNT(DISTINCT)`.

**Instance space vs view space:** SQL column `space` is the **instance** identity space
(`["node","space"]`), not the view definition space used in property paths.

See also: [EXPLAIN filter pushdown](../catalog_based/explain_filter_pushdown.md),
[pygen-spark #68](https://github.com/cognitedata/pygen-spark/issues/68),
[pygen-spark #69](https://github.com/cognitedata/pygen-spark/issues/69).

## Filter Examples

### Equality Filters

```sql
-- Filter by single property
SELECT * FROM small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE name = 'MyBoat'
LIMIT 10;

-- Filter by space and external_id
SELECT * FROM vessel_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE space = 'sailboat' AND external_id = 'vessel-123'
LIMIT 10;
```

### Range Filters

```sql
-- Filter by timestamp range
SELECT * FROM pump_view_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE timestamp > '2025-01-01' AND timestamp < '2025-12-31'
ORDER BY timestamp;

-- Filter by numeric range
SELECT * FROM sensor_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE value > 100 AND value < 200
LIMIT 10;
```

### NULL Handling

```sql
-- Filter out NULL values
SELECT * FROM small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE description IS NOT NULL
LIMIT 10;

-- Find records with NULL values
SELECT * FROM vessel_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE name IS NULL
LIMIT 10;
```

### Multiple Conditions

```sql
-- Complex filtering with multiple conditions
SELECT * FROM pump_view_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
WHERE space = 'power'
  AND timestamp > '2025-01-01'
  AND status = 'active'
  AND value > 50
ORDER BY timestamp DESC
LIMIT 100;
```

## Next Steps

- Learn about [Joining](./joining.md) UDTFs together
- See [Querying](./querying.md) for more query examples


