# Filtering

## WHERE Clauses in SQL

UDTFs support filtering via WHERE clauses in SQL. The filters are pushed down to the CDF API call, improving performance:

```sql
-- Filter by external_id
SELECT * FROM smallboat_udtf(...)
WHERE external_id = 'my-boat-123'
LIMIT 10;
```

## Predicate Pushdown

Predicate pushdown means that WHERE clause conditions are evaluated in the UDTF's Python code before making CDF API calls. This reduces the amount of data transferred and improves query performance.

**Supported Filter Operations:**
- Equality: `WHERE external_id = 'value'`
- Inequality: `WHERE count > 100`, `WHERE timestamp < '2025-01-01'`
- NULL checks: `WHERE name IS NOT NULL`, `WHERE description IS NULL`
- Multiple conditions: `WHERE space = 'sailboat' AND external_id = 'vessel'`

## Filter Examples

### Equality Filters

```sql
-- Filter by single property
SELECT * FROM smallboat_udtf(...)
WHERE name = 'MyBoat'
LIMIT 10;

-- Filter by space and external_id
SELECT * FROM vessel_udtf(...)
WHERE space = 'sailboat' AND external_id = 'vessel-123'
LIMIT 10;
```

### Range Filters

```sql
-- Filter by timestamp range
SELECT * FROM pump_view_udtf(...)
WHERE timestamp > '2025-01-01' AND timestamp < '2025-12-31'
ORDER BY timestamp;

-- Filter by numeric range
SELECT * FROM sensor_udtf(...)
WHERE value > 100 AND value < 200
LIMIT 10;
```

### NULL Handling

```sql
-- Filter out NULL values
SELECT * FROM smallboat_udtf(...)
WHERE description IS NOT NULL
LIMIT 10;

-- Find records with NULL values
SELECT * FROM vessel_udtf(...)
WHERE name IS NULL
LIMIT 10;
```

### Multiple Conditions

```sql
-- Complex filtering with multiple conditions
SELECT * FROM pump_view_udtf(...)
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


