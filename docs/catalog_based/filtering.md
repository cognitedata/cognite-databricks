# Filtering

## WHERE Clauses on Views

Views support standard SQL WHERE clauses, which are pushed down to the underlying UDTF:

```sql
-- Filter by external_id
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE external_id = 'my-boat-123'
LIMIT 10;

-- Filter by multiple conditions
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE space = 'sailboat'
  AND name = 'MyBoat'
  AND description IS NOT NULL
ORDER BY external_id;
```

## Predicate Pushdown in Views

Predicate pushdown means WHERE clause conditions are evaluated in the UDTF's Python code before making CDF API calls. This improves performance by reducing data transfer.

**Supported Filter Operations:**
- Equality: `WHERE external_id = 'value'`
- Inequality: `WHERE count > 100`, `WHERE timestamp < '2025-01-01'`
- NULL checks: `WHERE name IS NOT NULL`, `WHERE description IS NULL`
- Multiple conditions: `WHERE space = 'sailboat' AND external_id = 'vessel'`

## Filter Examples

### Equality Filters

```sql
-- Filter by single property
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE name = 'MyBoat'
LIMIT 10;

-- Filter by space and external_id
SELECT * FROM main.sailboat_sailboat_1.vessel
WHERE space = 'sailboat' AND external_id = 'vessel-123'
LIMIT 10;
```

### Range Filters

```sql
-- Filter by timestamp range
SELECT * FROM main.power_windturbine_1.pump_view
WHERE timestamp > '2025-01-01' AND timestamp < '2025-12-31'
ORDER BY timestamp;

-- Filter by numeric range
SELECT * FROM main.power_windturbine_1.sensor_view
WHERE value > 100 AND value < 200
LIMIT 10;
```

### NULL Handling

```sql
-- Filter out NULL values
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE description IS NOT NULL
LIMIT 10;

-- Find records with NULL values
SELECT * FROM main.sailboat_sailboat_1.vessel
WHERE name IS NULL
LIMIT 10;
```

### Complex Filters

```sql
-- Complex filtering with multiple conditions
SELECT * FROM main.power_windturbine_1.pump_view
WHERE space = 'power'
  AND timestamp > '2025-01-01'
  AND status = 'active'
  AND value > 50
ORDER BY timestamp DESC
LIMIT 100;
```

## Next Steps

- Learn about [Joining](./joining.md) Views together
- See [Querying](./querying.md) for more query examples


