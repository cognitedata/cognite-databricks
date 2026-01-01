# Querying

## Querying Views (No Credentials Needed)

Views automatically handle credentials via SECRET() calls:

```sql
-- Basic query
SELECT * FROM main.sailboat_sailboat_1.smallboat
LIMIT 10;

-- Query with filters
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE name = 'MyBoat'
  AND description IS NOT NULL
ORDER BY external_id;
```

## Querying UDTFs Directly

When querying UDTFs directly, you must provide all required parameters:

```sql
-- Query UDTF with named parameters
SELECT * FROM main.sailboat_sailboat_1.smallboat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => 'MyBoat',
    description => NULL
) LIMIT 10;
```

## Named Parameters in Views

Views support named parameters for filtering:

```sql
-- Views can accept named parameters (if the underlying UDTF supports them)
-- However, the recommended approach is to use WHERE clauses
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE name = 'MyBoat'
LIMIT 10;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- Explore [Joining](./joining.md) Views together

