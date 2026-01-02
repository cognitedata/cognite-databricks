# Views

## What are Views and Why Use Them

Views are SQL abstractions that wrap UDTFs, providing several benefits:

1. **No Credentials Required**: Views automatically inject SECRET() calls, so users don't need to provide credentials
2. **Simplified Queries**: Users query Views directly without knowing the underlying UDTF parameters
3. **Discoverability**: Views are indexed in the Databricks UI and appear in search results
4. **Governance**: Unity Catalog permissions apply to Views, enabling fine-grained access control

## View Discovery in Databricks UI

After registration, Views appear in the Databricks UI:

1. **Catalog Explorer**: Navigate to `Catalog > main > sailboat_sailboat_1 > smallboat`
2. **Search Box**: Search for "smallboat" in the Databricks search box
3. **Data Explorer**: Views appear alongside tables in the Data Explorer

## Querying Views vs UDTFs Directly

### Querying Views (Recommended)

Views are easier to query because they don't require credentials:

```sql
-- Query a View (no credentials needed)
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE name = 'MyBoat'
LIMIT 10;
```

### Querying UDTFs Directly

You can also query UDTFs directly, but you must provide credentials:

```sql
-- Query UDTF directly (credentials required)
SELECT * FROM main.sailboat_sailboat_1.smallboat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => 'MyBoat'
) LIMIT 10;
```

## Next Steps

- Learn about [Querying](./querying.md) Views and UDTFs
- See [Filtering](./filtering.md) for filtering examples
- Set up [Governance](./governance.md) permissions

