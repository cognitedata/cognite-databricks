# Querying UDTFs

## Basic SQL Queries

Once registered, UDTFs can be queried using standard SQL:

```sql
-- Query a Data Model UDTF
SELECT * FROM smallboat_udtf(
    SECRET('cdf_sailboat_sailboat', 'client_id'),
    SECRET('cdf_sailboat_sailboat', 'client_secret'),
    SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    SECRET('cdf_sailboat_sailboat', 'project'),
    NULL,  -- name filter
    NULL,  -- description filter
    ...
) LIMIT 10;
```

## Named Parameters

UDTFs support named parameters for cleaner SQL:

```sql
-- Using named parameters (recommended)
SELECT * FROM smallboat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => 'MyBoat',
    description => NULL
) LIMIT 10;
```

## Using SECRET() for Credentials

The `SECRET()` function retrieves credentials from Databricks Secret Manager. **Always use SECRET() for credentials in UDTF queries**:

```sql
-- SECRET(scope_name, key_name)
SECRET('cdf_sailboat_sailboat', 'client_id')
```

**Important**: All UDTF SELECT statements must use SECRET() for credential parameters. Never use direct string literals for credentials in production code.

## Time Series UDTF Queries

### Single Time Series Datapoints

```sql
-- Query datapoints from a single time series
SELECT * FROM time_series_datapoints_udtf(
    instance_id => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
    start => '47w-ago',  -- Or use specific date like '2024-01-01T00:00:00Z'
    end => '46w-ago',    -- Or use specific date like '2024-12-31T23:59:59Z'
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY timestamp
LIMIT 10;
```

### Multiple Time Series (Long Format)

```sql
-- Query multiple time series in long format
-- Note: instance_ids uses format "space:external_id" and supports time series from different spaces
SELECT * FROM time_series_datapoints_long_udtf(
    instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',  -- Format: "space:external_id"
    start => '47w-ago',
    end => '46w-ago',
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY time_series_external_id, timestamp
LIMIT 20;
```

### Latest Datapoints

```sql
-- Get latest datapoints for one or more time series
-- Note: instance_ids uses format "space:external_id" and supports time series from different spaces
SELECT * FROM time_series_latest_datapoints_udtf(
    instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',  -- Format: "space:external_id"
    before => 'now',  -- Get latest before this time (or use '1h-ago', ISO 8601, etc.)
    include_status => true,  -- Include status_code in output
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY time_series_external_id;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- Explore [Joining](./joining.md) UDTFs together
- See [Time Series UDTFs](./time_series.md) for more details


