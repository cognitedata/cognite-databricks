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

The `SECRET()` function retrieves credentials from Databricks Secret Manager:

```sql
-- SECRET(scope_name, key_name)
SECRET('cdf_sailboat_sailboat', 'client_id')
```

**Note**: For session-scoped UDTFs, you can also pass credentials directly as string literals (not recommended for production):

```sql
-- Direct credentials (not recommended)
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    ...
) LIMIT 10;
```

## Time Series UDTF Queries

### Single Time Series Datapoints

```sql
-- Query datapoints from a single time series
SELECT * FROM time_series_datapoints_udtf(
    space => 'sailboat',
    external_id => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
    start => '1d-ago',
    end => 'now',
    client_id => SECRET('cdf_scope', 'client_id'),
    client_secret => SECRET('cdf_scope', 'client_secret'),
    tenant_id => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
    project => SECRET('cdf_scope', 'project')
) ORDER BY timestamp LIMIT 10;
```

### Multiple Time Series (Long Format)

```sql
-- Query multiple time series in long format
SELECT * FROM time_series_datapoints_long_udtf(
    space => ARRAY('sailboat', 'sailboat'),
    external_id => ARRAY(
        'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
        'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue'
    ),
    start => '1d-ago',
    end => 'now',
    client_id => SECRET('cdf_scope', 'client_id'),
    client_secret => SECRET('cdf_scope', 'client_secret'),
    tenant_id => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
    project => SECRET('cdf_scope', 'project')
) ORDER BY time_series_external_id, timestamp;
```

### Latest Datapoints

```sql
-- Get latest datapoints for one or more time series
SELECT * FROM time_series_latest_datapoints_udtf(
    space => ARRAY('sailboat', 'sailboat'),
    external_id => ARRAY(
        'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
        'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue'
    ),
    before => 'now',
    include_status => true,
    client_id => SECRET('cdf_scope', 'client_id'),
    client_secret => SECRET('cdf_scope', 'client_secret'),
    tenant_id => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
    project => SECRET('cdf_scope', 'project')
) ORDER BY time_series_external_id;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- Explore [Joining](./joining.md) UDTFs together
- See [Time Series UDTFs](./time_series.md) for more details

