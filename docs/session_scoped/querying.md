# Querying UDTFs

## Basic SQL Queries

Once registered, UDTFs can be queried using standard SQL. **For Unity Catalog registered UDTFs, all parameters must be explicitly provided.**

### Complete Example (All Parameters Required for Unity Catalog)

```sql
-- Complete query with all parameters (REQUIRED for Unity Catalog registered UDTFs)
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project'),
  name          => NULL,
  description   => NULL,
  tags          => NULL,
  aliases       => NULL,
  sourceId      => NULL,
  sourceContext => NULL,
  source        => NULL,
  sourceCreatedTime => NULL,
  sourceUpdatedTime => NULL,
  sourceCreatedUser => NULL,
  sourceUpdatedUser => NULL,
  asset         => NULL,
  serialNumber  => NULL,
  manufacturer  => NULL,
  equipmentType => NULL,
  files         => NULL,
  activities    => NULL,
  timeSeries    => NULL,
  mmsi_country  => NULL,
  boat_guid     => NULL,
  mmsi          => NULL,
  small_boat_guid => NULL
)
LIMIT 10;
```

### Table Function Syntax (Notebook vs SQL Warehouse)

UDTFs are table functions, and the SQL syntax differs by environment:

**Notebook `%sql` (cluster-backed):**
```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.time_series_datapoints_detailed_udtf(
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project'),
  instance_ids  => 'space1:ts1,space1:ts2',
  start         => current_timestamp() - INTERVAL 52 WEEKS,
  end           => current_timestamp() - INTERVAL 51 WEEKS,
  aggregates    => 'average',
  granularity   => '2h'
) AS t
ORDER BY timestamp
LIMIT 10;
```

**SQL Warehouse (Databricks SQL):**
```sql
SELECT *
FROM TABLE(
  f0connectortest.sailboat_sailboat_v1.time_series_datapoints_detailed_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project'),
    instance_ids  => 'space1:ts1,space1:ts2',
    start         => current_timestamp() - INTERVAL 52 WEEKS,
    end           => current_timestamp() - INTERVAL 51 WEEKS,
    aggregates    => 'average',
    granularity   => '2h'
  )
)
ORDER BY timestamp
LIMIT 10;
```

### Expected Output Columns

The UDTF returns 27 columns:
- **22 view property columns**: All properties from the data model view
- **space** (StringType, non-nullable): The space of the instance
- **external_id** (StringType, non-nullable): The external ID of the instance
- **createdTime** (TimestampType, nullable): When the instance was created in CDF
- **lastUpdatedTime** (TimestampType, nullable): When the instance was last updated in CDF
- **deletedTime** (TimestampType, nullable): When the instance was deleted (NULL if not deleted)

## Named Parameters

UDTFs support named parameters. **For Unity Catalog registered UDTFs, all parameters must be explicitly provided:**

```sql
-- Using named parameters (all parameters required for Unity Catalog)
SELECT * FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => 'MyBoat',
    description => NULL,
    tags => NULL,
    aliases => NULL,
    sourceId => NULL,
    sourceContext => NULL,
    source => NULL,
    sourceCreatedTime => NULL,
    sourceUpdatedTime => NULL,
    sourceCreatedUser => NULL,
    sourceUpdatedUser => NULL,
    asset => NULL,
    serialNumber => NULL,
    manufacturer => NULL,
    equipmentType => NULL,
    files => NULL,
    activities => NULL,
    timeSeries => NULL,
    mmsi_country => NULL,
    boat_guid => NULL,
    mmsi => NULL,
    small_boat_guid => NULL
) LIMIT 10;
```

## Using SECRET() for Credentials

The `SECRET()` function retrieves credentials from Databricks Secret Manager. **Always use SECRET() for credentials in UDTF queries**:

```sql
-- SECRET(scope_name, key_name)
SECRET('cdf_sailboat_sailboat', 'client_id')
```

**Important**: All UDTF SELECT statements must use SECRET() for credential parameters. Never use direct string literals for credentials in production code.

## Important Notes

### Unity Catalog Parameter Requirement

⚠️ **Unity Catalog requires ALL parameters to be explicitly provided in SQL queries**, even if they have default values in the Python code. Unity Catalog validates function calls at the SQL level and does not recognize Python default values or `parameter_default="NULL"` settings.

**Error if parameters are missing:**
```
[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `catalog`.`schema`.`small_boat_udtf` 
because the parameter named `name` is required, but the routine call did not supply a value.
```

**Solution**: Always provide all parameters when querying Unity Catalog registered UDTFs directly. Use the complete query format shown above.

### Pagination

The UDTF automatically handles pagination using `nextCursor` to fetch all pages of results from the CDF API. You don't need to handle pagination manually.

### Timestamp Fields

The `createdTime`, `lastUpdatedTime`, and `deletedTime` fields are automatically converted from milliseconds (CDF API format) to datetime objects (PySpark TimestampType).

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


