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

**⚠️ Important: Unity Catalog Requirement**

When querying Unity Catalog registered UDTFs directly, **ALL parameters must be explicitly provided in SQL**, even if they have default values in the Python code. Unity Catalog does not recognize Python default values and will raise an error if any parameter is missing.

### Complete Example (All Parameters Required)

```sql
-- Complete query with all parameters (REQUIRED for Unity Catalog)
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

### Expected Output Columns

The UDTF returns 27 columns:
- **22 view property columns**: name, description, tags, aliases, sourceId, sourceContext, source, sourceCreatedTime, sourceUpdatedTime, sourceCreatedUser, sourceUpdatedUser, asset, serialNumber, manufacturer, equipmentType, files, activities, timeSeries, mmsi_country, boat_guid, mmsi, small_boat_guid
- **space** (StringType, non-nullable): The space of the instance
- **external_id** (StringType, non-nullable): The external ID of the instance
- **createdTime** (TimestampType, nullable): When the instance was created in CDF
- **lastUpdatedTime** (TimestampType, nullable): When the instance was last updated in CDF
- **deletedTime** (TimestampType, nullable): When the instance was deleted (NULL if not deleted)

### Filtering Example

```sql
-- Query with filtering by name (all parameters still required)
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project'),
  name          => 'MyBoatName',  -- Filter by specific name
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

### Why All Parameters Are Required

Unity Catalog validates function calls at the SQL level and requires all parameters defined in the function signature to be provided, regardless of Python default values. This is a Unity Catalog limitation, not a limitation of the generated UDTF code.

**Note**: The UDTF automatically handles pagination using `nextCursor` to fetch all pages of results from the CDF API. The timestamp fields (`createdTime`, `lastUpdatedTime`, `deletedTime`) are automatically converted from milliseconds (CDF API format) to datetime objects (PySpark TimestampType).

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


