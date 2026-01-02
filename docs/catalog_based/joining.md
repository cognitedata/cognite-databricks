# Joining

## Joining Views on external_id

The most common join pattern is joining Views based on `external_id`:

```sql
-- Join two Views on external_id
SELECT 
    v.external_id,
    v.name AS vessel_name,
    s.external_id AS sensor_id,
    s.value AS sensor_value
FROM main.sailboat_sailboat_1.vessel v
JOIN main.power_windturbine_1.sensor s 
    ON v.external_id = s.vessel_external_id
WHERE v.space = 'sailboat'
LIMIT 10;
```

## Joining Views on space + external_id

For more precise joins, combine `space` and `external_id`:

```sql
-- Join on space and external_id (more precise)
SELECT 
    a.external_id,
    a.space,
    b.parent_external_id,
    b.child_external_id
FROM main.sailboat_sailboat_1.parent_view a
JOIN main.sailboat_sailboat_1.child_view b 
    ON a.space = b.space 
    AND a.external_id = b.parent_external_id
LIMIT 10;
```

## Joining Views with Time Series UDTFs

You can join Views with time series UDTFs using `CROSS JOIN LATERAL`:

```sql
-- Use vessel View to query time series
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    ts.timestamp,
    ts.value AS speed
FROM main.sailboat_sailboat_1.vessel v
CROSS JOIN LATERAL (
    SELECT * FROM main.cdf_models.time_series_datapoints_udtf(
        space => v.speed_ts_space,
        external_id => v.speed_ts_external_id,
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
        client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
        tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
        cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
        project => SECRET('cdf_sailboat_sailboat', 'project')
    )
) ts
WHERE v.space = 'sailboat'
ORDER BY v.external_id, ts.timestamp
LIMIT 100;
```

## Complex Join Patterns

### Multi-View Joins

```sql
-- Join multiple Views
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    s.external_id AS sensor_id,
    s.value AS sensor_value,
    p.external_id AS pump_id,
    p.status AS pump_status
FROM main.sailboat_sailboat_1.vessel v
JOIN main.power_windturbine_1.sensor s 
    ON v.external_id = s.vessel_external_id
JOIN main.power_windturbine_1.pump p 
    ON s.pump_external_id = p.external_id
WHERE v.space = 'sailboat'
ORDER BY v.external_id, s.timestamp
LIMIT 100;
```

### Join with Data Model References

When Views contain references to other nodes/edges, you can join them:

```sql
-- Join View with referenced View
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    v.owner_external_id,
    o.name AS owner_name
FROM main.sailboat_sailboat_1.vessel v
JOIN main.sailboat_sailboat_1.owner o 
    ON v.space = o.space 
    AND v.owner_external_id = o.external_id
WHERE v.space = 'sailboat'
LIMIT 10;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- See [Time Series UDTFs](./time_series.md) for more details

