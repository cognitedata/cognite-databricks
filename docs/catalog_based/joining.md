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

## Join order matters: what others have learned

When you join a View with a time series UDTF using `CROSS JOIN LATERAL`, **the order of the tables matters more than you might expect**. Here's what happens under the hood:

- The **first** table (the View) runs once—its query executes once and returns N rows (e.g., 50 vessels).
- The **second** table (the UDTF inside `LATERAL`) runs **once per row** from the first. If your View returns 50 rows, the time series UDTF's `eval()` method gets called 50 times, making 50 separate HTTP requests to CDF.

**What this means in practice**: If you put a View that returns thousands of rows on the left, you'll trigger thousands of UDTF calls. Teams have seen queries that should take seconds instead take minutes or even time out because of this "N+1" pattern.

**The rule of thumb**: Put the **smaller, filtered result set** (the "producer") on the left, and the **parameterized UDTF** (the "consumer") on the right. This way, the UDTF runs fewer times, and each call does less work because it's operating on a smaller set of IDs.

### How to structure your join: a practical guide

**The key principle**: In a `CROSS JOIN LATERAL`, the **left side runs `eval()` once** and produces N rows. The **right side (inside `LATERAL`) runs `eval()` N times**—once for each row from the left.

**To minimize UDTF invocations**: Put the **smallest result set on the LEFT** (runs once and produces N rows). The **right-hand side (inside `LATERAL`) runs N times**—once per row from the left. So fewer rows on the left means fewer UDTF calls.

**Example: Correct structure**

```sql
-- ✅ CORRECT: Small View (10 rows after WHERE) on LEFT, UDTF inside LATERAL
-- The View runs once and returns 10 rows; the UDTF runs only 10 times (once per vessel)
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    ts.timestamp,
    ts.value AS speed
FROM main.sailboat_sailboat_1.vessel v  -- Small result set: filter so you get few rows (e.g. 10 vessels)
CROSS JOIN LATERAL (
    SELECT * FROM main.cdf_models.time_series_datapoints_udtf(
        instance_id => CONCAT(v.speed_ts_space, ':', v.speed_ts_external_id),
        start => '1d-ago',
        end => 'now',
        ...
    )
) ts  -- Runs 10 times (once per row from the left)
WHERE v.space = 'sailboat' AND v.name = 'MyBoat';  -- Tight filter = small left result set
```

**What happens**: The View runs **once** and returns 10 rows (thanks to the filter). The time series UDTF runs **10 times** (once per vessel). Total UDTF invocations: 10.

**Example: Incorrect structure (what NOT to do)**

```sql
-- ❌ WRONG: Unfiltered View (1000 rows) on LEFT
-- The View runs once and returns 1000 rows; the UDTF runs 1000 times
-- That's 1000 CDF calls instead of 10—much slower

-- Better: Add a tight WHERE so the left side returns few rows (e.g. one vessel or a small set)
```

**The pattern to follow**:

1. **Estimate row counts**: How many rows will the left side produce after filters?
2. **Put the smaller result set on the LEFT**: Use a tight `WHERE` so the left side returns as few rows as possible. It runs once.
3. **Put the parameterized UDTF inside `LATERAL`**: It runs once per row from the left, so fewer rows on the left = fewer UDTF calls.

**When this matters most**: When the left side could produce many rows (e.g. thousands). Add filters so the left side is small; then the LATERAL UDTF runs fewer times and the query stays fast.

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
        instance_id => CONCAT(v.speed_ts_space, ':', v.speed_ts_external_id),
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

## Performance: when lateral joins become a bottleneck

**The reality**: A `CROSS JOIN LATERAL` between a View and a time series UDTF causes the UDTF to run **once per row** from the View. For small row counts (under ~100), this is usually fine—you'll barely notice the overhead. But when the View returns thousands of rows, teams have seen queries slow down significantly or even time out.

**Why it gets slow**: Each UDTF invocation makes a separate HTTP request to CDF, and there's overhead in setting up the UDTF's internal state (protobuf parsers, retry trackers, etc.) for each call. Multiply that by thousands of rows, and you're looking at minutes instead of seconds.

**A better approach many teams use**: Instead of one SQL query with `LATERAL`, run **separate queries with tight WHERE clauses** and then **join the DataFrames in Spark**. This way:

1. Each query does less work per row because the filters reduce the data upfront.
2. You can control how many UDTF calls you make (e.g., batch multiple instance_ids into fewer calls).
3. Spark's join is highly optimized and often faster than the nested loop that `LATERAL` creates.

Here's the pattern that's worked well for others:

```python
# 1. Query the View with filters (small result set)
meta_df = spark.sql("""
    SELECT external_id, space, speed_ts_space, speed_ts_external_id
    FROM main.sailboat_sailboat_1.vessel
    WHERE space = 'sailboat' AND name = 'MyBoat'
""")

# 2. Query the time series UDTF for the instance_ids you need (one or more calls—you control the count)
#    Example: one UDTF call per instance_id from meta_df, then union the DataFrames
ts_df = spark.sql("""
    SELECT space, external_id, timestamp, value
    FROM main.cdf_models.time_series_datapoints_udtf(
        instance_id => 'sailboat:my-speed-ts',
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
        client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
        tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
        cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
        project => SECRET('cdf_sailboat_sailboat', 'project')
    )
""")

# 3. Join on the time series keys (meta_df's refs match ts_df's space/external_id)
result = meta_df.join(
    ts_df,
    (meta_df.speed_ts_space == ts_df.space) & (meta_df.speed_ts_external_id == ts_df.external_id),
    how="inner",
)
```

**When to use this approach**: If your View would return more than a few hundred rows, or if you're seeing slow queries with `CROSS JOIN LATERAL`, try the separate-queries approach. You'll often see a 10x or more speedup because you're making far fewer CDF calls and doing less work per row.

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- See [Time Series UDTFs](./time_series.md) for more details


