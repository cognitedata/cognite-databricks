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

**To minimize UDTF invocations**: Put the **largest result set on the LEFT** (runs once), and the **smaller result set inside `LATERAL`** (runs per row). This way, the expensive operation (the UDTF) runs fewer times.

**Example: Correct structure**

```sql
-- ✅ CORRECT: Large View (1000 rows) on LEFT, small UDTF inside LATERAL
-- The View runs once, the UDTF runs 1000 times (once per vessel)
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    ts.timestamp,
    ts.value AS speed
FROM main.sailboat_sailboat_1.vessel v  -- Large result set (1000 vessels)
CROSS JOIN LATERAL (
    SELECT * FROM main.cdf_models.time_series_datapoints_udtf(
        instance_id => CONCAT(v.speed_ts_space, ':', v.speed_ts_external_id),
        start => '1d-ago',
        end => 'now',
        ...
    )
) ts  -- Small result set per vessel (runs 1000 times, but each call is fast)
WHERE v.space = 'sailboat';
```

**What happens**: The View query runs **once** and returns 1000 rows. The time series UDTF runs **1000 times** (once per vessel), but each call is fast because it's fetching data for a single time series.

**Example: Incorrect structure (what NOT to do)**

```sql
-- ❌ WRONG: If you put a small View (10 rows) on LEFT and a large UDTF inside LATERAL
-- The View runs once (10 rows), but the UDTF runs 10 times
-- If the UDTF returns 1000 datapoints per call, you're still doing 10 UDTF calls
-- This is inefficient if the UDTF is expensive or if you could batch the calls

-- Better: Put the large result set on the LEFT so it runs once
```

**The pattern to follow**:

1. **Estimate row counts**: How many rows will each side produce?
2. **Put the larger result set on the LEFT**: It runs `eval()` once.
3. **Put the smaller, parameterized side inside `LATERAL`**: It runs `eval()` per row, but each call is fast because it's processing a single parameter.

**When this matters most**: When one side is significantly larger than the other (e.g., 1000+ rows vs. 10 rows). If both sides are similar in size, the difference is less critical.

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

# 2. Query the time series UDTF for just the instance_ids you need
#    You can batch these or call the UDTF once per ID, but you control the number of calls
#    (e.g., collect instance_ids from meta_df, then run UDTF in batch or one query per ID)

# 3. Join the two DataFrames in Spark—this is fast and efficient
result = meta_df.join(ts_df, on=["space", "external_id"], how="inner")
```

**When to use this approach**: If your View would return more than a few hundred rows, or if you're seeing slow queries with `CROSS JOIN LATERAL`, try the separate-queries approach. You'll often see a 10x or more speedup because you're making far fewer CDF calls and doing less work per row.

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- See [Time Series UDTFs](./time_series.md) for more details


