# Joining UDTFs

## Joining on external_id

The most common join pattern is joining UDTFs based on `external_id`:

```sql
-- Join two UDTFs on external_id
SELECT 
    v.external_id,
    v.name AS vessel_name,
    s.external_id AS sensor_id,
    s.value AS sensor_value
FROM vessel_udtf(...) v
JOIN sensor_udtf(...) s ON v.external_id = s.vessel_external_id
WHERE v.space = 'sailboat'
LIMIT 10;
```

## Joining on space + external_id

For more precise joins, combine `space` and `external_id`:

```sql
-- Join on space and external_id (more precise)
SELECT 
    a.external_id,
    a.space,
    b.parent_external_id,
    b.child_external_id
FROM parent_udtf(...) a
JOIN child_udtf(...) b 
    ON a.space = b.space 
    AND a.external_id = b.parent_external_id
LIMIT 10;
```

## Join order matters: lessons from the field

When you join two UDTFs (or a UDTF with a time series UDTF) using `CROSS JOIN LATERAL`, **the order of the tables matters more than you might think**. Here's what actually happens:

- The **first** UDTF (left side) runs once—its `eval()` method executes once and returns N rows (e.g., 50 time series IDs).
- The **second** UDTF (inside `LATERAL`) runs **once per row** from the first. If the first UDTF returns 50 rows, the second UDTF's `eval()` gets called 50 times, making 50 separate HTTP requests to CDF.

**What teams have learned**: If you put a UDTF that returns thousands of rows on the left, you'll trigger thousands of calls to the right-hand UDTF. This is the classic "N+1 problem"—what should be a quick query can take minutes or time out entirely.

**The practical rule**: Put the **smaller, filtered result set** (the "producer") on the left, and the **parameterized UDTF** (the "consumer") on the right. This way, the consumer runs fewer times, and each call processes a smaller, more focused set of data.

### How to structure your join: a practical guide

**The key principle**: In a `CROSS JOIN LATERAL`, the **left side runs `eval()` once** and produces N rows. The **right side (inside `LATERAL`) runs `eval()` N times**—once for each row from the left.

**To minimize UDTF invocations**: Put the **smallest result set on the LEFT** (runs once and produces N rows). The **right-hand side (inside `LATERAL`) runs N times**—once per row from the left. So fewer rows on the left means fewer UDTF calls.

**Example: Correct structure**

```sql
-- ✅ CORRECT: Small result set (10 rows after WHERE) on LEFT, UDTF inside LATERAL
-- The first UDTF runs once and returns 10 rows; the second UDTF runs only 10 times
SELECT 
    meta.external_id AS ts_id,
    meta.name AS ts_name,
    data.timestamp,
    data.value
FROM nmea_time_series_udtf(..., name => 'MyBoat') meta  -- Small result set: filter to few rows (e.g. 10 time series)
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        instance_id => CONCAT(meta.space, ':', meta.external_id),
        start => '1d-ago',
        end => 'now',
        ...
    )
) data  -- Runs 10 times (once per row from the left)
WHERE meta.space = 'sailboat';
```

**What happens**: The first UDTF runs **once** and returns 10 time series IDs (thanks to the filter). The second UDTF runs **10 times** (once per time series). Total UDTF invocations: 10.

**Example: Incorrect structure (what NOT to do)**

```sql
-- ❌ WRONG: Unfiltered first UDTF (1000 rows) on LEFT
-- The first UDTF runs once and returns 1000 rows; the second UDTF runs 1000 times
-- That's 1000 CDF calls instead of 10—much slower

-- Better: Filter the left-hand UDTF so it returns few rows (e.g. one asset or a small set)
```

**The pattern to follow**:

1. **Estimate row counts**: How many rows will the left-hand side produce after filters?
2. **Put the smaller result set on the LEFT**: Use filters so the left side returns as few rows as possible. It runs once.
3. **Put the parameterized UDTF inside `LATERAL`**: It runs once per row from the left, so fewer rows on the left = fewer UDTF calls.

**When this matters most**: When the left side could produce many rows (e.g. thousands). Add filters so the left side is small; then the LATERAL UDTF runs fewer times and the query stays fast.

## CROSS JOIN LATERAL Patterns

`CROSS JOIN LATERAL` is useful for dynamic queries where one UDTF's output determines the parameters for another UDTF:

```sql
-- Use vessel data to query time series
-- Note: Construct instance_id from space and external_id columns
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    ts.timestamp,
    ts.value AS speed
FROM vessel_udtf(...) v
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        instance_id => CONCAT(v.space, ':', v.speed_ts_external_id),  -- Format: "space:external_id"
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_scope', 'client_id'),
        client_secret => SECRET('cdf_scope', 'client_secret'),
        tenant_id => SECRET('cdf_scope', 'tenant_id'),
        cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
        project => SECRET('cdf_scope', 'project')
    )
) ts
WHERE v.space = 'sailboat'
ORDER BY v.external_id, ts.timestamp
LIMIT 100;
```

## Joining with Data Model Views

You can join session-scoped UDTFs with Data Model views that contain time series references:

```sql
-- Join vessel view with time series using instance_id from view
-- Note: Construct instance_id from space and external_id columns
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    v.speed_ts_space,
    v.speed_ts_external_id,
    ts.timestamp,
    ts.value AS speed
FROM vessel_udtf(...) v
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        instance_id => CONCAT(v.speed_ts_space, ':', v.speed_ts_external_id),  -- Format: "space:external_id"
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_scope', 'client_id'),
        client_secret => SECRET('cdf_scope', 'client_secret'),
        tenant_id => SECRET('cdf_scope', 'tenant_id'),
        cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
        project => SECRET('cdf_scope', 'project')
    )
) ts
WHERE v.space = 'sailboat'
  AND v.speed_ts_external_id IS NOT NULL
ORDER BY v.external_id, ts.timestamp;
```

## Querying Multiple Time Series from Data Model Views

When a Data Model view contains references to multiple time series, you can query them all:

```sql
-- Query multiple time series referenced in a view
-- Note: Construct instance_id from space and external_id columns
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    speed_ts.timestamp AS speed_timestamp,
    speed_ts.value AS speed_value,
    course_ts.timestamp AS course_timestamp,
    course_ts.value AS course_value
FROM vessel_udtf(...) v
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        instance_id => CONCAT(v.speed_ts_space, ':', v.speed_ts_external_id),  -- Format: "space:external_id"
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_scope', 'client_id'),
        client_secret => SECRET('cdf_scope', 'client_secret'),
        tenant_id => SECRET('cdf_scope', 'tenant_id'),
        cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
        project => SECRET('cdf_scope', 'project')
    )
) speed_ts
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        instance_id => CONCAT(v.course_ts_space, ':', v.course_ts_external_id),  -- Format: "space:external_id"
        start => '1d-ago',
        end => 'now',
        client_id => SECRET('cdf_scope', 'client_id'),
        client_secret => SECRET('cdf_scope', 'client_secret'),
        tenant_id => SECRET('cdf_scope', 'tenant_id'),
        cdf_cluster => SECRET('cdf_scope', 'cdf_cluster'),
        project => SECRET('cdf_scope', 'project')
    )
) course_ts
WHERE v.space = 'sailboat'
  AND v.speed_ts_external_id IS NOT NULL
  AND v.course_ts_external_id IS NOT NULL
ORDER BY v.external_id, speed_ts.timestamp;
```

## Performance: when lateral joins become a bottleneck

**The reality**: A `CROSS JOIN LATERAL` between UDTFs causes the right-hand UDTF to run **once per row** from the left. For small row counts (under ~100), this is usually fine—the overhead is minimal. But when the left-hand UDTF returns thousands of rows, teams have seen queries that should take seconds instead take minutes or even time out.

**Why it gets slow**: Each UDTF invocation makes a separate HTTP request to CDF, and there's overhead in setting up the UDTF's internal state (protobuf parsers, retry trackers, etc.) for each call. Multiply that by thousands of rows, and you're looking at a significant performance hit.

**A better approach many teams use**: Instead of one SQL query with `LATERAL`, run **separate Spark SQL queries with tight WHERE clauses** and then **join the DataFrames in Spark**. This way:

1. Each query does less work per row because the filters reduce the data upfront.
2. You can control how many UDTF calls you make (e.g., batch multiple instance_ids into fewer calls).
3. Spark's join is highly optimized and often faster than the nested loop that `LATERAL` creates.

Here's the pattern that's worked well for others:

```python
# 1. Query the first UDTF with filters (small result set)
meta_df = spark.sql("""
    SELECT external_id, space, speed_ts_space, speed_ts_external_id
    FROM vessel_udtf(...)
    WHERE space = 'sailboat' AND name = 'MyBoat'
""")

# 2. Query the time series UDTF for the instance_ids you need (one or more calls—you control the count)
#    Example: one UDTF call per instance_id from meta_df, then union the DataFrames
ts_df = spark.sql("""
    SELECT space, external_id, timestamp, value
    FROM time_series_datapoints_udtf(
        instance_id => 'sailboat:my-speed-ts',
        start => '1d-ago',
        end => 'now',
        ...
    )
""")

# 3. Join on the time series keys (meta_df's refs match ts_df's space/external_id)
result = meta_df.join(
    ts_df,
    (meta_df.speed_ts_space == ts_df.space) & (meta_df.speed_ts_external_id == ts_df.external_id),
    how="inner",
)
```

**When to use this approach**: If your left-hand UDTF would return more than a few hundred rows, or if you're seeing slow queries with `CROSS JOIN LATERAL`, try the separate-queries approach. You'll often see a 10x or more speedup because you're making far fewer CDF calls and doing less work per row.

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- See [Time Series UDTFs](./time_series.md) for more details


