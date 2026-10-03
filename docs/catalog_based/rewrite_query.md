# Using rewrite_query in a notebook

`generator.rewrite_query` is for a Databricks notebook. You write SQL against the Unity Catalog view, the method turns that statement into a direct UDTF call, and `spark.sql` runs the call. Filters that the method understands are sent to CDF. Filters it does not understand stay out of the call.

Power BI cannot call this method. When the same statement has to run from Power BI, use [SQL functions for Power BI](./sql_functions.md).

The view must already be registered by this generator. The generator reads the view's columns, so it can tell a numeric column from a text column and pass every UDTF argument (`NULL` when the statement does not set it).

## Run a rewritten query

```python
sql = """
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
  AND name = 'XBOX'
  AND description IS NOT NULL
LIMIT 10
"""
rewritten = generator.rewrite_query(sql, secret_scope="cdf_sailboat_sailboat")
if rewritten is None:
    raise ValueError("This statement is not pushed to CDF")
df = spark.sql(rewritten)
df.show()
```

`secret_scope` is the scope used at registration (`cdf_sailboat_sailboat` for this model). Omit it and the generator uses `cdf_{space}_{external_id}`.

The returned SQL calls `small_boat_udtf` with:

- `SECRET()` for `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project`, and `base_url`
- `instance_space => 'inst_sailboat_fleet_a'` for the view column `space`
- `name => 'XBOX'`
- `_exists` for `description IS NOT NULL`
- `_row_limit => 10`
- every other property and pushdown argument set to `NULL`

Print `rewritten` before `spark.sql` when you want to see which arguments were bound.

## Supported statements

Each example below is a full statement you can pass to `rewrite_query`. Several of these can be combined in one `WHERE` clause. `AND` is the supported combination. `OR` is not rewritten.

### Instance space

`space` on the view is the instance space, not the view's model space.

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
```

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space IN ('inst_sailboat_fleet_a', 'inst_sailboat_fleet_b')
```

The UDTF argument is `instance_space`. An `IN` list is passed as a JSON array.

### External id

```sql
SELECT space, external_id, name
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE external_id = 'seed_small_boat_fleet_a_01'
```

```sql
SELECT external_id, name
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE external_id IN ('seed_small_boat_fleet_a_01', 'seed_small_boat_fleet_a_02')
```

### Property equality

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name = 'XBOX'
```

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name IN ('XBOX', 'YBOX')
```

### Exists

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE description IS NOT NULL
```

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE description IS NULL
```

These become `_exists` and `_not_exists`. There is no separate argument for the value, because the predicate has no value.

### Numeric range

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE aph_tod >= 10 AND aph_tod < 20
```

`>`, `>=`, `<`, and `<=` map to `_gt`, `_gte`, `_lt`, and `_lte`. The column must be numeric. A range on a text column is not pushed.

### Limit

```sql
SELECT external_id, name
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
LIMIT 10
```

`LIMIT` becomes `_row_limit` on a list query. `ORDER BY` is not pushed. A statement that also has `ORDER BY` is still rewritten for the filters and the limit, and the sort is not applied by CDF.

### Count, min, and max

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
```

The count is returned in `count_externalId`, as a string. The alias in the analyst query (`n`) is not the column name.

```sql
SELECT min(aph_tod) AS low, max(aph_tod) AS high
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
```

`MIN` and `MAX` come back as `min_aph_tod` and `max_aph_tod`. CDF accepts `MIN` and `MAX` on numeric properties only. `MIN(name)` makes `rewrite_query` return `None`.

Two aggregates on the same property share one output column, so `MIN(aph_tod)` and `MAX(aph_tod)` in one statement are pushed. `MIN(name)` together with `COUNT(*)` is not, because `name` is not numeric.

### Group by

`GROUP BY` is pushed when the select list is exactly the grouped columns plus `COUNT(*)` or a numeric `MIN` / `MAX`. The grouped columns are returned in the order you wrote them. A grouped aggregate returns at most 1000 groups.

```sql
SELECT name, count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
GROUP BY name
```

```sql
SELECT space, name, count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE aph_tod >= 10
GROUP BY space, name
```

`GROUP BY` of an expression (`lower(name)`) is not pushed. A selected column that is not grouped and not aggregated is not pushed. Grouping by a column and also aggregating that same column (`GROUP BY name` with `MIN(name)`, or `GROUP BY external_id` with `COUNT(*)`) is not pushed, because both results would use the same output column.

## When rewrite_query returns None

Run the original statement in Spark in that case. Nothing was pushed.

- The statement has a join, `HAVING`, `OFFSET`, or `COUNT(DISTINCT)`.
- `GROUP BY` does not match the select list, groups an expression, or has no `COUNT` / `MIN` / `MAX`.
- `MIN` or `MAX` is on a text or timestamp column.
- A column in the statement is not on the view.
- The view is not part of this generator's data model.
- The statement has no filter, aggregate, or limit that can be pushed.

`rewrite_query` returns `None` and does not raise. `create_sql_function` raises `QueryNotPushdownCompatible` for the same statements, because creating a function that does not push would leave Power BI on the slow path.

## Related

- [Filtering](./filtering.md) — the predicate table
- [SQL functions for Power BI](./sql_functions.md) — the same statements as Unity Catalog functions
- [Investigating performance (EXPLAIN)](./explain_filter_pushdown.md)
