# Filtering

## WHERE Clauses on Views

Views support standard SQL WHERE clauses. **Some** predicates can be pushed down to CDF
`instances/list` (or `instances/aggregate`) when you query through the UDTF with bound
parameters, or when you rewrite catalog SQL with
[`DataModelQueryRewriter`](../../cognite/databricks/data_model_query_rewriter.py).

```sql
-- Filter by view property (pushed when bound as UDTF arg or rewritten)
SELECT * FROM main.sailboat_sailboat_1.smallboat
WHERE name = 'MyBoat'
LIMIT 10;
```

Plain Unity Catalog view wrappers currently pass `prop => NULL` for every view property.
Spark may still apply WHERE **after** the UDTF returns rows unless you:

1. Call the UDTF directly with named filter arguments, or
2. Use `DataModelQueryRewriter.rewrite_to_udtf_sql(...)` to bind pushdown parameters.

## What is pushed to CDF today

| SQL pattern | CDF FilterDefinition / API | Status |
|-------------|----------------------------|--------|
| `prop = 'x'` (view property UDTF arg) | `equals` | Pushed |
| `prop IN (...)` | `in` | Pushed |
| Array property `=` / `IN` (UDTF knows which properties are arrays) | `containsAny` | Pushed (rewriter / explicit param) |
| `prop IS NOT NULL` via `_exists` | `exists` | Pushed (rewriter / explicit param) |
| `prop IS NULL` via `_not_exists` | `not.exists` | Pushed (rewriter / explicit param) |
| `space = '...'` via `instance_space` | `equals` on `["node\|edge", "space"]` | Pushed (instance identity, not view space) |
| `external_id = '...'` via UDTF `external_id` param | `equals` on `["node\|edge", "externalId"]` | Pushed |
| `>`, `<`, `BETWEEN` via `_gt`/`_gte`/`_lt`/`_lte` | `range` | Pushed (rewriter / explicit param) |
| `LIMIT n` via `_row_limit` (no `ORDER BY`) | list API `limit` + early stop | Pushed |
| `COUNT(*)` via `_query_mode='aggregate'` | `instances/aggregate` | Pushed |
| `MIN` / `MAX` on a numeric property | `instances/aggregate` | Pushed |
| `GROUP BY` selected columns plus `COUNT(*)` or numeric `MIN` / `MAX` | `instances/aggregate` groupBy | Pushed (at most 1000 groups) |
| `MIN` / `MAX` on text / timestamp | — | **Spark-only** — CDF aggregates only numeric properties |
| `ORDER BY ... LIMIT n` | — | **Spark-only** (sort may differ) |
| `OFFSET`, joins, `HAVING`, `COUNT(DISTINCT)` | — | **Spark-only / not rewritten** |

### Instance space vs view space

- **Instance `space`** (SQL column `space` on the view): identity of the node/edge. Pushdown uses
  `["node", "space"]` or `["edge", "space"]`.
- **View model space**: the first segment of view property paths
  `["viewSpace", "ViewExternalId/version", "prop"]`. Do not confuse the two.

Aggregate API `limit` caps **groupBy buckets**, not SQL `LIMIT` on list scans.

## Predicate pushdown with the rewriter

Views pass every pushdown argument as `NULL`, so a `WHERE` on a view runs in Spark. To push it to CDF,
rewrite the query into a UDTF call with bound arguments. Use the generator — it knows the view's columns:

```python
sql = """
SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name = 'XBOX'
  AND description IS NOT NULL
LIMIT 10
"""
rewritten = generator.rewrite_query(sql)  # binds name, _exists, _row_limit into small_boat_udtf(...)
df = spark.sql(rewritten if rewritten is not None else sql)
```

The notebook guide is [Using rewrite_query in a notebook](./rewrite_query.md). It walks through each supported statement: space, external id, property equality, exists, ranges, `LIMIT`, `COUNT`, numeric `MIN` / `MAX`, and `GROUP BY`.

`rewrite_query()` returns `None` when the query should run as-is in Spark:

- unsupported patterns (joins, `OFFSET`, `HAVING`, `COUNT(DISTINCT)`; `ORDER BY ... LIMIT` keeps the limit in Spark)
- a column the view does not have
- `MIN` / `MAX` on a non-numeric column — CDF only aggregates numeric properties
- a view outside the generator's data model

The low-level `DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=..., view_metadata=...)` does the
same; pass `view_metadata=DataModelViewMetadata.from_view(view)`. Without metadata it cannot tell numeric from
text columns, so `MIN(name)` is pushed and the UDTF rejects it with a clear error. Default UDTF names follow
`to_udtf_function_name(view_name)` (`SmallBoat` → `small_boat_udtf`), matching registration.

Requires cognite-pygen-spark 0.4.1 or newer, which generates the pushdown parameters and the trailing `base_url` argument.

## Slow view queries in Power BI

Power BI sends SQL to the view. The view does not bind pushdown arguments, so the filters run in Spark after CDF returns the rows. Pass that same statement to `generator.create_sql_function`. Power BI then calls the SQL function and passes the filter values as arguments.

See [SQL functions for Power BI](./sql_functions.md).

## EXPLAIN

See [Investigating UDTF-backed catalog view performance](./explain_filter_pushdown.md) for
`EXPLAIN` / `EXPLAIN FORMATTED`, Query Profile, and before/after `DataModelQueryRewriter`
examples (Databricks EXPLAIN adapted to Cognite UDTF views).

## Related

- [Using rewrite_query in a notebook](./rewrite_query.md)
- [Investigating UDTF-backed catalog view performance](./explain_filter_pushdown.md)
- [SQL functions for Power BI](./sql_functions.md)
- [Querying](./querying.md)
- [Troubleshooting](./troubleshooting.md)
