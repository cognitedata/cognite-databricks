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
| Array property filter | `containsAny` | Pushed |
| `prop IS NOT NULL` via `_exists` | `exists` | Pushed (rewriter / explicit param) |
| `prop IS NULL` via `_not_exists` | `not.exists` | Pushed (rewriter / explicit param) |
| `space = '...'` via `instance_space` | `equals` on `["node\|edge", "space"]` | Pushed (instance identity, not view space) |
| `external_id = '...'` via UDTF `external_id` param | `equals` on `["node\|edge", "externalId"]` | Pushed |
| `>`, `<`, `BETWEEN` via `_gt`/`_gte`/`_lt`/`_lte` | `range` | Pushed (rewriter / explicit param) |
| `LIMIT n` via `_row_limit` (no `ORDER BY`) | list API `limit` + early stop | Pushed |
| `COUNT(*)` / `MIN` / `MAX` via `_query_mode='aggregate'` | `instances/aggregate` | Pushed (see #68) |
| `ORDER BY ... LIMIT n` | — | **Spark-only** (sort may differ) |
| `OFFSET`, joins, `HAVING`, `COUNT(DISTINCT)` | — | **Spark-only / not rewritten** |

### Instance space vs view space

- **Instance `space`** (SQL column `space` on the view): identity of the node/edge. Pushdown uses
  `["node", "space"]` or `["edge", "space"]`.
- **View model space**: the first segment of view property paths
  `["viewSpace", "ViewExternalId/version", "prop"]`. Do not confuse the two.

Aggregate API `limit` caps **groupBy buckets**, not SQL `LIMIT` on list scans.

## Predicate pushdown with the rewriter

```python
from cognite.databricks import DataModelQueryRewriter

sql = """
SELECT * FROM adg_cdf_dev.gold.LimsResults
WHERE TestSeqNumber = '5889450'
  AND DilutionFactor IS NOT NULL
LIMIT 10
"""
rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql)
# Bind _exists, TestSeqNumber, _row_limit into the UDTF call
```

## EXPLAIN

See [EXPLAIN and filter / LIMIT pushdown](./explain_filter_pushdown.md) for how to verify
whether predicates are bound into the UDTF or applied only in Spark.

## Related

- pygen-spark issues [#68](https://github.com/cognitedata/pygen-spark/issues/68) (aggregates) and
  [#69](https://github.com/cognitedata/pygen-spark/issues/69) (WHERE / LIMIT / instance space)
