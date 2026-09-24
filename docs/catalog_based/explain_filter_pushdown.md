# Investigating UDTF-backed catalog view performance

Use this guide when a Unity Catalog **view** (or UDTF) against CDF feels slow: selective
`WHERE` that still takes minutes, `LIMIT 10` that does not reduce traffic, or
`COUNT(*)` that looks like a full instance scan.

Background for the cognite-pygen-spark **0.4.0** / cognite-databricks rewriter work:
catalog views wrap UDTFs with `prop => NULL` defaults, so Spark often applies filters
**after** CDF returns pages. Pushdown only happens when parameters are bound on the
UDTF (directly or via `DataModelQueryRewriter`).

Databricks documents general plan inspection in
[EXPLAIN](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-explain).
This page adapts that workflow to **UDTF-backed Cognite views**.

Related: [Filtering](./filtering.md), pygen-spark [#68](https://github.com/cognitedata/pygen-spark/issues/68)
(aggregates), [#69](https://github.com/cognitedata/pygen-spark/issues/69) (WHERE / LIMIT).

## What EXPLAIN can and cannot tell you

| Tool | Shows | Does **not** show |
|------|--------|-------------------|
| `EXPLAIN` / `EXPLAIN EXTENDED` / `EXPLAIN FORMATTED` | Spark logical/physical plan (Filter, Limit, PythonUDTF / view) | CDF HTTP body (`filter`, `limit`, aggregate vs list) |
| Query Profile → Driver logs | `[UDTF]` stderr from generated code | Always-complete REST payloads |
| Rewritten SQL / bound UDTF call | Named args (`_row_limit`, `_exists`, …) | Live CDF latency alone |

Treat EXPLAIN as a **Spark-side** signal: if `Filter` / `Limit` sit **above** a UDTF
invocation with all-null defaults, CDF may still see a broad `instances/list`.

## EXPLAIN syntax (Databricks SQL)

Same forms as the Databricks language manual:

```sql
EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement
```

| Form | Use for UDTF-backed views |
|------|---------------------------|
| `EXPLAIN` | Default physical plan — enough to spot Filter/Limit above the UDTF |
| `EXPLAIN EXTENDED` | Parsed → analyzed → optimized → physical; useful when relations look unresolved |
| `EXPLAIN FORMATTED` | Numbered nodes; easier to cite in a ticket (“node 3 Filter above node 1 UDTF”) |
| `EXPLAIN COST` | Only if stats exist; rarely decisive for CDF UDTFs |
| `EXPLAIN CODEGEN` | Rarely needed for pushdown diagnosis |

## Workflow

1. Reproduce the slow SQL against the **catalog view** (analyst path).
2. Run `EXPLAIN` / `EXPLAIN FORMATTED` on that statement.
3. Interpret whether filters/`LIMIT` are Spark post-ops or UDTF args (below).
4. If post-ops: rewrite with `DataModelQueryRewriter` or call the UDTF with bound params.
5. Re-run `EXPLAIN` on the rewritten SQL and compare.
6. Confirm at the HTTP / `[UDTF]` log layer if still unsure.

## Catalog examples (anchored)

Examples below use a catalog shaped like the sailboat demo registration. Replace names
with yours (e.g. `f0connectortest.sailboat_sailboat_v1`):

| Object | Example |
|--------|---------|
| Catalog | `f0connectortest` |
| Schema | `sailboat_sailboat_v1` |
| View (CDF external id) | `SmallBoat` |
| Registered UDTF | `small_boat_udtf` (snake_case + `_udtf`) |
| Secret scope | `cdf_sailboat_sailboat` (pattern `cdf_{space}_{external_id.lower()}`) |

LIMS-style examples use `LimsResults` / `lims_results_udtf` the same way.

### 1. Slow view query — start with EXPLAIN

```sql
EXPLAIN FORMATTED
SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name = 'XBOX'
LIMIT 10;
```

**Healthy pushdown story (goal):** the leaf is a UDTF (or view that expands to one)
whose arguments include `name => 'XBOX'` and `_row_limit => 10` (or equivalent), with
little or no redundant Filter/Limit above it.

**Typical default-view plan (problem):**

```text
== Physical Plan ==
* Limit (3)
+- * Filter (2)   -- Spark applies WHERE after fetch
   +- * ...Scan / PythonUDTF (1)
      -- UDTF args: name => NULL, _row_limit => NULL, ...
```

That pattern matches Unity Catalog views generated with all-`NULL` property params:
Spark filters locally; CDF may still page with a wide `instances/list`.

Also try:

```sql
EXPLAIN EXTENDED
SELECT count(*) FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name IS NOT NULL;
```

If you only see a Spark aggregate over a full UDTF scan, you are **not** on
`instances/aggregate` yet — see aggregates below.

### 2. Bind params with DataModelQueryRewriter

`DataModelQueryRewriter` is a **library helper** (not automatic in notebooks). It turns
simple catalog SQL into a UDTF call with pushdown args. Default FQN uses
`to_udtf_function_name` (`SmallBoat` → `small_boat_udtf`).

```python
from cognite.databricks import DataModelQueryRewriter

sql = """
SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name = 'XBOX'
LIMIT 10
"""
rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(
    sql,
    secret_scope="cdf_sailboat_sailboat",
)
print(rewritten)
# SELECT * FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
#     client_id => SECRET(...),
#     ...
#     name => 'XBOX',
#     _row_limit => 10
# )
```

Then explain the **rewritten** statement:

```sql
EXPLAIN FORMATTED
SELECT * FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
  client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project => SECRET('cdf_sailboat_sailboat', 'project'),
  name => 'XBOX',
  _row_limit => 10
);
```

Compare plans: bound args at the UDTF leaf vs Filter/Limit-only above null defaults.

### 3. Exists + LIMIT (LimsResults)

```sql
-- Analyst view (often Spark-only until rewritten)
EXPLAIN
SELECT * FROM adg_cdf_dev.gold.LimsResults
WHERE TestSeqNumber = '5889450'
  AND DilutionFactor IS NOT NULL
LIMIT 10;
```

```python
from cognite.databricks import DataModelQueryRewriter

rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(
    """
    SELECT * FROM adg_cdf_dev.gold.LimsResults
    WHERE TestSeqNumber = '5889450'
      AND DilutionFactor IS NOT NULL
    LIMIT 10
    """
)
# Expect lims_results_udtf(... TestSeqNumber => ..., _exists => ..., _row_limit => 10)
```

Expected CDF list filter fragments when bound:

```json
{
  "and": [
    {
      "equals": {
        "property": ["sp-lims", "LimsResults/v1", "TestSeqNumber"],
        "value": "5889450"
      }
    },
    {
      "exists": {
        "property": ["sp-lims", "LimsResults/v1", "DilutionFactor"]
      }
    }
  ]
}
```

List request `limit` should be capped by `_row_limit` (and adaptive page size), not a
full default page when only Spark `Limit` is present.

### 4. Aggregates — EXPLAIN vs instances/aggregate

```sql
EXPLAIN EXTENDED
SELECT count(*) FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE name IS NOT NULL;
```

**Spark-only smell:** `HashAggregate` / `Count` over a PythonUDTF/view scan with null
filter args.

**Pushed path:** rewrite or call UDTF with `_query_mode => 'aggregate'` and
`_aggregates => ...` (rewriter maps `count(*)` to `externalId`). Confirm with logs or
a capture of `POST .../models/instances/aggregate` — not with EXPLAIN alone.

Do **not** confuse:

| Limit | Meaning |
|-------|---------|
| SQL `LIMIT` on list | `_row_limit` → list API `limit` + early stop |
| Aggregate API `limit` | Caps **groupBy buckets**, not SQL row LIMIT |
| `ORDER BY ... LIMIT` | **Not** pushed (sort may differ from CDF) |

### 5. Query Profile and `[UDTF]` logs

1. Run the SQL in a warehouse or notebook.
2. Open **Query Profile** → **Driver** (and executors if needed).
3. Look for lines prefixed with `[UDTF]` (auth, request URL, errors).
4. For deeper payload checks, compare against `instances/list` or `instances/aggregate`
   JSON (`filter`, `limit`, `aggregates`).

## Side-by-side checklist

| Symptom | EXPLAIN / plan clue | Fix |
|---------|---------------------|-----|
| Selective `WHERE` still slow | Filter above UDTF; props `NULL` | Rewriter or bind equality / `_exists` / ranges |
| `LIMIT 10` still heavy | Limit above UDTF; `_row_limit` null | Bind `_row_limit` |
| `COUNT(*)` scans all rows | Spark aggregate over UDTF scan | `_query_mode='aggregate'` / rewriter |
| Hive metastore error on short name | Unqualified UDTF | Use `catalog.schema.fn` or `USE CATALOG` |
| Wrong function name | Plan shows `SmallBoat_udtf` | Use `small_boat_udtf` (`to_udtf_function_name`) |
| Array property via rewriter | Scalar `equals`/`in` in rewrite | Call UDTF with view metadata / `containsAny` path |

## Troubleshooting reminders

- Wrong property **casing** vs CDF view property id
- **Instance** `space` column ≠ view model space in property paths
- Unqualified names resolving to disabled Hive metastore
- Writing generated files to a non-writable `/Workspace/...` path — use
  `/local_disk0/tmp/pygen_udtf` for codegen (separate from query EXPLAIN)

## See also

- [Filtering](./filtering.md) — what pushes today
- [Querying](./querying.md)
- [Troubleshooting](./troubleshooting.md)
- Databricks SQL [EXPLAIN](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-explain)
