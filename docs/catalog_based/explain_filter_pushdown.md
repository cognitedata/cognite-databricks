# EXPLAIN and verifying filter / LIMIT pushdown

Use this page when a catalog view query feels like a full CDF scan: slow `WHERE`,
`LIMIT` that does not reduce traffic, or aggregates that still page through `instances/list`.

Related: list/WHERE/LIMIT pushdown ([pygen-spark #69](https://github.com/cognitedata/pygen-spark/issues/69)),
metric aggregates ([pygen-spark #68](https://github.com/cognitedata/pygen-spark/issues/68)).

## When to use EXPLAIN

- Suspected full scan despite a selective `WHERE`
- `LIMIT 1` / `LIMIT 10` still slow (first HTTP page may still be large)
- `COUNT(*)` / `MIN` / `MAX` appear to fetch every instance
- Uncertainty whether Unity Catalog view defaults (`prop => NULL`) prevented binding

## Run EXPLAIN

In a Databricks SQL warehouse or notebook:

```sql
EXPLAIN
SELECT * FROM catalog.schema.LimsResults
WHERE TestSeqNumber = '5889450'
  AND DilutionFactor IS NOT NULL
LIMIT 10;
```

Optional:

```sql
EXPLAIN EXTENDED
SELECT count(*) FROM catalog.schema.LimsResults
WHERE DilutionFactor IS NOT NULL;
```

## What to look for in the plan

1. **Filter / Limit above the UDTF or view scan**  
   If `Filter` and `Limit` sit above a UDTF/view with all-null defaults, Spark is post-filtering.
   CDF may still receive a broad `instances/list` call.
   Rewrite with `DataModelQueryRewriter` (library call — not automatic in notebooks) or bind
   UDTF args so the plan shows those parameters. Default FQN uses snake_case + `_udtf`
   (e.g. `LimsResults` → `lims_results_udtf`).

2. **Evidence of bound UDTF parameters**  
   Prefer plans (or rewritten SQL) where equality / `_exists` / `_row_limit` / `_query_mode`
   appear as UDTF arguments rather than only Spark operators.

3. **Partial pushdown**  
   Equality may bind while `IS NOT NULL` and `LIMIT` remain Spark-only unless you use
   `DataModelQueryRewriter` or call the UDTF with explicit params.

## Confirm at the HTTP layer

Generated UDTFs log with a `[UDTF]` stderr prefix. In a notebook, capture driver/worker logs
or compare against a minimal `requests` call to:

- `POST /api/v1/projects/{project}/models/instances/list`
- `POST /api/v1/projects/{project}/models/instances/aggregate`

Check the JSON body for:

- `filter` — `equals`, `exists`, `range`, identity `["node","space"]`, etc.
- `limit` — for list scans should be `min(SQL LIMIT, page_limit)` when `_row_limit` is set
- Aggregate path — top-level `view` (not `sources`), `aggregates` array

## Worked examples

### Equality that should push (when bound)

```sql
SELECT * FROM catalog.schema.LimsResults
WHERE TestSeqNumber = '5889450';
```

Expected CDF fragment:

```json
{
  "equals": {
    "property": ["sp-lims", "LimsResults/v1", "TestSeqNumber"],
    "value": "5889450"
  }
}
```

### Instance space (not view space)

```sql
SELECT * FROM catalog.schema.LimsResults
WHERE space = 'sp-lims-instances';
```

Expected CDF fragment (nodes):

```json
{
  "equals": {
    "property": ["node", "space"],
    "value": "sp-lims-instances"
  }
}
```

### Exists + LIMIT (requires rewriter or explicit params)

```sql
SELECT * FROM catalog.schema.LimsResults
WHERE DilutionFactor IS NOT NULL
LIMIT 5;
```

Without rewrite, Spark applies both after fetch. With rewrite, expect `exists` in `filter`
and list `"limit": 5`.

### Aggregates (issue #68)

```sql
SELECT count(*) FROM catalog.schema.LimsResults
WHERE DilutionFactor IS NOT NULL;
```

Expect `instances/aggregate` with `count.property = externalId` and the same `exists` filter.
Do **not** confuse aggregate `limit` (groupBy buckets) with SQL `LIMIT` on list scans.

## Troubleshooting checklist

- Wrong property name / casing vs CDF view property id
- Confusing **view model space** with **instance space**
- Confusing aggregate `limit` (#68) vs list page `limit` vs SQL `LIMIT` (#69)
- `ORDER BY ... LIMIT` — row-limit pushdown is disabled on purpose
- Catalog view still using all-`NULL` UDTF defaults — use rewriter or call UDTF directly

## See also

- [Filtering](./filtering.md)
- [Querying](./querying.md)
