# ADR 0001: SQL functions for query pushdown

- Status: Proposed
- Date: 2026-10-02
- Issue: https://github.com/cognitedata/cognite-databricks/issues/88

## Context

A registered catalog view calls the Python UDTF with every property and every pushdown argument set to `NULL`. Spark applies `WHERE`, `LIMIT`, and aggregates after the UDTF returns. `generator.rewrite_query` can turn one analyst statement into a direct UDTF call with those arguments bound, but only Python code that calls `rewrite_query` gets that statement. Power BI and any other SQL warehouse client send SQL to the view. They never call `rewrite_query`.

A Python UDTF is opaque to the Spark optimizer, so the warehouse cannot move an outer predicate into the UDTF arguments. Unity Catalog Python functions also cannot declare defaults, so a direct UDTF call must pass every parameter.

The predicates `rewrite_query` can combine are already open: instance space, external id, property equality, exists, ranges, `LIMIT`, and `count` / numeric `min` / `max` can appear together in one statement. A fixed menu of SQL functions cannot list those combinations, and it would miss the next combination a report needs.

## Decision

Add `generator.create_sql_function`. The caller passes the SQL statement they want to be fast, written against a Unity Catalog view this generator registered. The method asks `rewrite_query` whether that statement can be pushed. When it can, the method creates a zero-argument SQL table function whose body is the rewritten UDTF call. When it cannot, the method raises and creates nothing.

The view and the Python UDTF stay as they are. The SQL function is an additional object in the same catalog and schema. Values in the statement, such as `TestSeqNumber = '5089450'` or `LIMIT 2`, are constants in the function body. Power BI calls the function with no arguments. A slicer on the view does not change those constants. Publishing a different filter is a new call to `create_sql_function`.

Secrets are passed the same way as on the view: `SECRET(scope, key)` for `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project`, and `base_url`. The function is `SQL SECURITY DEFINER`. Callers do not pass credentials and do not need `READ` on the secret scope. The function owner keeps that `READ`.

There is no catalog of function types. Any statement `rewrite_query` accepts is a legal function. A statement it rejects is an error.

## API

```python
created = generator.create_sql_function(
    "lims_results_moisture_in_lab_a",
    f"""
    SELECT count(*) AS n
    FROM {catalog}.{schema}.LimsResult
    WHERE space = 'inst_lims_result_lab_a'
      AND componentName = 'MOISTURE_CONTENT'
    """,
    secret_scope="cdf_dm_dom_lims_result_limsresult_dom",
)
print(created.full_name)
```

`name` is the function name inside the view's schema. `sql` must be a single statement whose `FROM` clause is `catalog.schema.view` for a view in this generator's data model. `secret_scope` defaults to the scope `register_views` uses.

The method:

1. Resolves the view from the `FROM` clause and loads its column metadata.
2. Runs `DataModelQueryRewriter.analyze` with that metadata.
3. Raises `QueryNotPushdownCompatible` when `pushdown_supported` is false, when `skip_reasons` is not empty, or when `rewrite_query` returns `None`. The exception message includes the reasons. No function is created.
4. Otherwise runs `CREATE OR REPLACE FUNCTION catalog.schema.{name}()` with the rewritten statement as the body.

`CREATE OR REPLACE` matches view registration. Calling `create_sql_function` again with the same name replaces the body.

`skip_reasons` is a failure even when some other part of the statement could be pushed. `ORDER BY` plus `LIMIT` is the case that matters: the rewriter records that `LIMIT` was not pushed. Creating a function that silently drops the limit would not be the statement the user wrote.

## What is accepted

A statement is accepted when `rewrite_query` returns SQL and the analysis has no skip reasons. Combinations are accepted when each part is. For example, a count filtered by instance space and a property equality is one function: `instance_space`, the property argument, `_query_mode => 'aggregate'`, and `_aggregates` are all set, and every other argument is `NULL`.

The same rules as `rewrite_query` apply:

- One view, no join.
- No `HAVING`, no `OFFSET`, no `COUNT(DISTINCT)`.
- `LIMIT` without `ORDER BY`.
- `count(*)`, and `min` / `max` of a numeric column. A select list that also contains a plain column, including `GROUP BY`, is rejected.
- `min` / `max` of a text column is rejected.
- Columns that are not on the view are rejected.
- A statement with nothing to push, such as `SELECT *` from the view with no filter and no limit, is rejected. That would only wrap the full scan.

## Created function

```sql
CREATE OR REPLACE FUNCTION f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a()
RETURNS TABLE (count_externalId STRING)
SQL SECURITY DEFINER
RETURN
SELECT external_id AS count_externalId
FROM f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_result_udtf(
  client_id => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'client_id'),
  client_secret => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'client_secret'),
  tenant_id => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'tenant_id'),
  cdf_cluster => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'cdf_cluster'),
  project => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'project'),
  componentName => 'MOISTURE_CONTENT',
  instance_space => 'inst_lims_result_lab_a',
  _query_mode => 'aggregate',
  _aggregates => '[{"fn": "count", "property": "externalId"}]',
  base_url => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'base_url')
);
```

Every UDTF parameter is present. Parameters the statement does not set are `NULL`. The return columns are the select list `rewrite_query` produces: `*` for a list or limit, `count_externalId` for a count, and `min_{column}` / `max_{column}` for a numeric aggregate.

## Unity Catalog

The function is listed under Functions in the view's schema, next to the Python UDTF. It does not replace the view.

| Name | Kind | Language |
|------|------|----------|
| `LimsResult` | View | |
| `lims_result_udtf` | Function | Python |
| `lims_results_moisture_in_lab_a` | Function | SQL, no arguments |

```sql
SHOW FUNCTIONS IN f0connectortest.dm_dom_lims_result_limsresult_dom_v1;
DESCRIBE FUNCTION EXTENDED f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a;
```

## Grants

The principal that calls `create_sql_function` needs `CREATE FUNCTION` on the schema and `READ` on the secret scope.

```sql
GRANT EXECUTE ON FUNCTION f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a
  TO `account users`;
```

Replace `account users` with the group that owns the Power BI connection. Do not grant that group `READ` on the secret scope. Confirm on the warehouse whether definer rights are enough for the inner Python UDTF. If the caller also needs `EXECUTE` on `lims_result_udtf`, grant that, and still do not grant secret `READ`.

## How Power BI calls it

The dataset query is the function, not the view:

```sql
SELECT *
FROM f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a()
```

There is no parameter to bind. The filter was fixed when the function was created. A report that needs a second combination, or a second literal, gets a second function. A visual that then filters or counts the returned rows does that in Power BI, on the rows the function already pushed down.

## Errors

`QueryNotPushdownCompatible` is raised, and no function is created, when:

- The `FROM` clause is not a three-part view name, or the view is not in this data model.
- The statement uses a pattern `rewrite_query` does not rewrite.
- The analysis records a skip reason, including `ORDER BY` plus `LIMIT`.
- `rewrite_query` returns `None` because nothing would be pushed.

The message quotes the statement and the skip reasons. A later compatible call can reuse the same function name and replace the body.

## What this does not do

- It does not intercept SQL that Power BI generates against the view.
- It does not take parameters. Changing a literal means creating or replacing the function.
- It does not push joins, `HAVING`, `OFFSET`, `COUNT(DISTINCT)`, `GROUP BY`, or `MIN` / `MAX` on text.
- It does not change page size or OAuth. A pushed function is one CDF request. The view scan remains many `instances/list` pages.

Transparent folding for every slicer remains the optimizer-aware table provider in issue 88. `create_sql_function` is how a known statement becomes a Unity Catalog function that already contains the pushdown.

## Consequences

- Tests feed `create_sql_function` the same statements as `rewrite_query`, and assert the function body is that SQL plus `SECRET()` for the credential arguments and `base_url`.
- Incompatible statements are asserted to raise and to leave the catalog unchanged.
- A function created from a count of one lab and one property value is the combination case. It must bind both filters and the aggregate, and it must not be expressed as two separate function types.
- Docs that tell analysts to filter the view need this second path: the view when a full read is acceptable, a SQL function when a specific statement must reach CDF.
