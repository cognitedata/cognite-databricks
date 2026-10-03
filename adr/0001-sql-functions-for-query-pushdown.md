# ADR 0001: SQL functions for query pushdown

- Status: Proposed
- Date: 2026-10-02
- Updated: 2026-10-03
- Issue: https://github.com/cognitedata/cognite-databricks/issues/88

## Context

A registered catalog view calls the Python UDTF with every property and every pushdown argument set to `NULL`. Spark applies `WHERE`, `LIMIT`, and aggregates after the UDTF returns. `generator.rewrite_query` can turn one analyst statement into a direct UDTF call with those arguments bound, but only Python code that calls `rewrite_query` gets that statement. Power BI and any other SQL warehouse client send SQL to the view. They never call `rewrite_query`.

A Python UDTF is opaque to the Spark optimizer, so the warehouse cannot move an outer predicate into the UDTF arguments. Unity Catalog Python functions also cannot declare defaults, so a direct UDTF call must pass every parameter.

The predicates `rewrite_query` can combine are already open: instance space, external id, property equality, exists, ranges, `LIMIT`, `count` / numeric `min` / `max`, and `GROUP BY` of those aggregates can appear together in one statement. A fixed menu of SQL functions cannot list those combinations, and it would miss the next combination a report needs.

## Decision

Add `generator.create_sql_function`. The caller passes one SQL statement written against a Unity Catalog view this generator registered. The statement is the query shape: which view, which predicates, whether the result is a list, a limit, or an aggregate. The method asks `rewrite_query` whether that shape can be pushed. When it can, the method creates a SQL table function whose arguments are the values that shape binds. When it cannot, the method raises and creates nothing.

The literals in the statement are examples. They prove the statement is valid and they set each argument's type. They are not stored in the function. A later call passes different values through the same function, so one function covers every lab and every component name that share that shape. A different shape, such as adding a range or switching from `count(*)` to a row list, is a new function.

The view and the Python UDTF stay as they are. The SQL function is an additional object in the same catalog and schema. Unity Catalog SQL functions accept parameters. The Python UDTF still receives every argument, because a Python UDTF cannot omit one. The SQL function fills the ones the caller did not vary with `NULL`, and it passes secrets itself.

Secrets are passed the same way as on the view: `SECRET(scope, key)` for `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project`, and `base_url`. They are not function arguments. The function is `SQL SECURITY DEFINER`. Callers do not pass credentials and do not need `READ` on the secret scope. The function owner keeps that `READ`.

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
4. Replaces each bound literal with a function argument. The sample value is discarded.
5. Runs `CREATE OR REPLACE FUNCTION catalog.schema.{name}(...)` whose body is the rewritten UDTF call, with those arguments passed through.

`CREATE OR REPLACE` matches view registration. Calling `create_sql_function` again with the same name replaces the signature and the body.

Argument order follows the rewriter:

| Bound value | SQL argument | Type |
|-------------|--------------|------|
| `space = '...'` | `space` | `STRING` |
| `space IN (...)` | `space` | `ARRAY<STRING>` |
| `external_id = '...'` | `external_id` | `STRING` |
| `external_id IN (...)` | `external_id` | `ARRAY<STRING>` |
| `column = '...'` | the view column name | the column's SQL type |
| `column IN (...)` | the view column name | `ARRAY<STRING>` |
| `column > n`, and `>=`, `<`, `<=` | `{column}_gt`, `{column}_gte`, `{column}_lt`, `{column}_lte` | the column's numeric SQL type |
| `LIMIT n` on a list | `row_limit` | `BIGINT` |

The argument name is the column written in the statement. `space` stays `space` on the SQL function. The body passes it to the UDTF parameter `instance_space`, which is the name the Python function already uses. Property and range arguments follow view column order. `IS NULL` and `IS NOT NULL` have no value, so they stay in the body as `_exists` and `_not_exists`. `count`, `min`, and `max` stay in the body as `_query_mode` and `_aggregates`, because they decide the return columns. `GROUP BY` columns stay in the body as `_group_by`. They are part of the query shape, so they are not function arguments unless the same column also has a literal in the `WHERE` clause. A range or an `IN` list is still one UDTF argument (`_gte`, or the column set to a JSON list). The SQL function builds that JSON from its typed arguments, so Power BI passes a string, a number, or an array.

`skip_reasons` is a failure even when some other part of the statement could be pushed. `ORDER BY` plus `LIMIT` is the case that matters: the rewriter records that `LIMIT` was not pushed. Creating a function that silently drops the limit would not be the statement the user wrote.

## What is accepted

A statement is accepted when `rewrite_query` returns SQL and the analysis has no skip reasons. Combinations are accepted when each part is. The count filtered by `space` and `componentName` is one function with those two arguments. The body sets `instance_space`, `componentName`, `_query_mode => 'aggregate'`, and `_aggregates`, and every other UDTF argument is `NULL`.

The same rules as `rewrite_query` apply:

- One view, no join.
- No `HAVING`, no `OFFSET`, no `COUNT(DISTINCT)`.
- `LIMIT` without `ORDER BY`.
- `count(*)`, and `min` / `max` of a numeric column.
- `min` / `max` of a text column is rejected.
- Columns that are not on the view are rejected. `space` and `external_id` are identity columns, so they are allowed in `GROUP BY`.
- A statement with nothing to push, such as `SELECT *` from the view with no filter and no limit, is rejected. That would only wrap the full scan.

## GROUP BY

`rewrite_query` pushes `GROUP BY` when the select list is exactly the grouped columns plus `count`, `min`, or `max`. The grouped columns come back in the order the analyst wrote them. The metric columns are the same aliases as an ungrouped aggregate: `external_id AS count_externalId`, or `{column} AS min_{column}` / `max_{column}`.

A `WHERE` clause on that statement still pushes. These filters combine with `GROUP BY` the same way they combine with an ungrouped count:

- `space` equals and `space IN`
- `external_id` equals and `external_id IN`
- property equals and property `IN`
- `IS NULL` and `IS NOT NULL`
- numeric `>`, `>=`, `<`, and `<=`
- any `AND` of those

```sql
SELECT componentName, count(*) AS n
FROM f0connectortest.dm_dom_lims_result_limsresult_dom_v1.LimsResult
WHERE space = 'inst_lims_result_lab_a'
GROUP BY componentName
```

That statement is one function. `space` is the only argument, because it is the only literal. `componentName` is fixed in `_group_by`. The body still passes every other UDTF argument as `NULL`.

```sql
CREATE OR REPLACE FUNCTION f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_result_count_by_component(
  space STRING
)
RETURNS TABLE (componentName STRING, count_externalId STRING)
SQL SECURITY DEFINER
RETURN
SELECT componentName, external_id AS count_externalId
FROM f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_result_udtf(
  client_id => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'client_id'),
  client_secret => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'client_secret'),
  tenant_id => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'tenant_id'),
  cdf_cluster => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'cdf_cluster'),
  project => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'project'),
  instance_space => space,
  _query_mode => 'aggregate',
  _aggregates => '[{"fn": "count", "property": "externalId"}]',
  _group_by => '["componentName"]',
  base_url => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'base_url')
);
```

`_group_by` uses CDF property names. A reserved SQL column such as `class_` is sent as `class`. `GROUP BY space` is sent as `space`, and the UDTF writes that key into the `space` output column. `GROUP BY external_id` is sent as `externalId`.

The same column can be both a filter and a group key. `WHERE componentName = 'MOISTURE_CONTENT' GROUP BY componentName` binds `componentName` as an argument and still sets `_group_by`.

These grouped statements are rejected, and no function is created:

- The select list has a column that is not grouped and not aggregated, or a `GROUP BY` column is missing from the select list.
- `GROUP BY` uses an expression, such as `lower(name)`.
- The group column and the metric share one output column. `count(*)` already occupies `external_id`, so `GROUP BY external_id` with `count(*)` is rejected. `max(numericValue)` occupies `numericValue`, so grouping by `numericValue` with that max is rejected.
- CDF returns at most 1000 groups. The UDTF sends one `instances/aggregate` request with `limit` 1000 and does not request another page.

## Created function

```sql
CREATE OR REPLACE FUNCTION f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a(
  space STRING,
  componentName STRING
)
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
  componentName => componentName,
  instance_space => space,
  _query_mode => 'aggregate',
  _aggregates => '[{"fn": "count", "property": "externalId"}]',
  base_url => SECRET('cdf_dm_dom_lims_result_limsresult_dom', 'base_url')
);
```

Every UDTF parameter is present. Parameters the statement does not set are `NULL`. Arguments the statement does set are the function's own arguments, not the sample literals. The return columns are the select list `rewrite_query` produces: `*` for a list or limit, `count_externalId` for a count, `min_{column}` / `max_{column}` for a numeric aggregate, and the grouped columns in front of those metrics when the statement has `GROUP BY`.

## Unity Catalog

The function is listed under Functions in the view's schema, next to the Python UDTF. It does not replace the view.

| Name | Kind | Language |
|------|------|----------|
| `LimsResult` | View | |
| `lims_result_udtf` | Function | Python |
| `lims_results_moisture_in_lab_a` | Function | SQL, arguments for each bound value |

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
FROM f0connectortest.dm_dom_lims_result_limsresult_dom_v1.lims_results_moisture_in_lab_a(
  space => 'inst_lims_result_lab_a',
  componentName => 'MOISTURE_CONTENT'
)
```

Power BI binds those arguments with Dynamic M query parameters, so a slicer changes the call. It does not change the function, and it does not query the view. A second shape, such as the same filters with `LIMIT` or with a numeric range, is a second function. A visual that then filters the returned rows does that in Power BI, on the rows the function already pushed down.

## Errors

`QueryNotPushdownCompatible` is raised, and no function is created, when:

- The `FROM` clause is not a three-part view name, or the view is not in this data model.
- The statement uses a pattern `rewrite_query` does not rewrite.
- The analysis records a skip reason, including `ORDER BY` plus `LIMIT`.
- `GROUP BY` does not match the select list, uses an expression, or collides with the metric column.
- `rewrite_query` returns `None` because nothing would be pushed.

The message quotes the statement and the skip reasons. A later compatible call can reuse the same function name and replace the body.

## What this does not do

- It does not intercept SQL that Power BI generates against the view. Power BI has to call the function and pass its arguments.
- It does not turn the query shape into arguments. Which columns are filtered, and whether the function lists rows or returns `count(*)`, are fixed when the function is created.
- It does not push joins, `HAVING`, `OFFSET`, `COUNT(DISTINCT)`, `GROUP BY` expressions, or `MIN` / `MAX` on text.
- It does not page grouped aggregates. A grouped function returns at most 1000 groups.
- It does not change page size or OAuth. A pushed function is one CDF request. The view scan remains many `instances/list` pages.

Transparent folding for every slicer remains the optimizer-aware table provider in issue 88. `create_sql_function` is how a known statement becomes a Unity Catalog function that already contains the pushdown.

## Consequences

- Tests feed `create_sql_function` the same statements as `rewrite_query`, and assert the function arguments replace the sample literals while `SECRET()` still supplies the credential arguments and `base_url`.
- Incompatible statements are asserted to raise and to leave the catalog unchanged.
- A function created from a count of one lab and one property value is the combination case. It must bind both filters and the aggregate, and it must not be expressed as two separate function types.
- A function created from `GROUP BY componentName` with a `space` filter has `space` as its only argument, returns `componentName` and `count_externalId`, and sets `_group_by`. The same statement with an added property filter, range, `IN`, or `IS NULL` still pushes that filter. A grouped statement whose select list does not match, or whose group column collides with the metric, raises and creates nothing.
- Docs that tell analysts to filter the view need this second path: the view when a full read is acceptable, a SQL function when a specific statement must reach CDF.
