# ADR 0001: SQL functions for query pushdown

- Status: Proposed
- Date: 2026-10-02
- Issue: https://github.com/cognitedata/cognite-databricks/issues/88

## Context

A registered catalog view calls the Python UDTF with every property and every pushdown argument set to `NULL`. Spark applies `WHERE`, `LIMIT`, and aggregates after the UDTF returns. `generator.rewrite_query` can turn one analyst statement into a direct UDTF call with those arguments bound, but only Python code that calls `rewrite_query` gets that statement. Power BI and any other SQL warehouse client send SQL to the view. They never call `rewrite_query`.

A Python UDTF is opaque to the Spark optimizer, so the warehouse cannot move an outer predicate into the UDTF arguments. Unity Catalog Python functions also cannot declare defaults, so a direct UDTF call must pass every parameter.

The view stays. It is the object for `SELECT *` and for clients that do not bind arguments. Pushdown for a named access pattern is a separate Unity Catalog object: a SQL table function in the same schema. The function body calls the Python UDTF with the pattern's arguments set and every other argument `NULL`. Credentials and `base_url` stay `SECRET()` references, as they do on the view.

## Decision

Ship SQL table functions in addition to the view and the Python UDTF. Each function is one access pattern that 0.4.1 can already push to CDF. A user chooses which patterns to register. The package does not generate a function for every column and every operator.

The function is created with `CREATE OR REPLACE FUNCTION`, language SQL, `RETURNS TABLE`. The body is the UDTF call `rewrite_query` would have produced. Callers receive `EXECUTE` on the SQL function. They do not pass credentials.

`GROUP BY` is not a function type. The rewriter leaves a select list that mixes a plain column with an aggregate in Spark. `MIN` and `MAX` on a text column are not a function type. CDF accepts `MIN` and `MAX` only on numeric properties.

## Function types

Names use the same snake_case stem as the Python UDTF, without the `_udtf` suffix. For view `SmallBoat` the stem is `small_boat`. For view `LimsResult` the stem is `lims_result`.

| Type | Function | Arguments | Bound UDTF arguments |
|------|----------|-----------|----------------------|
| Property equals | `{stem}_by_{column}` | `value` | that column `=> value` |
| Property IN | `{stem}_by_{column}_in` | `values ARRAY<STRING>` | that column `=>` the JSON list |
| Instance space equals | `{stem}_by_space` | `instance_space STRING` | `instance_space` |
| Instance space IN | `{stem}_by_space_in` | `instance_spaces ARRAY<STRING>` | `instance_space` as a JSON list |
| External id equals | `{stem}_by_external_id` | `external_id STRING` | `external_id` |
| External id IN | `{stem}_by_external_id_in` | `external_ids ARRAY<STRING>` | `external_id` as a JSON list |
| Exists | `{stem}_where_{column}_is_not_null` | none | `_exists => '["{column}"]'` |
| Not exists | `{stem}_where_{column}_is_null` | none | `_not_exists => '["{column}"]'` |
| Range above | `{stem}_where_{column}_gt` | `bound` | `_gt` JSON map. Column must be numeric. |
| Range at or above | `{stem}_where_{column}_gte` | `bound` | `_gte` |
| Range below | `{stem}_where_{column}_lt` | `bound` | `_lt` |
| Range at or below | `{stem}_where_{column}_lte` | `bound` | `_lte` |
| Range between | `{stem}_where_{column}_between` | `low`, `high` | `_gte` and `_lte` |
| Row limit | `{stem}_limit` | `row_limit BIGINT`, optional `instance_space STRING` | `_row_limit`, and `instance_space` when passed |
| Count | `{stem}_count` | none | `_query_mode => 'aggregate'`, `_aggregates` count of `externalId` |
| Count with equals | `{stem}_count_by_{column}` | `value` | the column plus the count aggregate |
| Count with space | `{stem}_count_by_space` | `instance_space STRING` | `instance_space` plus the count aggregate |
| Numeric min | `{stem}_min_{column}` | optional `instance_space STRING` | `_aggregates` `min` on that column |
| Numeric max | `{stem}_max_{column}` | optional `instance_space STRING` | `_aggregates` `max` on that column |

List functions return the UDTF row, `SELECT *`. Aggregate functions return the one metric column the rewriter already uses: `external_id AS count_externalId`, or `{column} AS min_{column}` / `max_{column}`.

A range or min/max function is rejected at registration when the column is not numeric. An equals function is rejected when the column is not on the view.

Optional `instance_space` on limit, min, and max uses a SQL default of `NULL`. When the caller omits it, the UDTF argument is `NULL` and CDF does not filter by space. Databricks SQL table functions support this default. The Python UDTF still receives the argument, because the SQL function passes it explicitly.

## Commands

Registration of the view and the Python UDTF does not change. Run that first. Then register the SQL functions.

```python
from cognite.databricks.query_functions import (
    BetweenFunction,
    CountByColumnFunction,
    CountBySpaceFunction,
    CountFunction,
    EqualsFunction,
    ExistsFunction,
    ExternalIdFunction,
    ExternalIdInFunction,
    InListFunction,
    InstanceSpaceFunction,
    InstanceSpaceInFunction,
    LimitFunction,
    NotExistsFunction,
    NumericMaxFunction,
    NumericMinFunction,
    RangeFunction,
)

registered = generator.register_query_functions(
    secret_scope="cdf_sailboat_sailboat",
    functions=[
        EqualsFunction(column="name"),
        InListFunction(column="name"),
        InstanceSpaceFunction(),
        InstanceSpaceInFunction(),
        ExternalIdFunction(),
        ExternalIdInFunction(),
        ExistsFunction(column="description"),
        NotExistsFunction(column="description"),
        RangeFunction(column="aph_tod", operator="gt"),
        BetweenFunction(column="aph_tod"),
        LimitFunction(),
        CountFunction(),
        CountByColumnFunction(column="name"),
        CountBySpaceFunction(),
        NumericMinFunction(column="aph_tod"),
        NumericMaxFunction(column="aph_tod"),
    ],
)
print([item.full_name for item in registered.functions])
```

`register_query_functions` issues one `CREATE OR REPLACE FUNCTION` per entry, then returns the full names. Replace is the same rule as view registration: the function body is updated in place.

The statement for `small_boat_by_name` looks like this. Every UDTF parameter is present. The comment where columns are omitted stands for the other view columns, each passed as `NULL`, in view order, then any unbound pushdown arguments as `NULL`.

```sql
CREATE OR REPLACE FUNCTION f0connectortest.sailboat_sailboat_v1.small_boat_by_name(
  value STRING
)
RETURNS TABLE
SQL SECURITY DEFINER
RETURN
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
  client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project => SECRET('cdf_sailboat_sailboat', 'project'),
  name => value,
  description => NULL,
  instance_space => NULL,
  external_id => NULL,
  _exists => NULL,
  _not_exists => NULL,
  _gt => NULL,
  _gte => NULL,
  _lt => NULL,
  _lte => NULL,
  _row_limit => NULL,
  _query_mode => NULL,
  _aggregates => NULL,
  _group_by => NULL,
  base_url => SECRET('cdf_sailboat_sailboat', 'base_url')
);
```

A count function selects the aggregate column instead of `*`:

```sql
CREATE OR REPLACE FUNCTION f0connectortest.sailboat_sailboat_v1.small_boat_count()
RETURNS TABLE (count_externalId STRING)
SQL SECURITY DEFINER
RETURN
SELECT external_id AS count_externalId
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
  client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project => SECRET('cdf_sailboat_sailboat', 'project'),
  name => NULL,
  _query_mode => 'aggregate',
  _aggregates => '[{"fn": "count", "property": "externalId"}]',
  base_url => SECRET('cdf_sailboat_sailboat', 'base_url')
);
```

The count statement above is the shape, not a statement to paste onto a wide view. `small_boat_udtf` has more view columns than `name`. Each of those columns is still an argument and is passed as `NULL`. `DESCRIBE FUNCTION EXTENDED` on the Python UDTF is the parameter order to copy. `register_query_functions` emits the full argument list so a hand-written statement is only needed before that API exists.

Users who need one function before the Python API exists can run `CREATE OR REPLACE FUNCTION` in the SQL warehouse. `SHOW CREATE TABLE` is the wrong command for a function.

## How they appear in Unity Catalog

Same schema as the view and the Python UDTF. Three different kinds:

| Name | Catalog kind | Language |
|------|----------------|----------|
| `SmallBoat` | View | |
| `small_boat_udtf` | Function | Python |
| `small_boat_by_name`, `small_boat_count`, and the rest | Function | SQL |

```sql
DESCRIBE FUNCTION EXTENDED f0connectortest.sailboat_sailboat_v1.small_boat_by_name;
SHOW FUNCTIONS IN f0connectortest.sailboat_sailboat_v1;
SHOW VIEWS IN f0connectortest.sailboat_sailboat_v1;
```

Catalog Explorer shows the SQL functions under the schema's Functions folder. They are not columns on the view. A client that selects `SmallBoat` from the table browser still runs the view.

## Grants

The principal that runs `register_query_functions` needs `CREATE FUNCTION` on the schema and `READ` on the secret scope. The function is `SQL SECURITY DEFINER`, so a later caller does not pass secrets and does not need `READ` on the scope. The function owner keeps that `READ`.

```sql
GRANT EXECUTE ON FUNCTION f0connectortest.sailboat_sailboat_v1.small_boat_by_name
  TO `account users`;
GRANT EXECUTE ON FUNCTION f0connectortest.sailboat_sailboat_v1.small_boat_count
  TO `account users`;
```

Replace `account users` with the group that owns the Power BI connection. `EXECUTE` on the SQL function does not grant `SELECT` on the view. Confirm on the warehouse whether the definer's rights are enough for the inner call to `small_boat_udtf`. If the warehouse still requires the caller to have `EXECUTE` on `small_boat_udtf`, grant that too. Do not grant `READ` on the secret scope to the Power BI principal.

## Queries

From the SQL warehouse:

```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_by_name('Seed Fleet A 01');

SELECT count_externalId
FROM f0connectortest.sailboat_sailboat_v1.small_boat_count();

SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_limit(2, 'inst_sailboat_fleet_a');
```

From Power BI, the dataset query is that function call, not the view. The argument is a Power Query parameter bound with Dynamic M query parameters:

```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.small_boat_by_name('<name parameter>')
```

A slicer on the view does not call the function. A visual that counts rows returned by `small_boat_by_name` counts those rows in Power BI. A count that must run in CDF uses `small_boat_count` or `small_boat_count_by_name`.

## What this does not do

- It does not rewrite arbitrary SQL that a Power BI dataset generates against the view.
- It does not push a filter on a column that has no function registered.
- It does not push joins, `HAVING`, `OFFSET`, `COUNT(DISTINCT)`, or `GROUP BY`.
- It does not change page size or OAuth. A pushed equals or count is one CDF request. A view scan remains many `instances/list` pages.

Transparent folding for every slicer remains the optimizer-aware table provider described in issue 88. These functions are the zero-copy path that works with the UDTF that 0.4.1 already registers.

## Consequences

- Docs that tell analysts to filter the view need a second path: filter the view when a full read is acceptable, call a SQL function when the predicate must reach CDF.
- `register_query_functions` belongs next to `register_views`. Both take the secret scope and both use `CREATE OR REPLACE`.
- Tests should compare each function body with the SQL `rewrite_query` returns for the same predicate, and reject a numeric aggregate on a text column.
- Power BI acceptance is one native query per function, with the parameter changed, and a query profile that shows a bound UDTF argument rather than a filter above an all-`NULL` view call.
