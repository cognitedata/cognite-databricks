# SQL functions for Power BI

A Unity Catalog view is the right place to write the query. It is the wrong place for Power BI to run that query when the view is large. `generator.create_sql_function` turns the slow statement into a SQL function that pushes the filters to CDF.

## 1. Write the query against the view

Register the data model, then query the view until the result is the one the report needs:

```sql
SELECT count(*) AS n
FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
WHERE space = 'inst_sailboat_fleet_a'
  AND name = 'XBOX'
```

That statement is valid SQL. Power BI can send it to the SQL warehouse, and the warehouse runs it against the view.

The view calls the Python UDTF with every filter argument set to `NULL`. Spark applies `WHERE`, `LIMIT`, and aggregates after the UDTF has returned rows. On a large view, Power BI waits while CDF pages through the instances. `COUNT(*)` on the view does the same full read.

`generator.rewrite_query` can bind those filters, and a notebook can run the rewritten statement. Power BI never calls `rewrite_query`. It only sends SQL. In a notebook, [rewrite_query](./rewrite_query.md) runs that statement with the filters pushed to CDF.

Use the view to decide the query. Use a SQL function to run that query from Power BI.

## 2. Create a SQL function from that query

Pass the slow statement to `create_sql_function`. The literals in the statement are sample values. They become the function arguments.

```python
created = generator.create_sql_function(
    "boats_named",
    """
    SELECT count(*) AS n
    FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
    WHERE space = 'inst_sailboat_fleet_a'
      AND name = 'XBOX'
    """,
    secret_scope="cdf_sailboat_sailboat",
)
print(created.full_name)
print([(arg.name, arg.sql_type) for arg in created.arguments])
```

`generator` is the same object that registered the view. The statement must use a view from that generator's data model, in that generator's catalog and schema.

On success the method creates this function in Unity Catalog:

```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.boats_named(
  space => 'inst_sailboat_fleet_a',
  name => 'XBOX'
)
```

What the method does:

1. Checks that the statement can be pushed. The same patterns as [Filtering](./filtering.md) apply, including combinations and `GROUP BY`.
2. Replaces each literal with an argument. `space = 'inst_sailboat_fleet_a'` becomes `space STRING`. `name = 'XBOX'` becomes `name STRING`. The sample values are not stored in the function.
3. Keeps CDF credentials and `base_url` as `SECRET()` references, with `SQL SECURITY DEFINER`, the same way the view does.
4. Runs `CREATE OR REPLACE FUNCTION` on the SQL warehouse.

Call it from the notebook to confirm the result before handing it to Power BI:

```python
spark.sql(f"""
SELECT *
FROM {created.full_name}(
  space => 'inst_sailboat_fleet_b',
  name => 'XBOX'
)
""").show()
```

A different argument value is a different CDF filter. The function definition stays as it was created.

If the statement cannot be pushed, `create_sql_function` raises `QueryNotPushdownCompatible` and creates nothing. Typical reasons:

- a join, `HAVING`, `OFFSET`, or `COUNT(DISTINCT)`
- `ORDER BY`
- `MIN` or `MAX` on a text or timestamp column
- `GROUP BY` columns that are not selected, or a selected column that is neither grouped nor aggregated
- a column the view does not have
- a view outside this data model, or a catalog and schema that do not match the generator

`IS NULL` and `IS NOT NULL` have no value to pass in, so they stay in the function body. `GROUP BY` columns stay in the body too, unless that column also has a literal in `WHERE`.

## 3. Call the SQL function from Power BI

Point the Power BI dataset at the function. A native SQL query is enough:

```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.boats_named(
  space => 'inst_sailboat_fleet_a',
  name => 'XBOX'
)
```

The count comes back in `count_externalId` as `STRING`. The UDTF stores the count in the string `external_id` column, so the function keeps that type. Cast the column in Power BI or SQL when you need a number. A numeric `MIN` or `MAX` comes back in `min_<column>` or `max_<column>` with the property's numeric type. A list query comes back as the view row (`space`, `external_id`, the properties, and the CDF timestamps).

Anything outside that grammar is not turned into a function. `OR`, subqueries, quoted identifiers, joins, `HAVING`, `OFFSET`, `COUNT(DISTINCT)`, and a `GROUP BY` that does not match the select list raise `QueryNotPushdownCompatible` and create nothing. The view query is unchanged, and Power BI keeps scanning it until you simplify the statement and create the function again. `rewrite_query` returns `None` for the same statements so a notebook can run the original SQL.

When a slicer should change a filter, bind that slicer to the function argument with a Dynamic M query parameter. Changing the slicer changes the argument sent to CDF. The function is unchanged, and the dataset queries the function.

A second query shape is a second function. A count by fleet, a count grouped by name, and a limited list are three functions, because each one has its own arguments and its own return columns.

```python
generator.create_sql_function(
    "boats_by_name",
    """
    SELECT name, count(*) AS n
    FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
    WHERE space = 'inst_sailboat_fleet_a'
    GROUP BY name
    """,
    secret_scope="cdf_sailboat_sailboat",
)
```

Power BI then calls `boats_by_name(space => '...')`. The group column `name` is fixed in the function. A grouped function returns at most 1000 groups.

```sql
SELECT *
FROM f0connectortest.sailboat_sailboat_v1.boats_by_name(
  space => 'inst_sailboat_fleet_a'
)
```

## Arguments

| In the view query | SQL function argument | Passed to the UDTF as |
|-------------------|----------------------|------------------------|
| `space = 'fleet'` | `space STRING` | `instance_space` |
| `space IN (...)` | `space ARRAY<STRING>` | `instance_space`, as JSON |
| `external_id = 'id'` | `external_id STRING` | `external_id` |
| `external_id IN (...)` | `external_id ARRAY<STRING>` | `external_id`, as JSON |
| `name = 'XBOX'` | `name STRING` | that property |
| `name IN (...)` | `name ARRAY<STRING>` | that property, as JSON |
| `aph_tod >= 10` | `aph_tod_gte DOUBLE` | `_gte` |
| `LIMIT 10` on a list | `row_limit BIGINT` | `_row_limit` |
| `description IS NOT NULL` | none | `_exists`, fixed in the body |
| `GROUP BY name` | none | `_group_by`, fixed in the body |

`space` on the function is the instance space column from the view. Inside the function it is passed to the UDTF parameter `instance_space`.

## Permissions

The function is created with `SQL SECURITY DEFINER`. Calls run as the function owner, which is the principal that ran `create_sql_function`. That owner must be allowed to read the secret scope. The Power BI connection only needs `EXECUTE` on the function and `USAGE` on the catalog and schema. Do not grant that connection `READ` on the secret scope. If a principal that cannot read the scope recreates the function, calls fail when the body evaluates `SECRET()`.

```sql
GRANT USAGE ON CATALOG f0connectortest TO `powerbi@company.com`;
GRANT USAGE ON SCHEMA f0connectortest.sailboat_sailboat_v1 TO `powerbi@company.com`;
GRANT EXECUTE ON FUNCTION f0connectortest.sailboat_sailboat_v1.boats_named TO `powerbi@company.com`;
```

See [Governance](./governance.md) for the same grants on views.

## Related

- [Filtering](./filtering.md) — which predicates push to CDF
- [rewrite_query in a notebook](./rewrite_query.md)
- [Investigating performance (EXPLAIN)](./explain_filter_pushdown.md) — how to see that a view query filters in Spark
- [Views](./views.md)
- Example notebook: `examples/catalog_based/pushdown_latency_cogsail_lims.ipynb`
