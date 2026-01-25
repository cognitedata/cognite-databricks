# Time Series UDTFs

## Catalog-Based Registration

Time Series UDTFs are automatically generated when using `generate_udtf_notebook()` along with data model UDTFs. They use the same template-based generation pattern for consistency.

```python
from cognite.databricks import generate_udtf_notebook, UDTFGenerator
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()
client = load_cognite_client_from_toml("config.toml")

# Generate UDTFs (includes both data model and time series UDTFs)
generator = generate_udtf_notebook(
    data_model=DataModelId(space="sailboat", external_id="sailboat", version="v1"),
    client=client,
    workspace_client=workspace_client,
    output_dir="/Workspace/Users/user@example.com/udtf",
    catalog="main",
    schema="cdf_models",
)

# Register all UDTFs (data model + time series) in Unity Catalog
secret_scope = "cdf_sailboat_sailboat"
result = generator.register_udtfs_and_views(secret_scope=secret_scope)

# Time series UDTFs are included in the result
print(f"Registered {result.total_count} UDTF(s) including time series UDTFs")
```

## Time Series Views

You can create Views for time series UDTFs:

```python
from cognite.databricks import generate_time_series_udtf_view_sql
from cognite.databricks import UDTFRegistry

registry = UDTFRegistry(workspace_client)

# Generate View SQL
view_sql = generate_time_series_udtf_view_sql(
    udtf_name="time_series_datapoints_udtf",
    secret_scope="cdf_sailboat_sailboat",
    view_name="time_series_datapoints",
    catalog="main",
    schema="cdf_models",
)

# Register View
registry.register_view(
    catalog="main",
    schema="cdf_models",
    view_name="time_series_datapoints",
    view_sql=view_sql,
)
```

## SQL-Native Time Series (Alpha)

The SQL-native time series UDTF (`time_series_sql_udtf`) is **alpha** and not fully tested. Expect changes. See
[SQL-Native Time Series](./time_series_sql.md) for details and usage.

## Table Function Syntax (Notebook vs SQL Warehouse)

Time series UDTFs are table functions. Use the syntax that matches your environment:

**Notebook `%sql` (cluster-backed):**
```sql
SELECT *
FROM main.cdf_models.time_series_datapoints_detailed_udtf(
  client_id     => SECRET('cdf_scope', 'client_id'),
  client_secret => SECRET('cdf_scope', 'client_secret'),
  tenant_id     => SECRET('cdf_scope', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_scope', 'cdf_cluster'),
  project       => SECRET('cdf_scope', 'project'),
  instance_ids  => 'space1:ts1,space1:ts2',
  start         => current_timestamp() - INTERVAL 52 WEEKS,
  end           => current_timestamp() - INTERVAL 51 WEEKS,
  aggregates    => 'average',
  granularity   => '2h'
) AS t;
```

**SQL Warehouse (Databricks SQL):**
```sql
SELECT *
FROM TABLE(
  main.cdf_models.time_series_datapoints_detailed_udtf(
    client_id     => SECRET('cdf_scope', 'client_id'),
    client_secret => SECRET('cdf_scope', 'client_secret'),
    tenant_id     => SECRET('cdf_scope', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_scope', 'cdf_cluster'),
    project       => SECRET('cdf_scope', 'project'),
    instance_ids  => 'space1:ts1,space1:ts2',
    start         => current_timestamp() - INTERVAL 52 WEEKS,
    end           => current_timestamp() - INTERVAL 51 WEEKS,
    aggregates    => 'average',
    granularity   => '2h'
  )
);
```

## Querying Time Series Views

```sql
-- Query time series View
SELECT * FROM main.cdf_models.time_series_datapoints
WHERE space = 'sailboat'
  AND external_id = 'vessel.speed'
  AND start = '1d-ago'
  AND end = 'now'
ORDER BY timestamp
LIMIT 10;
```

## Next Steps

- Learn about [Joining](./joining.md) Views with time series UDTFs
- See [Querying](./querying.md) for more query examples

