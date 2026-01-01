# Time Series UDTFs

## Catalog-Based Registration

Time Series UDTFs can be registered in Unity Catalog:

```python
from cognite.databricks import UDTFGenerator, generate_time_series_udtf_files
from cognite.databricks import UDTFRegistry
from databricks.sdk import WorkspaceClient
from pathlib import Path

workspace_client = WorkspaceClient()
registry = UDTFRegistry(workspace_client)

# Generate time series UDTF files
udtf_files = generate_time_series_udtf_files(
    output_dir=Path("/tmp/time_series_udtfs"),
    top_level_package="cognite_databricks",
)

# Register time series UDTFs
for udtf_name, udtf_file in udtf_files.items():
    # Read UDTF code
    udtf_code = udtf_file.read_text()
    
    # Register in Unity Catalog
    registry.register_udtf(
        catalog="main",
        schema="cdf_models",
        function_name=udtf_name,
        udtf_code=udtf_code,
        input_params=input_params,  # From TimeSeriesUDTFConfig
        return_type=return_type,  # From TimeSeriesUDTFConfig
        return_params=return_params,  # From TimeSeriesUDTFConfig
        dependencies=["cognite-sdk>=7.90.1"],
    )
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

