# Time Series UDTFs

## Session-Scoped Registration

Time Series UDTFs are **template-generated** using the same template-based generation approach as Data Model UDTFs. They are generated when you call `generate_udtf_notebook()` and can be registered for session-scoped use:

Time series UDTFs are automatically generated along with data model UDTFs when using `generate_udtf_notebook()`. They use the same template-based generation for consistency:

```python
from databricks.sdk import WorkspaceClient
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Create WorkspaceClient (auto-detects credentials in Databricks)
workspace_client = WorkspaceClient()

# Load client and generate UDTFs (includes time series UDTFs)
client = load_cognite_client_from_toml("config.toml")
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,  # Include this for full functionality
    output_dir="/Workspace/Users/user@example.com/udtf",
    # Note: catalog and schema parameters are only used for Unity Catalog registration,
    # not for session-scoped UDTFs. They can be omitted for session-scoped use.
)

# Register all UDTFs (data model + time series) for session-scoped use
registered = generator.register_session_scoped_udtfs()
# Returns: {"SmallBoat": "small_boat_udtf", ..., "time_series_datapoints": "time_series_datapoints_udtf", ...}
```

**Note**: Time series UDTFs are generated using the same Jinja2 templates as Data Model UDTFs, ensuring consistent behavior, error handling, and initialization patterns.

## Querying Time Series Datapoints

See the [Querying](./querying.md) section for detailed examples of querying time series UDTFs, including:

- Single time series datapoints
- Multiple time series (long format)
- Latest datapoints

## Detailed vs SQL-Native UDTFs

**`time_series_datapoints_detailed_udtf`**:
- Flexible: supports raw datapoints or aggregates (aggregates/granularity optional).
- Returns status metadata (`status_code`, `status_symbol`) for datapoints.
- Best for debugging or when you need status info.

**`time_series_sql_udtf`** (catalog-based):
- Requires aggregate + granularity hints (pushdown enforced).
- Returns aggregated datapoints only (no status columns).
- Best for SQL-native analytics and pushdown performance.

## Table Function Syntax (Notebook vs SQL Warehouse)

UDTFs are table functions, and the SQL syntax differs by environment:

**Notebook `%sql` (cluster-backed):**
```sql
SELECT *
FROM time_series_datapoints_detailed_udtf(
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project'),
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
  time_series_datapoints_detailed_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project'),
    instance_ids  => 'space1:ts1,space1:ts2',
    start         => current_timestamp() - INTERVAL 52 WEEKS,
    end           => current_timestamp() - INTERVAL 51 WEEKS,
    aggregates    => 'average',
    granularity   => '2h'
  )
);
```

## Available Time Series UDTFs

1. **`time_series_datapoints_udtf`**: Retrieve datapoints from a single time series
2. **`time_series_datapoints_long_udtf`**: Retrieve datapoints from multiple time series in long format
3. **`time_series_latest_datapoints_udtf`**: Retrieve the latest datapoints for one or more time series

## Next Steps

- See [Querying](./querying.md) for detailed query examples
- Learn about [Joining](./joining.md) time series UDTFs with Data Model views
- For production use, see [Catalog-Based Time Series UDTFs](../catalog_based/time_series.md)

