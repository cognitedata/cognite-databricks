# Registration

## Register All UDTFs (Recommended)

The easiest way to register UDTFs is to use `register_session_scoped_udtfs()` which registers all generated UDTFs at once:

```python
from databricks.sdk import WorkspaceClient
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Create WorkspaceClient (auto-detects credentials in Databricks)
workspace_client = WorkspaceClient()

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")

# Generate UDTFs with all parameters
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,  # Include this for full functionality
    output_dir="/Workspace/Users/user@example.com/udtf",
    # Note: catalog and schema parameters are only used for Unity Catalog registration,
    # not for session-scoped UDTFs. They can be omitted for session-scoped use.
)

# Register all UDTFs for session-scoped use (includes time series UDTFs automatically)
registered = generator.register_session_scoped_udtfs()
# Returns: {"SmallBoat": "smallboat_udtf", "LargeBoat": "largeboat_udtf", 
#           "time_series_datapoints": "time_series_datapoints_udtf", ...}

# Print registered functions
print("\n✓ Registered UDTFs:")
for view_id, func_name in registered.items():
    print(f"  - {view_id} -> {func_name}")
```

## Register Single UDTF

To register a single UDTF from a generated file:

```python
from cognite.databricks import register_udtf_from_file

# Register a single UDTF
function_name = register_udtf_from_file(
    "/Workspace/Users/user@example.com/udtf/sailboat_sailboat_v1/SmallBoat_udtf.py",
    function_name="smallboat_udtf"  # Optional: auto-extracted from class name if None
)

print(f"✓ Registered: {function_name}")
```

## Time Series UDTF Registration

Time Series UDTFs are **automatically generated and registered** when you call `register_session_scoped_udtfs()`. They use the same template-based generation as Data Model UDTFs and are included in the registration result:

```python
# Time series UDTFs are automatically included when registering
registered = generator.register_session_scoped_udtfs()

# Time series UDTFs will appear in the registered dictionary:
# - "time_series_datapoints" -> "time_series_datapoints_udtf"
# - "time_series_datapoints_long" -> "time_series_datapoints_long_udtf"
# - "time_series_latest_datapoints" -> "time_series_latest_datapoints_udtf"
```

**Note**: No manual registration is needed for time series UDTFs - they are template-generated and auto-registered along with Data Model UDTFs.

## Next Steps

After registration, you can start [Querying](./querying.md) your UDTFs.


