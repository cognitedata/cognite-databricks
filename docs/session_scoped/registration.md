# Registration

## Register All UDTFs (Recommended)

The easiest way to register UDTFs is to use `register_session_scoped_udtfs()` which registers all generated UDTFs at once:

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

# Generate UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    output_dir="/Workspace/Users/user@example.com/udtf",
)

# Register all UDTFs for session-scoped use
registered = generator.register_session_scoped_udtfs()
# Returns: {"SmallBoat": "smallboat_udtf", "LargeBoat": "largeboat_udtf"}

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

## Register Time Series UDTFs

Time Series UDTFs are pre-built in the `cognite-databricks` package and can be registered directly:

```python
from pyspark.sql.functions import udtf
from cognite.databricks.time_series_udtfs import (
    TimeSeriesDatapointsUDTF,
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesLatestDatapointsUDTF,
)

# Register time series UDTFs
time_series_datapoints_udtf = udtf(TimeSeriesDatapointsUDTF)
time_series_datapoints_long_udtf = udtf(TimeSeriesDatapointsLongUDTF)
time_series_latest_datapoints_udtf = udtf(TimeSeriesLatestDatapointsUDTF)

print("✓ Time Series UDTFs registered")
```

## Next Steps

After registration, you can start [Querying](./querying.md) your UDTFs.


