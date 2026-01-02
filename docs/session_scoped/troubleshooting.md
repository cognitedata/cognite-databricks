# Troubleshooting

## Common Issues and Solutions

### Issue: "No active SparkSession found"

**Solution**: Ensure you're running the code in a Databricks notebook with an active Spark session. If using a Python script, create a SparkSession:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UDTF Registration").getOrCreate()
```

### Issue: "PySpark is required for session-scoped UDTF registration"

**Solution**: Install PySpark or ensure you're running in a Databricks environment where PySpark is available:

```python
%pip install pyspark
```

### Issue: UDTF returns no results

**Possible Causes:**
1. **Incorrect credentials**: Verify that SECRET() values are correct
2. **No matching data**: Check that filters match existing data in CDF
3. **Time series doesn't exist**: Verify the time series external_id exists in CDF

**Debug Steps:**
```python
# Test credentials
from cognite.pygen import load_cognite_client_from_toml
client = load_cognite_client_from_toml("config.toml")

# Test data model query
from cognite.client.data_classes.data_modeling.ids import DataModelId
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")
views = client.data_modeling.views.list(data_model_id)
print(f"Found {len(views)} views")
```

### Issue: "Module not found" errors

**Solution**: Restart the Python kernel after installing packages with `%pip`:

1. Run `%pip install cognite-sdk cognite-databricks`
2. When prompted, click "Restart" to restart the kernel
3. Re-run your registration code

### Issue: UDTF registration succeeds but SQL query fails

**Possible Causes:**
1. **Function name mismatch**: Verify the registered function name matches what you're calling in SQL
2. **Parameter mismatch**: Check that all required parameters are provided
3. **Type errors**: Ensure parameter types match the UDTF's expected types

**Debug Steps:**
```python
# Check registered functions
registered = generator.register_session_scoped_udtfs()
print("Registered functions:", registered)

# Verify function name in SQL matches
# If registered as "smallboat_udtf", use:
# SELECT * FROM smallboat_udtf(
#     client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
#     client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
#     tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
#     cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
#     project => SECRET('cdf_sailboat_sailboat', 'project'),
#     name => NULL,
#     description => NULL
# ) LIMIT 10;
```

### Issue: Time Series UDTF returns NULL values

**Possible Causes:**
1. **Time series doesn't exist**: Verify the time series external_id exists
2. **No datapoints in range**: Check that the time range (start/end) contains data
3. **Incorrect instance_id format**: Ensure space and external_id are correct

**Debug Steps:**
```python
# Test time series existence
from cognite.client.data_classes.data_modeling.ids import NodeId

instance_id = NodeId(space="sailboat", external_id="vessel.speed")
ts = client.time_series.retrieve(external_id=instance_id.external_id)
print(f"Time series exists: {ts is not None}")

# Test datapoints retrieval
datapoints = client.time_series.data.retrieve(
    external_id=instance_id.external_id,
    start="1d-ago",
    end="now"
)
print(f"Found {len(datapoints)} datapoints")
```

## Getting Help

If you encounter issues not covered here:

1. **Check the logs**: Look for error messages in the notebook output or Databricks logs
2. **Verify credentials**: Ensure CDF credentials are correct and have proper permissions
3. **Test with simple queries**: Start with basic queries before adding complex filters or joins
4. **Review the Technical Plan**: See the Technical Plan document for detailed architecture and implementation details

## Next Steps

After successfully using session-scoped UDTFs, consider:

1. **Unity Catalog Registration**: Register UDTFs and Views in Unity Catalog for production use (see [Catalog-Based UDTF Registration](../catalog_based/index.md))
2. **View Creation**: Create SQL Views that wrap UDTFs for easier querying
3. **Governance**: Set up Unity Catalog permissions for production deployments

For more information, see:
- [Catalog-Based UDTF Registration](../catalog_based/index.md)
- Technical Plan: CDF Databricks Integration (UDTF-Based)


