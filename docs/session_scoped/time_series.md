# Time Series UDTFs

## Session-Scoped Registration

Time Series UDTFs are pre-built in the `cognite-databricks` package and can be registered for session-scoped use:

```python
from pyspark.sql.functions import udtf
from cognite.databricks.time_series_udtfs import (
    TimeSeriesDatapointsUDTF,
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesLatestDatapointsUDTF,
)

# Register all time series UDTFs
time_series_datapoints_udtf = udtf(TimeSeriesDatapointsUDTF)
time_series_datapoints_long_udtf = udtf(TimeSeriesDatapointsLongUDTF)
time_series_latest_datapoints_udtf = udtf(TimeSeriesLatestDatapointsUDTF)
```

## Querying Time Series Datapoints

See the [Querying](./querying.md) section for detailed examples of querying time series UDTFs, including:

- Single time series datapoints
- Multiple time series (long format)
- Latest datapoints

## Available Time Series UDTFs

1. **`time_series_datapoints_udtf`**: Retrieve datapoints from a single time series
2. **`time_series_datapoints_long_udtf`**: Retrieve datapoints from multiple time series in long format
3. **`time_series_latest_datapoints_udtf`**: Retrieve the latest datapoints for one or more time series

## Next Steps

- See [Querying](./querying.md) for detailed query examples
- Learn about [Joining](./joining.md) time series UDTFs with Data Model views
- For production use, see [Catalog-Based Time Series UDTFs](../catalog_based/time_series.md)

