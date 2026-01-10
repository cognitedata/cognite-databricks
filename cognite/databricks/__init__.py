"""Helper SDK for Databricks UDTF registration and Unity Catalog integration."""

from cognite.databricks.generator import (
    UDTFGenerator,
    generate_session_scoped_notebook_code,
    generate_time_series_udtf_view_sql,
    generate_udtf_notebook,
    generate_udtf_sql_query,
    register_udtf_from_file,
)
from cognite.databricks.models import (
    CDFConnectionConfig,
    RegisteredUDTFResult,
    TimeSeriesUDTFConfig,
    TimeSeriesUDTFRegistry,
    UDTFRegistrationResult,
    time_series_udtf_registry,
)
from cognite.databricks.secret_manager import SecretManagerHelper
from cognite.databricks.type_converter import TypeConverter
from cognite.databricks.udtf_registry import UDTFRegistry
from cognite.databricks.utils import (
    inspect_function_parameters,
    inspect_recently_created_udtf,
    list_functions_in_schema,
    to_udtf_function_name,
)
from cognite.pygen_spark.time_series_udtfs import (
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesDatapointsUDTF,
    TimeSeriesLatestDatapointsUDTF,
)

__all__ = [
    "generate_udtf_notebook",
    "UDTFGenerator",
    "register_udtf_from_file",
    "generate_udtf_sql_query",
    "generate_session_scoped_notebook_code",
    "generate_time_series_udtf_view_sql",
    "UDTFRegistry",
    "SecretManagerHelper",
    "TypeConverter",
    "CDFConnectionConfig",
    "RegisteredUDTFResult",
    "UDTFRegistrationResult",
    "TimeSeriesUDTFConfig",
    "TimeSeriesUDTFRegistry",
    "time_series_udtf_registry",
    "inspect_function_parameters",
    "list_functions_in_schema",
    "inspect_recently_created_udtf",
    "to_udtf_function_name",
    "TimeSeriesDatapointsUDTF",
    "TimeSeriesDatapointsLongUDTF",
    "TimeSeriesLatestDatapointsUDTF",
]

__version__ = "0.1.0"
