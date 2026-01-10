"""Helper SDK for Databricks UDTF registration and Unity Catalog integration."""

from cognite.pygen_spark.time_series_udtfs import (
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesDatapointsUDTF,
    TimeSeriesLatestDatapointsUDTF,
)

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

__all__ = [
    "CDFConnectionConfig",
    "RegisteredUDTFResult",
    "SecretManagerHelper",
    "TimeSeriesDatapointsLongUDTF",
    "TimeSeriesDatapointsUDTF",
    "TimeSeriesLatestDatapointsUDTF",
    "TimeSeriesUDTFConfig",
    "TimeSeriesUDTFRegistry",
    "TypeConverter",
    "UDTFGenerator",
    "UDTFRegistrationResult",
    "UDTFRegistry",
    "__version__",
    "generate_session_scoped_notebook_code",
    "generate_time_series_udtf_view_sql",
    "generate_udtf_notebook",
    "generate_udtf_sql_query",
    "inspect_function_parameters",
    "inspect_recently_created_udtf",
    "list_functions_in_schema",
    "register_udtf_from_file",
    "time_series_udtf_registry",
    "to_udtf_function_name",
]

from cognite.databricks._version import __version__
