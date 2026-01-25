"""Helper SDK for Databricks UDTF registration and Unity Catalog integration."""

try:
    from cognite.pygen_spark.time_series_udtfs import (
        TimeSeriesDatapointsUDTF,
        TimeSeriesLatestDatapointsUDTF,
    )
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    TimeSeriesDatapointsUDTF = None  # type: ignore[assignment,misc]
    TimeSeriesLatestDatapointsUDTF = None  # type: ignore[assignment,misc]

from cognite.databricks._version import __version__

try:
    from cognite.databricks.generator import (
        UDTFGenerator,
        generate_session_scoped_notebook_code,
        generate_time_series_udtf_view_sql,
        generate_udtf_notebook,
        generate_udtf_sql_query,
        register_udtf_from_file,
    )
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    UDTFGenerator = None  # type: ignore[assignment,misc]
    generate_session_scoped_notebook_code = None  # type: ignore[assignment,misc]
    generate_time_series_udtf_view_sql = None  # type: ignore[assignment,misc]
    generate_udtf_notebook = None  # type: ignore[assignment,misc]
    generate_udtf_sql_query = None  # type: ignore[assignment,misc]
    register_udtf_from_file = None  # type: ignore[assignment,misc]
from cognite.databricks.models import (
    CDFConnectionConfig,
    RegisteredUDTFResult,
    TimeSeriesUDTFConfig,
    TimeSeriesUDTFRegistry,
    UDTFRegistrationResult,
    time_series_udtf_registry,
)
from cognite.databricks.secret_manager import SecretManagerHelper

try:
    from cognite.databricks.type_converter import TypeConverter
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    TypeConverter = None  # type: ignore[assignment,misc]
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
    "TimeSeriesUDTFConfig",
    "TimeSeriesUDTFRegistry",
    "UDTFRegistrationResult",
    "UDTFRegistry",
    "__version__",
    "inspect_function_parameters",
    "inspect_recently_created_udtf",
    "list_functions_in_schema",
    "time_series_udtf_registry",
    "to_udtf_function_name",
]

if TimeSeriesDatapointsUDTF is not None:
    __all__.append("TimeSeriesDatapointsUDTF")

if TimeSeriesLatestDatapointsUDTF is not None:
    __all__.append("TimeSeriesLatestDatapointsUDTF")

if TypeConverter is not None:
    __all__.append("TypeConverter")

if UDTFGenerator is not None:
    __all__.append("UDTFGenerator")
    __all__.append("generate_session_scoped_notebook_code")
    __all__.append("generate_time_series_udtf_view_sql")
    __all__.append("generate_udtf_notebook")
    __all__.append("generate_udtf_sql_query")
    __all__.append("register_udtf_from_file")
