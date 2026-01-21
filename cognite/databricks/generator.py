"""generate_udtf_notebook and UDTFGenerator - High-level APIs for UDTF generation and registration."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Semaphore
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

try:
    from importlib.metadata import distribution  # noqa: F401
except ImportError:
    pass  # type: ignore

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.views import (
    MultiReverseDirectRelation,
    SingleReverseDirectRelation,
    ViewProperty,
)
from cognite.pygen_spark import SparkUDTFGenerator
from cognite.pygen_spark.fields import UDTFField

from cognite.databricks.models import (
    RegisteredUDTFResult,
    RegisteredViewResult,
    UDTFRegistrationResult,
    ViewRegistrationResult,
)
from cognite.databricks.secret_manager import SecretManagerHelper
from cognite.databricks.type_converter import TypeConverter
from cognite.databricks.udtf_registry import UDTFRegistry
from cognite.databricks.utils import to_udtf_function_name
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterMode,
    FunctionParameterType,
)

# PySpark is provided by Databricks runtime - import with error handling
try:
    from pyspark.sql.types import (  # type: ignore[import-not-found]
        ArrayType,
        BooleanType,
        DataType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError as e:
    raise ImportError(
        "PySpark is required but not available. "
        "In Databricks, PySpark is provided by the runtime. "
        "For local development, install with: pip install cognite-databricks[local]"
    ) from e

if TYPE_CHECKING:
    pass

# Define DataModel type alias (same as pygen)
# Short-term: We define our own type alias using public types from cognite.client
# This avoids depending on pygen's private API (_generator module)
DataModel = DataModelIdentifier | dm.DataModel[dm.View]


def register_udtf_from_file(
    udtf_file_path: str | Path,
    function_name: str | None = None,
    spark_session: object | None = None,  # SparkSession type
) -> str:
    """Register a UDTF from a generated Python file for session-scoped use.

    This function loads a generated UDTF file and registers it with PySpark
    for session-scoped use. The generated files already have the `analyze` method
    required for PySpark Connect.

    Args:
        udtf_file_path: Path to the generated UDTF Python file (e.g., "SmallBoat_udtf.py")
        function_name: Optional function name for registration. If None, extracts from class name.
        spark_session: Optional SparkSession. If None, uses the active SparkSession.

    Returns:
        The registered function name

    Example:
        # After running generate_udtf_notebook():
        generator = generate_udtf_notebook(data_model_id, client, ...)

        # Register a single UDTF for session-scoped use:
        register_udtf_from_file(
            "/Workspace/Users/user@example.com/udtf/sailboat_sailboat_v1/SmallBoat_udtf.py",
            function_name="smallboat_udtf"
        )

        # Now you can use it in SQL:
        # SELECT * FROM smallboat_udtf(...)
    """
    # Import here to avoid requiring PySpark at module level
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "PySpark is required for session-scoped UDTF registration. "
            "Make sure you're running this in a Databricks notebook or have PySpark installed."
        ) from e

    if spark_session is None:
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError(
                "No active SparkSession found. Please provide spark_session parameter "
                "or ensure you're running in a Databricks notebook with an active Spark session."
            )

    # Type narrow spark_session for mypy
    if not hasattr(spark_session, "udtf"):
        raise RuntimeError("spark_session does not have udtf attribute")

    udtf_file_path = Path(udtf_file_path)
    if not udtf_file_path.exists():
        raise FileNotFoundError(f"UDTF file not found: {udtf_file_path}")

    # Read and execute the UDTF file
    with Path(udtf_file_path).open(encoding="utf-8") as f:
        udtf_code = f.read()

    # Execute the code in a temporary namespace to get the UDTF object
    namespace: dict[str, object] = {}
    exec(udtf_code, namespace)

    # Find the UDTF class name via AST, then fetch the object from the namespace.
    # The generated template uses scalar mode only (no Arrow)
    # is already decorated and ready to register.
    import ast

    try:
        tree = ast.parse(udtf_code)
    except SyntaxError as e:
        raise ValueError(f"Invalid Python syntax in {udtf_file_path}: {e}") from e

    udtf_class_name: str | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            has_eval = any(isinstance(item, ast.FunctionDef) and item.name == "eval" for item in node.body)
            if has_eval:
                udtf_class_name = node.name
                break

    if not udtf_class_name:
        raise ValueError(f"No UDTF class found in {udtf_file_path}. Expected a class with 'eval' method.")

    if udtf_class_name not in namespace:
        raise ValueError(f"UDTF class '{udtf_class_name}' not found in executed namespace for {udtf_file_path}.")

    udtf_obj = namespace[udtf_class_name]

    # Determine function name
    if function_name is None:
        # Extract from class name: SmallboatUDTF -> small_boat_udtf
        class_name = udtf_class_name
        # Remove "UDTF" suffix if present
        base_name = class_name[:-4] if class_name.endswith("UDTF") else class_name
        # Use pygen-main's to_snake for consistent conversion
        function_name = to_udtf_function_name(base_name)

    # Register the UDTF object
    # For session-scoped UDTFs, we avoid the @udtf decorator in the generated code
    # to prevent PySpark Connect from importing pyspark.sql.connect.udtf during serialization,
    # which triggers a circular import bug. Instead, we apply the decorator here at registration time.
    if hasattr(udtf_obj, "analyze"):
        # UDTF has analyze method - register directly (PySpark Connect will use it)
        # Note: This path should not be used for session-scoped UDTFs (analyze is disabled)
        spark_session.udtf.register(function_name, udtf_obj)  # type: ignore[attr-defined,union-attr]
    elif hasattr(udtf_obj, "outputSchema"):
        # UDTF doesn't have analyze method - use outputSchema for return type
        # Apply @udtf decorator here at registration time to avoid circular import during serialization
        # Import udtf lazily to minimize chance of triggering circular import
        try:
            # Try importing from non-Connect module first
            import sys

            if "pyspark.sql.functions" in sys.modules:
                udtf_func = sys.modules["pyspark.sql.functions"].udtf
            else:
                from pyspark.sql.functions import udtf as udtf_func  # type: ignore[import-not-found]
            output_schema = udtf_obj.outputSchema()
            udtf_wrapped = udtf_func(returnType=output_schema)(udtf_obj)
            spark_session.udtf.register(function_name, udtf_wrapped)  # type: ignore[attr-defined,union-attr]
        except Exception as e:
            # If registration fails, try without decorator (some PySpark versions may support this)
            print(f"Warning: Failed to apply @udtf decorator: {e}")
            print("Attempting registration without decorator...")
            spark_session.udtf.register(function_name, udtf_obj)  # type: ignore[attr-defined,union-attr]
    else:
        raise ValueError(f"UDTF class '{udtf_class_name}' must have either 'analyze' method or 'outputSchema' method.")

    print(f"✓ UDTF registered successfully: {function_name}")
    print(f"✓ Class: {udtf_class_name}")
    print(f"✓ File: {udtf_file_path}")

    return function_name


def generate_time_series_udtf_view_sql(
    udtf_name: str,
    secret_scope: str,
    view_name: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    udtf_params: list[str] | None = None,
) -> str:
    """Generate SQL CREATE VIEW statement for a time series datapoints UDTF.

    Uses Pydantic models for type-safe configuration, following pygen-main patterns.
    The view calls the UDTF with SECRET parameters for credentials.
    All UDTF-specific parameters are passed as NULL to allow Unity Catalog validation.

    Note: Views created this way will return empty results unless parameters are provided.
    For useful views, create specific views with hardcoded parameter values.

    Args:
        udtf_name: Name of the UDTF function (e.g., "time_series_datapoints_udtf")
        view_name: Optional view name. If None, uses default from TimeSeriesUDTFRegistry
        secret_scope: Databricks Secret Manager scope name
        catalog: Optional catalog name. If None, uses placeholder "{{ catalog }}"
        schema: Optional schema name. If None, uses placeholder "{{ schema }}"
        udtf_params: Optional list of parameter names from config (used to generate NULL parameters)

    Returns:
        SQL CREATE VIEW statement

    Example:
        sql = generate_time_series_udtf_view_sql(
            udtf_name="time_series_datapoints_udtf",
            secret_scope="cdf_sailboat_sailboat",
            view_name="time_series_datapoints",
            catalog="main",
            schema="cdf_models",
        )
        # Returns:
        # CREATE OR REPLACE VIEW main.cdf_models.time_series_datapoints AS
        # SELECT * FROM main.cdf_models.time_series_datapoints_udtf(
        #     client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
        #     client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
        #     tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
        #     cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
        #     project       => SECRET('cdf_sailboat_sailboat', 'project'),
        #     instance_id   => NULL,
        #     start         => NULL,
        #     end           => NULL,
        #     aggregates    => NULL,
        #     granularity   => NULL
        # )
    """
    from cognite.databricks.models import time_series_udtf_registry

    # Get configuration from Pydantic registry
    config = time_series_udtf_registry.get_config(udtf_name)
    if config is None:
        available_udtfs = time_series_udtf_registry.get_all_udtf_names()
        raise ValueError(f"Unknown time series UDTF: {udtf_name}. Available UDTFs: {available_udtfs}")

    # Build catalog.schema prefix
    if catalog and schema:
        catalog_schema_prefix = f"{catalog}.{schema}."
    elif catalog:
        catalog_schema_prefix = f"{catalog}.{{{{ schema }}}}."
    elif schema:
        catalog_schema_prefix = f"{{{{ catalog }}}}.{schema}."
    else:
        catalog_schema_prefix = "{{ catalog }}.{{ schema }}."

    # Use view_name from config if not provided
    if view_name is None:
        view_name = config.view_name

    # Build SQL - include SECRET parameters and all UDTF-specific parameters as NULL
    # This allows Unity Catalog to validate the view SQL syntax
    # Note: Views created this way will return empty results - users should call UDTF directly
    # or create specific views with hardcoded parameter values
    sql_lines = [
        f"CREATE OR REPLACE VIEW {catalog_schema_prefix}{view_name} AS",
        f"SELECT * FROM {catalog_schema_prefix}{udtf_name}(",
        f"    client_id     => SECRET('{secret_scope}', 'client_id'),",
        f"    client_secret => SECRET('{secret_scope}', 'client_secret'),",
        f"    tenant_id     => SECRET('{secret_scope}', 'tenant_id'),",
        f"    cdf_cluster   => SECRET('{secret_scope}', 'cdf_cluster'),",
        f"    project       => SECRET('{secret_scope}', 'project'),",
    ]

    # Add all UDTF-specific parameters as NULL (from config.parameters)
    if config.parameters:
        for i, param in enumerate(config.parameters):
            comma = "," if i < len(config.parameters) - 1 else ""
            sql_lines.append(f"    {param} => NULL{comma}")

    sql_lines.append(")")

    return "\n".join(sql_lines)


def generate_udtf_sql_query(
    catalog: str,
    schema: str,
    function_name: str,
    secret_scope: str,
    view_properties: list[str] | None = None,
    use_named_parameters: bool = True,
    limit: int = 10,
) -> str:
    """Generate SQL query for a UDTF with optional named parameters.

    This helper function generates clean SQL queries that avoid the need for
    dozens of positional NULL parameters. When UDTF parameters have default
    values (DEFAULT NULL), you can use named parameter syntax to only specify
    the credentials.

    Args:
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        function_name: UDTF function name
        secret_scope: Secret scope name for credentials
        view_properties: Optional list of view property names to include as named parameters.
                        If None and use_named_parameters=True, only credentials are included.
        use_named_parameters: If True, uses named parameter syntax (param => value).
                             If False, uses positional parameters with NULLs.
        limit: LIMIT clause value

    Returns:
        SQL query string

    Example:
        # Named parameters (clean, recommended)
        sql = generate_udtf_sql_query(
            "f0connectortest", "sailboat_sailboat_v1", "smallboat_udtf",
            "cdf_sailboat_sailboat",
            use_named_parameters=True
        )
        # Result: SELECT * FROM ...(client_id => SECRET(...), ...)

        # Positional parameters (legacy, for compatibility)
        sql = generate_udtf_sql_query(
            "f0connectortest", "sailboat_sailboat_v1", "smallboat_udtf",
            "cdf_sailboat_sailboat",
            use_named_parameters=False,
            view_properties=["name", "description", "tags", ...]
        )
        # Result: SELECT * FROM ...(SECRET(...), NULL, NULL, ...)
    """
    full_function_name = f"{catalog}.{schema}.{function_name}"

    if use_named_parameters:
        # Named parameters - only include credentials
        sql_lines = [
            f"-- Test the {function_name} UDTF using named parameters",
            "-- All view property parameters use default NULL values",
            f"SELECT * FROM {full_function_name}(",
            f"    client_id     => SECRET('{secret_scope}', 'client_id'),",
            f"    client_secret => SECRET('{secret_scope}', 'client_secret'),",
            f"    tenant_id     => SECRET('{secret_scope}', 'tenant_id'),",
            f"    cdf_cluster   => SECRET('{secret_scope}', 'cdf_cluster'),",
            f"    project       => SECRET('{secret_scope}', 'project')",
            f") LIMIT {limit};",
        ]
    else:
        # Positional parameters - include all NULLs
        sql_lines = [
            f"-- Test the {function_name} UDTF using positional parameters",
            f"SELECT * FROM {full_function_name}(",
            f"    SECRET('{secret_scope}', 'client_id'),",
            f"    SECRET('{secret_scope}', 'client_secret'),",
            f"    SECRET('{secret_scope}', 'tenant_id'),",
            f"    SECRET('{secret_scope}', 'cdf_cluster'),",
            f"    SECRET('{secret_scope}', 'project'),",
            "    -- View property parameters (all NULL to get all rows)",
        ]

        if view_properties:
            for i, prop in enumerate(view_properties):
                comma = "," if i < len(view_properties) - 1 else ""
                sql_lines.append(f"    NULL{comma} -- {prop}")
        else:
            sql_lines.append("    NULL, -- Add property parameters here")

        sql_lines.append(f") LIMIT {limit};")

    return "\n".join(sql_lines)


def generate_session_scoped_notebook_code(
    generator: UDTFGenerator,
    secret_scope: str | None = None,
    data_model: DataModel | None = None,
    view_id: str | None = None,
) -> dict[str, str]:
    """Generate notebook-ready code snippets for session-scoped UDTF registration.

    Returns formatted code strings that can be copied directly into notebook cells.
    This makes it easy for users to get the exact code they need without manual typing.

    Args:
        generator: UDTFGenerator instance (from generate_udtf_notebook)
        secret_scope: Optional secret scope name. If None, will try to extract from generator.
        data_model: Optional DataModel identifier. If None, uses the one from generator.
        view_id: Optional specific view ID to use for SQL example. If None, uses first available.

    Returns:
        Dictionary with keys:
        - "cell1_dependencies": Code for installing dependencies
        - "cell2_registration": Code for registering UDTFs
        - "cell3_sql_example": Example SQL query for one UDTF (using named parameters)
        - "all_cells": Combined code for all cells (for reference)

    Example:
        generator = generate_udtf_notebook(data_model_id, client, ...)
        code_snippets = generate_session_scoped_notebook_code(generator)

        # Print and copy each cell
        print("Cell 1 - Install Dependencies:")
        print(code_snippets["cell1_dependencies"])
        print("\nCell 2 - Register UDTFs:")
        print(code_snippets["cell2_registration"])
        print("\nCell 3 - SQL Example:")
        print(code_snippets["cell3_sql_example"])
    """
    # Get the generated UDTF files to determine which UDTFs are available
    if data_model:
        udtf_result = generator.code_generator.generate_udtfs(data_model)
        udtf_files = udtf_result.generated_files
    else:
        try:
            udtf_files = generator._find_generated_udtf_files()
        except (FileNotFoundError, OSError):
            # If files don't exist yet, we'll use a generic example
            udtf_files = {}

    # Try to determine secret scope
    if secret_scope is None:
        if data_model:
            if isinstance(data_model, dm.DataModel):
                model_id = data_model.as_id()
            else:
                # Type narrowing - data_model is DataModelId at this point
                model_id = data_model  # type: ignore[assignment]
            secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
        else:
            try:
                data_model_obj = generator.code_generator._data_model
                if isinstance(data_model_obj, list) and data_model_obj:
                    data_model_obj = data_model_obj[0]
                if isinstance(data_model_obj, dm.DataModel):
                    model_id = data_model_obj.as_id()
                    secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
                else:
                    secret_scope = "cdf_<space>_<external_id>"
            except (AttributeError, KeyError):
                secret_scope = "cdf_<space>_<external_id>"

    # Get view IDs
    view_ids = list(udtf_files.keys()) if udtf_files else ["<ViewName>"]
    example_view_id = view_id if view_id and view_id in view_ids else (view_ids[0] if view_ids else "<ViewName>")

    # Convert view ID to function name using pygen-main's to_snake
    example_function_name = to_udtf_function_name(example_view_id)

    # Get view properties for SQL example (optional - for positional parameter example)
    view_property_names = []
    try:
        view = generator._get_view_by_id(example_view_id)
        if view and view.properties:
            view_property_names = list(view.properties.keys())
    except (AttributeError, KeyError):
        pass

    # Try to get actual values from generator
    try:
        actual_output_dir = str(generator.code_generator.output_dir)
    except AttributeError:
        actual_output_dir = "/Workspace/Users/<your_email>/udtf"

    # Try to get catalog and schema (not used, but kept for potential future use)
    try:
        _ = generator.catalog
        _ = generator.schema
    except AttributeError:
        pass  # Use placeholders in code snippets

    # Cell 1: Install Dependencies
    cell1 = """# Install dependencies (required for session-scoped UDTFs)
# Note: Restart Python kernel after installation when prompted

%pip install cognite-sdk"""

    # Cell 2: Register UDTFs
    cell2 = f"""# Register UDTFs for session-scoped use
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client (adjust path as needed)
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="<space>", external_id="<external_id>", version="<version>")

# Generate UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=None,  # Not needed for session-scoped registration
    output_dir="{actual_output_dir}",
)

# Register all UDTFs for session-scoped use
registered = generator.register_session_scoped_udtfs()

# Print registered functions
print("\\n✓ Registered UDTFs:")
for view_id, func_name in registered.items():
    print(f"  - {{view_id}} -> {{func_name}}")"""

    # Cell 3: SQL Example (using named parameters - clean!)
    # For session-scoped UDTFs, we don't use catalog.schema prefix
    sql_lines = [
        f"-- Test the {example_function_name} UDTF using named parameters",
        "-- All view property parameters use default NULL values",
        f"SELECT * FROM {example_function_name}(",
        f"    client_id     => SECRET('{secret_scope}', 'client_id'),",
        f"    client_secret => SECRET('{secret_scope}', 'client_secret'),",
        f"    tenant_id     => SECRET('{secret_scope}', 'tenant_id'),",
        f"    cdf_cluster   => SECRET('{secret_scope}', 'cdf_cluster'),",
        f"    project       => SECRET('{secret_scope}', 'project')",
        ") LIMIT 10;",
    ]
    cell3 = "\n".join(sql_lines)

    # Add alternative positional example as comment
    if view_property_names:
        positional_sql_lines = [
            "-- Alternative: Positional parameters (if you need to specify some properties)",
            f"SELECT * FROM {example_function_name}(",
            f"    SECRET('{secret_scope}', 'client_id'),",
            f"    SECRET('{secret_scope}', 'client_secret'),",
            f"    SECRET('{secret_scope}', 'tenant_id'),",
            f"    SECRET('{secret_scope}', 'cdf_cluster'),",
            f"    SECRET('{secret_scope}', 'project'),",
            "    -- View property parameters (all NULL to get all rows)",
        ]
        for i, prop in enumerate(view_property_names):
            comma = "," if i < len(view_property_names) - 1 else ""
            positional_sql_lines.append(f"    NULL{comma} -- {prop}")
        positional_sql_lines.append(") LIMIT 10;")
        cell3 += "\n\n" + "\n".join(positional_sql_lines)

    # All cells combined
    all_cells = f"""# ============================================================================
# NOTEBOOK CELL 1: Install Dependencies
# ============================================================================
{cell1}

# ============================================================================
# NOTEBOOK CELL 2: Register UDTFs
# ============================================================================
{cell2}

# ============================================================================
# NOTEBOOK CELL 3: SQL Query Example (Named Parameters - Recommended)
# ============================================================================
{cell3}"""

    return {
        "cell1_dependencies": cell1,
        "cell2_registration": cell2,
        "cell3_sql_example": cell3,
        "all_cells": all_cells,
    }


def _get_package_build_datetime(package_name: str) -> datetime | None:
    """Get build datetime for a package from embedded build timestamp.

    Args:
        package_name: Package name (e.g., 'cognite-pygen-spark' or 'cognite-databricks')

    Returns:
        datetime of the build, or None if not found
    """
    try:
        # Try to import the package and get build timestamp from _version
        if package_name == "cognite-pygen-spark":
            from cognite.pygen_spark import _version

            if hasattr(_version, "__build_timestamp__") and _version.__build_timestamp__:
                try:
                    return datetime.fromisoformat(_version.__build_timestamp__)
                except (ValueError, AttributeError, TypeError):
                    pass
        elif package_name == "cognite-databricks":
            from cognite.databricks import _version as databricks_version

            if hasattr(databricks_version, "__build_timestamp__") and databricks_version.__build_timestamp__:
                try:
                    return datetime.fromisoformat(databricks_version.__build_timestamp__)
                except (ValueError, AttributeError, TypeError):
                    pass

        return None
    except (ImportError, AttributeError):
        return None


def generate_udtf_notebook(
    data_model: DataModel,
    client: CogniteClient,
    workspace_client: WorkspaceClient | None = None,
    catalog: str = "main",
    schema: str | None = None,
    output_dir: Path | str | None = None,
    warehouse_id: str | None = None,
    debug: bool = False,
) -> UDTFGenerator:
    """Generate UDTFs for a Data Model in a notebook (aligned with pygen's generate_sdk_notebook).

    This function:
    1. Downloads the data model
    2. Generates UDTF code
    3. Creates a UDTFGenerator instance for registration

    Args:
        data_model: DataModel identifier (DataModelId or DataModel object)
        client: CogniteClient instance (can be created via load_cognite_client_from_toml)
        workspace_client: Optional WorkspaceClient (if None, will need to be set later)
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name. If None, auto-generates from data model:
                {space}_{external_id.lower()}_{version} (matches folder pattern)
        output_dir: Optional output directory path. If None, uses /local_disk0/tmp/pygen_udtf/{folder_name}.
                   Can be a string or Path object.
        warehouse_id: Optional SQL warehouse ID for view registration.
                     If None, will try to find a default warehouse when registering views.
        debug: If True, prints build datetime information for installed packages.

    Returns:
        UDTFGenerator instance ready for registration

    Example:
        from cognite.databricks import generate_udtf_notebook
        from cognite.pygen import load_cognite_client_from_toml
        from cognite.client.data_classes.data_modeling.ids import DataModelId

        client = load_cognite_client_from_toml("config.toml")
        data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
        generator = generate_udtf_notebook(
            data_model_id,
            client,
            output_dir="/local_disk0/tmp/pygen_udtf",  # Custom output directory
        )
        # Use data model-specific scope: cdf_{space}_{external_id}
        secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
        generator.register_udtfs_and_views(secret_scope=secret_scope)
    """
    # Create temporary output directory (similar to pygen's generate_sdk_notebook)
    # Extract identifier for folder name and schema name
    # Note: DataModelIdentifier supports tuples, but examples use DataModelId for clarity
    if isinstance(data_model, dm.DataModel):
        model_id = data_model.as_id()
        folder_name = f"{model_id.space}_{model_id.external_id}_{model_id.version}"
        # Auto-generate schema name if not provided (matches folder pattern but lowercase external_id)
        if schema is None:
            schema = f"{model_id.space}_{model_id.external_id.lower()}_{model_id.version}"
    elif isinstance(data_model, tuple):
        # Backward compatibility: DataModelIdentifier supports tuples
        if len(data_model) >= 3:
            folder_name = f"{data_model[0]}_{data_model[1]}_{data_model[2]}"
            if schema is None:
                schema = f"{data_model[0]}_{data_model[1].lower()}_{data_model[2]}"
        else:
            raise ValueError(f"Invalid tuple format for data_model: {data_model}")
    else:
        folder_name = f"{data_model.space}_{data_model.external_id}_{data_model.version}"
        if schema is None:
            schema = f"{data_model.space}_{data_model.external_id.lower()}_{data_model.version}"

    # Use provided output_dir or default to /local_disk0/tmp/pygen_udtf (Databricks writable location)
    if output_dir is None:
        output_dir = Path("/local_disk0/tmp/pygen_udtf") / folder_name
    else:
        output_dir = Path(output_dir) / folder_name

    output_dir.mkdir(parents=True, exist_ok=True)

    # Print build datetime information if debug=True
    if debug:
        print("=" * 60)
        print("[DEBUG] Package build datetime information")
        print("=" * 60)
        for pkg_name in ["cognite-pygen-spark", "cognite-databricks"]:
            build_dt = _get_package_build_datetime(pkg_name)
            if build_dt:
                print(f"[DEBUG] {pkg_name}: {build_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"[DEBUG] {pkg_name}: Unable to determine build datetime")
        print("=" * 60)

    # Create code generator - pass data_model here so it's loaded in __init__
    code_generator = SparkUDTFGenerator(
        client=client,
        output_dir=output_dir,
        data_model=data_model,  # Pass data_model here
        top_level_package="cognite_databricks",
    )

    # Generate UDTF files to disk
    # Note: __init__ only prepares the generator; generate_udtfs() actually writes files
    udtf_result = code_generator.generate_udtfs()

    # Also generate time series UDTF files using the same generator instance
    try:
        ts_result = code_generator.generate_time_series_udtfs()
        # Add to the result
        udtf_result.generated_files.update(ts_result.generated_files)
        udtf_result.total_count = len(udtf_result.generated_files)
    except (ValueError, AttributeError, KeyError) as e:
        print(f"[WARNING] Failed to generate time series UDTF files: {e}")

    # Create UDTFGenerator for registration
    return UDTFGenerator(
        workspace_client=workspace_client,
        cognite_client=client,
        catalog=catalog,
        schema=schema,
        code_generator=code_generator,
        warehouse_id=warehouse_id,
    )


class UDTFGenerator:
    """High-level API for generating and registering UDTFs and Views."""

    def __init__(
        self,
        workspace_client: WorkspaceClient | None = None,
        cognite_client: CogniteClient | None = None,
        catalog: str = "main",
        schema: str | None = None,
        code_generator: SparkUDTFGenerator | None = None,
        warehouse_id: str | None = None,
    ) -> None:
        """Initialize UDTF generator.

        Args:
            workspace_client: Optional Databricks WorkspaceClient
            cognite_client: Optional CogniteClient instance
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name. Should be provided or auto-generated from data_model.
            code_generator: Optional pre-configured SparkUDTFGenerator
            warehouse_id: Optional SQL warehouse ID for view registration.
                          If None, will try to find a default warehouse when registering views.
        """
        self.workspace_client = workspace_client
        self.cognite_client = cognite_client
        self.catalog = catalog
        if schema is None:
            raise ValueError("schema must be provided or auto-generated from data_model")
        self.schema = schema
        self.warehouse_id = warehouse_id

        if workspace_client:
            self.udtf_registry = UDTFRegistry(workspace_client)
            self.secret_helper = SecretManagerHelper(workspace_client)
        else:
            self.udtf_registry = None  # type: ignore[assignment]
            self.secret_helper = None  # type: ignore[assignment]

        # Note: If code_generator is None, we can't create SparkUDTFGenerator without a data_model
        # This should only happen if code_generator is provided or if generate_udtf_notebook was called
        if code_generator is None:
            raise ValueError("code_generator must be provided, or use generate_udtf_notebook() to create UDTFGenerator")
        self.code_generator = code_generator

    def _verify_udtfs_exist(
        self,
        view_ids: list[str],
        debug: bool = False,
    ) -> None:
        """Verify that all required UDTFs exist and are resolvable in Unity Catalog.

        This is a mandatory pre-test that runs before any views are created. If any
        required UDTF is missing, a ValueError is raised with a clear message listing
        all missing UDTFs.

        Args:
            view_ids: List of view IDs (UDTF names derived from these)
            debug: Enable debug output

        Raises:
            ValueError: If any required UDTF is missing, with detailed error message
        """
        from databricks.sdk.errors import DatabricksError, NotFound

        missing_udtfs: list[str] = []

        # Collect all UDTF names that are required
        for view_id in view_ids:
            udtf_name = to_udtf_function_name(view_id)
            full_function_name = f"{self.catalog}.{self.schema}.{udtf_name}"

            # Check if function exists in Unity Catalog
            if self.workspace_client is None:
                raise RuntimeError("workspace_client is required for UDTF verification")
            try:
                function_info = self.workspace_client.functions.get(full_function_name)
                if function_info:
                    if debug:
                        print(f"[DEBUG] ✓ UDTF {full_function_name} verified")
                else:
                    missing_udtfs.append(full_function_name)
            except NotFound:
                missing_udtfs.append(full_function_name)
            except DatabricksError as e:
                # For other Databricks SDK errors, also consider it missing
                if debug:
                    print(f"[DEBUG] Error checking UDTF {full_function_name}: {e}")
                missing_udtfs.append(full_function_name)

        # If any UDTFs are missing, raise error with clear message
        if missing_udtfs:
            missing_list = "\n  ".join(missing_udtfs)
            raise ValueError(
                f"Cannot create views: The following UDTFs are required but not found in Unity Catalog:\n"
                f"  {missing_list}\n\n"
                f"Please run register_udtfs() first in a separate notebook cell to register all UDTFs.\n"
                f"After UDTF registration completes successfully, you can then run register_views() in a new cell."
            )

    def _register_single_udtf_and_view(
        self,
        view_id: str,
        udtf_file: Path,
        view_sql: str | None,
        secret_scope: str,
        warehouse_id: str | None,
        if_exists: str,
        debug: bool,
        rate_limiter: Semaphore | None = None,
    ) -> RegisteredUDTFResult:
        """Register a single UDTF and its view (helper method for parallel execution).

        This method is called by ThreadPoolExecutor for parallel registration.
        Similar to how cognite-sdk-scala handles individual requests.

        Args:
            view_id: View external_id
            udtf_file: Path to the generated UDTF Python file
            view_sql: SQL CREATE VIEW statement (optional)
            secret_scope: Secret Manager scope name
            warehouse_id: SQL warehouse ID for view registration
            if_exists: What to do if UDTF already exists
            debug: Enable debug output
            rate_limiter: Optional Semaphore for rate limiting (similar to RateLimitingBackend)

        Returns:
            RegisteredUDTFResult for this view
        """
        # Apply rate limiting if specified (similar to cognite-sdk-scala's RateLimitingBackend)
        if rate_limiter:
            rate_limiter.acquire()
            try:
                return self._register_single_udtf_and_view_impl(
                    view_id, udtf_file, view_sql, secret_scope, warehouse_id, if_exists, debug
                )
            finally:
                rate_limiter.release()
        else:
            return self._register_single_udtf_and_view_impl(
                view_id, udtf_file, view_sql, secret_scope, warehouse_id, if_exists, debug
            )

    def _register_single_udtf_only(
        self,
        view_id: str,
        udtf_file: Path,
        if_exists: str,
        debug: bool,
    ) -> RegisteredUDTFResult:
        """Register a single UDTF only (without view) using SQL registration."""
        if not self.workspace_client:
            raise ValueError("WorkspaceClient must be set before SQL registration")

        function_name = to_udtf_function_name(view_id)
        udtf_code = udtf_file.read_text()
        udtf_class = self._extract_udtf_class_from_ast(udtf_code, udtf_file)

        input_params = self._parse_udtf_params(view_id, debug=debug, udtf_file=udtf_file)
        return_type = self._parse_return_type(view_id, udtf_file=udtf_file, debug=debug)

        self.udtf_registry.register_udtf_via_sql(
            catalog=self.catalog,
            schema=self.schema,
            function_name=function_name,
            udtf_code=udtf_code,
            input_params=input_params,
            return_type=return_type,
            handler_name=udtf_class.__name__,
            warehouse_id=None,
            comment=f"Auto-generated UDTF for {view_id}",
            if_exists=if_exists,
            debug=debug,
        )

        try:
            function_info = self.udtf_registry.workspace_client.functions.get(
                f"{self.catalog}.{self.schema}.{function_name}"
            )
        except Exception:
            function_info = None

        return RegisteredUDTFResult(
            view_id=view_id,
            function_info=function_info,
            view_name=None,
            udtf_file_path=udtf_file,
            view_registered=False,
        )

    def _register_single_udtf_and_view_impl(
        self,
        view_id: str,
        udtf_file: Path,
        view_sql: str | None,
        secret_scope: str,
        warehouse_id: str | None,
        if_exists: str,
        debug: bool,
    ) -> RegisteredUDTFResult:
        """Implementation of single UDTF registration (SQL/UC)."""
        if not self.workspace_client:
            raise ValueError("WorkspaceClient must be set before SQL registration")

        function_name = to_udtf_function_name(view_id)
        udtf_code = udtf_file.read_text()
        udtf_class = self._extract_udtf_class_from_ast(udtf_code, udtf_file)

        input_params = self._parse_udtf_params(view_id, debug=debug, udtf_file=udtf_file)
        return_type = self._parse_return_type(view_id, udtf_file=udtf_file, debug=debug)

        self.udtf_registry.register_udtf_via_sql(
            catalog=self.catalog,
            schema=self.schema,
            function_name=function_name,
            udtf_code=udtf_code,
            input_params=input_params,
            return_type=return_type,
            handler_name=udtf_class.__name__,
            warehouse_id=warehouse_id,
            comment=f"Auto-generated UDTF for {view_id}",
            if_exists=if_exists,
            debug=debug,
        )

        function_info = None
        try:
            function_info = self.udtf_registry.workspace_client.functions.get(
                f"{self.catalog}.{self.schema}.{function_name}"
            )
        except Exception:
            pass

        view_registered = False
        view_name = None
        if view_sql:
            try:
                self.udtf_registry.register_view(
                    catalog=self.catalog,
                    schema=self.schema,
                    view_name=view_id,
                    view_sql=view_sql,
                )
                view_registered = True
                view_name = f"{self.catalog}.{self.schema}.{view_id}"
            except Exception:
                view_registered = False

        return RegisteredUDTFResult(
            view_id=view_id,
            function_info=function_info,
            view_name=view_name,
            udtf_file_path=udtf_file,
            view_registered=view_registered,
        )

    def register_udtfs(
        self,
        data_model: DataModel | None = None,
        secret_scope: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
        max_workers: int = 5,
        max_parallel_requests: int | None = None,
    ) -> UDTFRegistrationResult:
        """Register UDTFs only (without views).

        This method registers UDTFs in Unity Catalog based on files in the output_dir.
        It does NOT generate new files - it uses whatever files already exist.

        Args:
            data_model: Optional DataModel identifier (only used for auto-generating secret_scope).
                       If None and secret_scope is None, raises ValueError.
            secret_scope: Secret Manager scope name. If None, auto-generates from data model:
                         `cdf_{space}_{external_id}` (e.g., "cdf_sp_pygen_power_windturbine")
            if_exists: What to do if UDTF already exists:
                      - "skip": Skip registration and return existing function (default)
                      - "replace": Delete and recreate the function
                      - "error": Raise ResourceAlreadyExists error
            debug: If True, prints detailed information about parameters and SQL being sent.
            max_workers: Maximum number of parallel worker threads for registration (default: 5).
            max_parallel_requests: Optional rate limiting via Semaphore.
                                  If None, no rate limiting is applied. If set, limits concurrent API calls.

        Returns:
            UDTFRegistrationResult with registered UDTF information (view_registered=False for all)

        Raises:
            ValueError: If PySpark version is less than 4.0.0 (required for vectorized UDTFs).
        """
        # Scalar-only SQL registration does not require PySpark.
        # Ensure schema exists before registering functions
        self._ensure_schema_exists()

        # Auto-generate scope name from data model if not provided
        if secret_scope is None:
            if data_model:
                if isinstance(data_model, dm.DataModel):
                    model_id = data_model.as_id()
                else:
                    model_id = data_model  # type: ignore[assignment]
                secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
            else:
                raise ValueError("secret_scope must be provided if data_model is None")

        # ALWAYS use files from output_dir - don't generate new files
        # Registration is based solely on what files exist in the output directory
        udtf_files = self._find_generated_udtf_files()

        registered_udtfs: list[RegisteredUDTFResult] = []

        if debug:
            print(f"\n{'=' * 60}")
            print("[DEBUG] Starting UDTF registration (Unity Catalog SQL)")
            print(f"[DEBUG] Catalog: {self.catalog}")
            print(f"[DEBUG] Schema: {self.schema}")
            print(f"[DEBUG] Secret scope: {secret_scope}")
            print(f"[DEBUG] if_exists: {if_exists}")
            print(f"[DEBUG] Max workers: {max_workers}")
            print(f"[DEBUG] Max parallel requests: {max_parallel_requests or 'unlimited'}")
            print(f"[DEBUG] UDTFs to register: {list(udtf_files.keys())}")
            print(f"{'=' * 60}\n")

        # Create rate limiter if specified
        rate_limiter = Semaphore(max_parallel_requests) if max_parallel_requests else None

        # Register UDTFs in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[Future[RegisteredUDTFResult], str] = {
                executor.submit(
                    self._register_single_udtf_only_with_rate_limit,
                    view_id,
                    udtf_file,
                    if_exists,
                    debug,
                    rate_limiter,
                ): view_id
                for view_id, udtf_file in udtf_files.items()
            }

            # Process results as they complete
            for future in as_completed(futures):
                view_id = futures[future]
                try:
                    result = future.result()
                    registered_udtfs.append(result)
                    if debug:
                        status = "✓" if result.function_info else "⚠"
                        print(f"[DEBUG] {status} Completed UDTF registration for {view_id}")
                except (ValueError, RuntimeError, AttributeError) as e:
                    # Handle errors per view (don't fail entire registration)
                    error_result = RegisteredUDTFResult(
                        view_id=view_id,
                        function_info=None,  # type: ignore[arg-type]
                        view_name=None,
                        udtf_file_path=udtf_files.get(view_id),
                        view_registered=False,
                    )
                    registered_udtfs.append(error_result)
                    print(f"[ERROR] Failed to register UDTF for {view_id}: {e}")
                    if debug:
                        import traceback

                        traceback.print_exc()

        # CRITICAL: Clean up any remaining PySpark mocks from sys.modules
        # This prevents "cannot import name 'proto' from 'connect'" errors at query time
        self._cleanup_pyspark_mocks()

        return UDTFRegistrationResult(
            registered_udtfs=registered_udtfs,
            catalog=self.catalog,
            schema_name=self.schema,
            total_count=len(registered_udtfs),
        )

    def _register_single_udtf_only_with_rate_limit(
        self,
        view_id: str,
        udtf_file: Path,
        if_exists: str,
        debug: bool,
        rate_limiter: Semaphore | None = None,
    ) -> RegisteredUDTFResult:
        """Register a single UDTF only (with rate limiting support)."""
        if rate_limiter:
            rate_limiter.acquire()
            try:
                return self._register_single_udtf_only(view_id, udtf_file, if_exists, debug)
            finally:
                rate_limiter.release()
        else:
            return self._register_single_udtf_only(view_id, udtf_file, if_exists, debug)

    def register_views(
        self,
        data_model: DataModel | None = None,
        secret_scope: str | None = None,
        warehouse_id: str | None = None,
        if_exists: str = "replace",
        debug: bool = False,
        max_workers: int = 5,
        max_parallel_requests: int | None = None,
    ) -> ViewRegistrationResult:
        """Register views for previously registered UDTFs.

        Generates view SQL based on UDTFs registered in Unity Catalog that are part of the schema
        created from the data model. Creates views for:
        - Data model UDTFs (from data model views)
        - Time series UDTFs (generated as part of the schema)

        Excludes any other UDTFs that might be registered in the same schema.

        Views pass secrets (via SECRET() calls) and parameters (view properties as NULL) to UDTFs.

        **CRITICAL**: This method queries Unity Catalog to find registered UDTFs, filters to only
        include data model and time series UDTFs, and creates views for them. It performs a pre-test
        validation to ensure all required UDTFs exist in Unity Catalog before attempting to create views.

        This method is designed to be called in a separate notebook cell after
        register_udtfs() has completed successfully.

        Args:
            data_model: Optional DataModel identifier (only used for generating view SQL for data model UDTFs).
                       If None, tries to use the data model from code_generator initialization.
            secret_scope: Secret Manager scope name. If None, auto-generates from data model:
                         `cdf_{space}_{external_id}` (e.g., "cdf_sp_pygen_power_windturbine")
            warehouse_id: Optional SQL warehouse ID for view registration.
                         If None, uses the warehouse_id from __init__ or tries to find a default warehouse.
            if_exists: What to do if view already exists:
                      - "skip": Skip registration if view exists
                      - "replace": Replace existing view (default)
                      - "error": Raise error if view exists
            debug: If True, prints detailed information about parameters and SQL being sent.
            max_workers: Maximum number of parallel worker threads for registration (default: 5).
            max_parallel_requests: Optional rate limiting via Semaphore.
                                  If None, no rate limiting is applied. If set, limits concurrent API calls.

        Returns:
            ViewRegistrationResult with registered view information

        Raises:
            ValueError: If any required UDTF is missing from Unity Catalog
        """
        if not self.workspace_client:
            raise ValueError("WorkspaceClient must be set before registration")

        # Ensure schema exists
        self._ensure_schema_exists()

        # Auto-generate scope name from data model if not provided
        if secret_scope is None:
            if data_model:
                if isinstance(data_model, dm.DataModel):
                    model_id = data_model.as_id()
                else:
                    model_id = data_model  # type: ignore[assignment]
                secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
            else:
                # Try to get from code_generator if available
                if hasattr(self.code_generator, "data_model") and self.code_generator.data_model:
                    if isinstance(self.code_generator.data_model, dm.DataModel):
                        model_id = self.code_generator.data_model.as_id()
                    else:
                        model_id = self.code_generator.data_model  # type: ignore[assignment]
                    secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
                else:
                    raise ValueError(
                        "secret_scope must be provided if data_model is None and code_generator has no data_model"
                    )

        # Query Unity Catalog to find registered UDTFs
        # We'll filter to only include data model and time series UDTFs
        registered_udtf_names: dict[str, str] = {}  # view_id -> full_udtf_name
        time_series_udtf_names: dict[str, str] = {}  # view_id -> full_udtf_name

        try:
            from cognite.databricks.models import time_series_udtf_registry

            if debug:
                print(f"[DEBUG] Querying Unity Catalog for registered UDTFs in {self.catalog}.{self.schema}...")

            # Collect all functions first for debugging
            all_functions = list(
                self.workspace_client.functions.list(
                    catalog_name=self.catalog,
                    schema_name=self.schema,
                )
            )

            if debug:
                print(f"[DEBUG] Total functions returned from Unity Catalog: {len(all_functions)}")
                time_series_funcs = [
                    f for f in all_functions if hasattr(f, "name") and f.name and "time_series" in f.name
                ]
                print(f"[DEBUG] Functions with 'time_series' in name: {[f.name for f in time_series_funcs]}")

            for func in all_functions:
                func_name = func.name if hasattr(func, "name") and func.name else None

                if debug and func_name and "time_series" in func_name:
                    print(f"[DEBUG] Processing function: {func_name}")
                    print(f"[DEBUG]   - ends with '_udtf': {func_name.endswith('_udtf') if func_name else False}")
                    print(f"[DEBUG]   - hasattr(func, 'name'): {hasattr(func, 'name')}")
                    if hasattr(func, "name"):
                        print(f"[DEBUG]   - func.name: {func.name}")

                if func_name and func_name.endswith("_udtf"):
                    # Extract view_id by removing _udtf suffix
                    view_id = func_name[:-5]  # Remove "_udtf" suffix

                    if debug and func_name and "time_series" in func_name:
                        print(f"[DEBUG]   - Extracted view_id: {view_id}")
                        print(f"[DEBUG]   - Calling time_series_udtf_registry.get_config('{func_name}')...")

                    # Check if this is a time series UDTF
                    config = time_series_udtf_registry.get_config(func_name)

                    if debug and func_name and "time_series" in func_name:
                        print(f"[DEBUG]   - get_config() returned: {config}")
                        print(f"[DEBUG]   - config is None: {config is None}")
                        if config:
                            print(
                                f"[DEBUG]   - Config details: udtf_name={config.udtf_name}, "
                                f"view_name={config.view_name}"
                            )

                    if config:
                        # This is a time series UDTF - include it
                        time_series_udtf_names[view_id] = func_name
                        if debug:
                            print(f"[DEBUG] ✓ Found time series UDTF: {func_name} -> view_id: {view_id}")
                    else:
                        # This might be a data model UDTF - we'll verify later when generating view SQL
                        registered_udtf_names[view_id] = func_name
                        if debug and func_name and "time_series" in func_name:
                            print(f"[DEBUG] ⚠ Time series function '{func_name}' NOT matched by registry!")

            if debug:
                print(
                    f"[DEBUG] Found {len(registered_udtf_names)} potential data model UDTF(s): "
                    f"{list(registered_udtf_names.keys())}"
                )
                print(
                    f"[DEBUG] Found {len(time_series_udtf_names)} time series UDTF(s): "
                    f"{list(time_series_udtf_names.keys())}"
                )
                if time_series_udtf_names:
                    print(f"[DEBUG] Time series UDTF details: {time_series_udtf_names}")

        except (RuntimeError, ValueError, AttributeError, ImportError) as e:
            if debug:
                print(f"[WARNING] Failed to query Unity Catalog for UDTFs: {e}")
            registered_udtf_names = {}
            time_series_udtf_names = {}

        if not registered_udtf_names and not time_series_udtf_names:
            if debug:
                print("[WARNING] No data model or time series UDTFs found in Unity Catalog. Cannot create views.")
            return ViewRegistrationResult(
                registered_views=[],
                catalog=self.catalog,
                schema_name=self.schema,
                total_count=0,
            )

        view_sqls: dict[str, str] = {}

        # Generate view SQL for data model UDTFs
        # Only include views for UDTFs that have matching view SQL from the data model
        if data_model:
            if debug:
                print("[DEBUG] === BRANCH: Using data_model parameter ===")
            try:
                view_sql_result = self.code_generator.generate_views(
                    data_model=data_model,
                    secret_scope=secret_scope,
                    catalog=self.catalog,
                    schema=self.schema,
                )
                if debug:
                    print(f"[DEBUG] generate_views() returned {len(view_sql_result.view_sqls)} view SQL(s)")
                    if view_sql_result.view_sqls:
                        sample_view_ids = list(view_sql_result.view_sqls.keys())[:5]
                        print(f"[DEBUG] Sample view_ids from generate_views(): {sample_view_ids}")

                # Only include views for registered UDTFs that have matching view SQL from data model
                # Note: view_id from generate_views is PascalCase (e.g., "SmallBoat"), but registered_udtf_names
                # uses snake_case (e.g., "small_boat"), so we need to convert for matching
                matched_count = 0
                for view_id, view_sql in view_sql_result.view_sqls.items():
                    # Convert PascalCase view_id to snake_case to match registered_udtf_names
                    # to_udtf_function_name converts "SmallBoat" -> "small_boat_udtf", so remove "_udtf" suffix
                    udtf_name = to_udtf_function_name(view_id)  # e.g., "small_boat_udtf"
                    snake_case_view_id = udtf_name[:-5]  # Remove "_udtf" suffix -> "small_boat"

                    if debug and matched_count < 3:
                        print(f"[DEBUG] Conversion: '{view_id}' -> '{udtf_name}' -> '{snake_case_view_id}'")
                        print(
                            f"[DEBUG] Checking if '{snake_case_view_id}' in registered_udtf_names: "
                            f"{snake_case_view_id in registered_udtf_names}"
                        )

                    if snake_case_view_id in registered_udtf_names:
                        view_sqls[view_id] = view_sql
                        matched_count += 1
                        if debug:
                            print(f"[DEBUG] ✓ Matched data model UDTF: {view_id} (snake_case: {snake_case_view_id})")

                if debug:
                    print(
                        f"[DEBUG] Total matched data model views: {matched_count} out of {len(view_sql_result.view_sqls)}"
                    )
            except (ValueError, AttributeError, KeyError) as e:
                if debug:
                    print(f"[WARNING] Failed to generate data model view SQL: {e}")
                    import traceback

                    traceback.print_exc()
        else:
            # Try to use data model from code_generator initialization
            # generate_views() will use self._data_model internally if data_model=None
            if debug:
                print("[DEBUG] === BRANCH: Trying code_generator.generate_views(data_model=None) ===")
            try:
                view_sql_result = self.code_generator.generate_views(
                    data_model=None,  # Use the one from __init__ (stored in _data_model)
                    secret_scope=secret_scope,
                    catalog=self.catalog,
                    schema=self.schema,
                )
                if debug:
                    print(f"[DEBUG] generate_views() returned {len(view_sql_result.view_sqls)} view SQL(s)")
                    if view_sql_result.view_sqls:
                        sample_view_ids = list(view_sql_result.view_sqls.keys())[:5]
                        print(f"[DEBUG] Sample view_ids from generate_views(): {sample_view_ids}")

                # Only include views for registered UDTFs that have matching view SQL from data model
                # Note: view_id from generate_views is PascalCase (e.g., "SmallBoat"), but registered_udtf_names
                # uses snake_case (e.g., "small_boat"), so we need to convert for matching
                matched_count = 0
                for view_id, view_sql in view_sql_result.view_sqls.items():
                    # Convert PascalCase view_id to snake_case to match registered_udtf_names
                    # to_udtf_function_name converts "SmallBoat" -> "small_boat_udtf", so remove "_udtf" suffix
                    udtf_name = to_udtf_function_name(view_id)  # e.g., "small_boat_udtf"
                    snake_case_view_id = udtf_name[:-5]  # Remove "_udtf" suffix -> "small_boat"

                    if debug and matched_count < 3:
                        print(f"[DEBUG] Conversion: '{view_id}' -> '{udtf_name}' -> '{snake_case_view_id}'")
                        print(
                            f"[DEBUG] Checking if '{snake_case_view_id}' in registered_udtf_names: "
                            f"{snake_case_view_id in registered_udtf_names}"
                        )

                    if snake_case_view_id in registered_udtf_names:
                        view_sqls[view_id] = view_sql
                        matched_count += 1
                        if debug:
                            print(f"[DEBUG] ✓ Matched data model UDTF: {view_id} (snake_case: {snake_case_view_id})")

                if debug:
                    print(
                        f"[DEBUG] Total matched data model views: {matched_count} out of {len(view_sql_result.view_sqls)}"
                    )
            except (ValueError, AttributeError, KeyError) as e:
                if debug:
                    print(f"[WARNING] Failed to generate data model view SQL: {e}")
                    import traceback

                    traceback.print_exc()
                # If generate_views fails, it means no data model is available
                if debug:
                    print("[WARNING] No data model available. Cannot generate view SQL for data model UDTFs.")

        # Generate view SQL for time series UDTFs
        try:
            # time_series_udtf_registry is already imported above, but import again for safety
            from cognite.databricks.models import time_series_udtf_registry

            for view_id, udtf_name in time_series_udtf_names.items():
                config = time_series_udtf_registry.get_config(udtf_name)
                if config:
                    # This is a time series UDTF - generate view SQL
                    # generate_time_series_udtf_view_sql is in the same module, call directly
                    view_sql = generate_time_series_udtf_view_sql(
                        udtf_name=udtf_name,
                        view_name=config.view_name,
                        secret_scope=secret_scope,
                        catalog=self.catalog,
                        schema=self.schema,
                        udtf_params=config.parameters,
                    )
                    # Use view_id (without _udtf) as the key to match UDTF registration
                    view_sqls[view_id] = view_sql
                    if debug:
                        print(f"[DEBUG] Generated view SQL for time series UDTF: {view_id}")
        except (ValueError, AttributeError, KeyError, ImportError) as e:
            if debug:
                print(f"[WARNING] Failed to generate time series UDTF view SQL: {e}")

        # MANDATORY: Pre-test validation - verify all required UDTFs exist
        # Since we queried Unity Catalog, they should all exist, but verify anyway
        view_ids = list(view_sqls.keys())
        if view_ids:
            if debug:
                print(f"[DEBUG] Pre-test: Verifying {len(view_ids)} UDTFs exist in Unity Catalog...")
            self._verify_udtfs_exist(view_ids, debug=debug)
            if debug:
                print("[DEBUG] ✓ All required UDTFs verified")

        registered_views: list[RegisteredViewResult] = []

        if debug:
            print(f"\n{'=' * 60}")
            print("[DEBUG] Starting view registration")
            print(f"[DEBUG] Catalog: {self.catalog}")
            print(f"[DEBUG] Schema: {self.schema}")
            print(f"[DEBUG] Secret scope: {secret_scope}")
            print(f"[DEBUG] if_exists: {if_exists}")
            print(f"[DEBUG] Max workers: {max_workers}")
            print(f"[DEBUG] Data model UDTFs: {list(registered_udtf_names.keys())}")
            print(f"[DEBUG] Time series UDTFs: {list(time_series_udtf_names.keys())}")
            print(f"[DEBUG] Views to register: {list(view_sqls.keys())}")
            print(f"{'=' * 60}\n")

        # Create rate limiter if specified
        rate_limiter = Semaphore(max_parallel_requests) if max_parallel_requests else None

        # Register views in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[Future[RegisteredViewResult], str] = {
                executor.submit(
                    self._register_single_view_with_rate_limit,
                    view_id,
                    view_sql,
                    warehouse_id or self.warehouse_id,
                    if_exists,
                    debug,
                    rate_limiter,
                ): view_id
                for view_id, view_sql in view_sqls.items()
            }

            # Process results as they complete
            for future in as_completed(futures):
                view_id = futures[future]
                try:
                    result = future.result()
                    registered_views.append(result)
                    if debug:
                        status = "✓" if result.view_registered else "⚠"
                        print(f"[DEBUG] {status} Completed view registration for {view_id}")
                except (ValueError, RuntimeError, AttributeError) as e:
                    # Handle errors per view (don't fail entire registration)
                    error_result = RegisteredViewResult(
                        view_id=view_id,
                        view_name=None,
                        view_registered=False,
                        error_message=str(e),
                    )
                    registered_views.append(error_result)
                    print(f"[ERROR] Failed to register view {view_id}: {e}")
                    if debug:
                        import traceback

                        traceback.print_exc()

        return ViewRegistrationResult(
            registered_views=registered_views,
            catalog=self.catalog,
            schema_name=self.schema,
            total_count=len(registered_views),
        )

    def _register_single_view_with_rate_limit(
        self,
        view_id: str,
        view_sql: str,
        warehouse_id: str | None,
        if_exists: str,
        debug: bool,
        rate_limiter: Semaphore | None = None,
    ) -> RegisteredViewResult:
        """Register a single view (with rate limiting support)."""
        if rate_limiter:
            rate_limiter.acquire()
            try:
                return self._register_single_view(view_id, view_sql, warehouse_id, if_exists, debug)
            finally:
                rate_limiter.release()
        else:
            return self._register_single_view(view_id, view_sql, warehouse_id, if_exists, debug)

    def _register_single_view(
        self,
        view_id: str,
        view_sql: str,
        warehouse_id: str | None,
        if_exists: str,
        debug: bool,
    ) -> RegisteredViewResult:
        """Register a single view.

        Args:
            view_id: View external_id
            view_sql: SQL CREATE VIEW statement
            warehouse_id: SQL warehouse ID for view registration
            if_exists: What to do if view already exists
            debug: Enable debug output

        Returns:
            RegisteredViewResult for this view
        """
        from cognite.databricks.models import time_series_udtf_registry

        # View SQL should already have catalog and schema from generate_views
        # But if placeholders are still present, replace them as fallback
        if "{{ catalog }}" in view_sql or "{{ schema }}" in view_sql:
            view_sql = view_sql.replace("{{ catalog }}", self.catalog).replace("{{ schema }}", self.schema)

        # Check if this is a time series UDTF view
        is_time_series_udtf = time_series_udtf_registry.get_config(view_id) is not None

        try:
            # For time series UDTFs, use the view name from the Pydantic registry
            if is_time_series_udtf:
                config = time_series_udtf_registry.get_config(view_id)
                if config:
                    actual_view_name = config.view_name
                    view_comment = f"Auto-generated View for {actual_view_name} (Time Series Datapoints)"
                else:
                    actual_view_name = view_id
                    view_comment = f"Auto-generated View for {view_id} (Time Series Datapoints)"
            else:
                actual_view_name = view_id
                view_comment = f"Auto-generated View for {view_id}"

            # Handle if_exists for views
            # Note: Unity Catalog doesn't have a direct "get view" API, so we'll try to create it
            # and catch the error if it already exists (handled in register_view call below)

            self.udtf_registry.register_view(
                catalog=self.catalog,
                schema=self.schema,
                view_name=actual_view_name,
                view_sql=view_sql,
                comment=view_comment,
                warehouse_id=warehouse_id,
                debug=debug,
            )
            view_name = f"{self.catalog}.{self.schema}.{actual_view_name}"

            # Register foreign key constraints for relationship properties (only for Data Model views)
            if not is_time_series_udtf:
                if debug:
                    print(f"[DEBUG] Extracting foreign key relationships for {view_id}")
                try:
                    self._extract_and_register_foreign_keys(
                        view_id=view_id,
                        warehouse_id=warehouse_id,
                        debug=debug,
                    )
                except (RuntimeError, ValueError, AttributeError) as e:
                    if debug:
                        print(f"[DEBUG] Failed to register foreign keys for {view_id}: {e}")

            return RegisteredViewResult(
                view_id=view_id,
                view_name=view_name,
                view_registered=True,
                error_message=None,
            )
        except (RuntimeError, ValueError) as e:
            # Check if it's a "view already exists" error and if_exists is "skip"
            error_msg = str(e).lower()
            if if_exists == "skip" and ("already exists" in error_msg or "exists" in error_msg):
                # View already exists, skip it
                view_name = (
                    f"{self.catalog}.{self.schema}.{actual_view_name if 'actual_view_name' in locals() else view_id}"
                )
                return RegisteredViewResult(
                    view_id=view_id,
                    view_name=view_name,
                    view_registered=False,  # Not newly registered, but exists
                    error_message=None,
                )
            else:
                if debug:
                    print(f"[DEBUG] Failed to register view {view_id}: {e}")
                return RegisteredViewResult(
                    view_id=view_id,
                    view_name=None,
                    view_registered=False,
                    error_message=str(e),
                )

    def register_udtfs_and_views(
        self,
        data_model: DataModel | None = None,
        secret_scope: str | None = None,
        warehouse_id: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
        max_workers: int = 5,
        max_parallel_requests: int | None = None,
    ) -> UDTFRegistrationResult:
        """Register UDTFs and Views in a single call (convenience method).

        This method calls register_udtfs() followed by register_views().
        For better control and error recovery, use register_udtfs() and
        register_views() separately in different notebook cells.

        Note: This method includes a delay between UDTF and view registration
        to allow Unity Catalog to propagate metadata. For production use, consider
        using register_udtfs() and register_views() in separate notebook cells.

        Args:
            data_model: Optional DataModel identifier (DataModelId or DataModel object).
                       If None, uses the data model from code_generator initialization.
            secret_scope: Secret Manager scope name. If None, auto-generates from data model:
                         `cdf_{space}_{external_id}` (e.g., "cdf_sp_pygen_power_windturbine")
            warehouse_id: Optional SQL warehouse ID for view registration.
                         If None, uses the warehouse_id from __init__ or tries to find a default warehouse.
            if_exists: What to do if UDTF/view already exists:
                      - "skip": Skip registration and return existing function/view (default)
                      - "replace": Delete and recreate the function/view
                      - "error": Raise ResourceAlreadyExists error
            debug: If True, prints detailed information about parameters and SQL being sent.
            max_workers: Maximum number of parallel worker threads for registration (default: 5).
            max_parallel_requests: Optional rate limiting via Semaphore.
                                  If None, no rate limiting is applied. If set, limits concurrent API calls.

        Returns:
            UDTFRegistrationResult with structured information about registered UDTFs and views.
            Access individual results via result['view_id'] or result.get('view_id').

        Raises:
            ValueError: If PySpark version is less than 4.0.0 (required for vectorized UDTFs).
        """
        # Register UDTFs first
        udtf_result = self.register_udtfs(
            data_model=data_model,
            secret_scope=secret_scope,
            if_exists=if_exists,
            debug=debug,
            max_workers=max_workers,
            max_parallel_requests=max_parallel_requests,
        )

        if debug:
            print("[DEBUG] Skipping view registration: session-scoped UDTFs are not available in Unity Catalog")

        return udtf_result

    def register_session_scoped_udtfs(
        self,
        data_model: DataModel | None = None,
        spark_session: object | None = None,  # SparkSession type
        function_name_prefix: str | None = None,
    ) -> dict[str, str]:
        """Register all generated UDTFs for session-scoped use in PySpark.

        This method registers UDTFs for use within the current Spark session,
        without requiring Unity Catalog registration. This is useful for:
        - Testing UDTFs before Unity Catalog registration
        - Development and prototyping
        - Environments where dependencies can be installed via %pip

        Args:
            data_model: Optional DataModel identifier. If None, uses the data model from code_generator.
            spark_session: Optional SparkSession. If None, uses the active SparkSession.
            function_name_prefix: Optional prefix for function names. If None, uses view external_id.

        Returns:
            Dictionary mapping view external_id to registered function name

        Example:
            # Generate UDTFs
            generator = generate_udtf_notebook(data_model_id, client, ...)

            # Install dependencies (run in separate cell)
            # %pip install cognite-sdk
            # (Restart kernel after installation)

            # Register all UDTFs for session-scoped use
            registered = generator.register_session_scoped_udtfs()
            # Returns: {"SmallBoat": "smallboat_udtf", "LargeBoat": "largeboat_udtf"}

            # Use in SQL
            # SELECT * FROM smallboat_udtf(...)
        """
        # Import here to avoid requiring PySpark at module level
        try:
            from pyspark.sql import SparkSession  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(
                "PySpark is required for session-scoped UDTF registration. "
                "Make sure you're running this in a Databricks notebook or have PySpark installed."
            ) from e

        if spark_session is None:
            spark_session = SparkSession.getActiveSession()
            if spark_session is None:
                raise RuntimeError(
                    "No active SparkSession found. Please provide spark_session parameter "
                    "or ensure you're running in a Databricks notebook with an active Spark session."
                )

        # Get generated UDTF files
        udtf_files: dict[str, Path] = {}
        if data_model:
            # Generate UDTF code (will use the provided data_model)
            udtf_result = self.code_generator.generate_udtfs(data_model)
            # For session-scoped, we need files from session_scoped/ directory
            # The result contains both _session and _catalog files, filter for _session
            udtf_files = {
                view_id.replace("_session", ""): file_path
                for view_id, file_path in udtf_result.generated_files.items()
                if view_id.endswith("_session")
            }
        else:
            # Use the data model from code_generator initialization
            # UDTF code was already generated in __init__, look in session_scoped/ directory
            session_dir = self.code_generator.output_dir / "session_scoped" / self.code_generator.top_level_package
            if not session_dir.exists():
                raise FileNotFoundError(
                    f"Session-scoped UDTF directory not found: {session_dir}. "
                    "Make sure generate_udtfs() was called first."
                )
            for file_path in session_dir.glob("*_udtf.py"):
                view_id = file_path.stem.replace("_udtf", "")
                udtf_files[view_id] = file_path

        registered_functions: dict[str, str] = {}

        # Register each UDTF
        for view_id, udtf_file in udtf_files.items():
            # Determine function name
            if function_name_prefix:
                # Use consistent snake_case conversion even with prefix
                base_name = to_udtf_function_name(view_id)  # e.g., "small_boat_udtf"
                function_name = f"{function_name_prefix}_{base_name}"
            else:
                # Extract from view_id: SmallBoat -> small_boat_udtf using pygen-main's to_snake
                function_name = to_udtf_function_name(view_id)

            try:
                registered_name = register_udtf_from_file(
                    udtf_file,
                    function_name=function_name,
                    spark_session=spark_session,
                )
                registered_functions[view_id] = registered_name
            except (ValueError, AttributeError, FileNotFoundError, ImportError) as e:
                print(f"[ERROR] Failed to register {view_id} as {function_name}: {e}")
                # Continue with other UDTFs even if one fails
                continue

        print(f"\n✓ Registered {len(registered_functions)} UDTF(s) for session-scoped use:")
        for view_id, func_name in registered_functions.items():
            print(f"  - {view_id} -> {func_name}")

        return registered_functions

    def _find_generated_udtf_files(self) -> dict[str, Path]:
        """Find generated UDTF files in output directory.

        Looks in catalog_registered/ subdirectory for Unity Catalog registration.

        Returns:
            Dict mapping view external_id to Path of UDTF file
        """
        if not self.code_generator:
            raise ValueError("code_generator must be set to find generated files")

        # Files are stored in: output_dir / catalog_registered / top_level_package / "{view_external_id}_udtf.py"
        catalog_dir = self.code_generator.output_dir / "catalog_registered" / self.code_generator.top_level_package

        # Fallback to old structure for backward compatibility
        if catalog_dir.exists():
            udtf_dir = catalog_dir
        else:
            udtf_dir = self.code_generator.output_dir / self.code_generator.top_level_package
            if not udtf_dir.exists():
                raise FileNotFoundError(
                    f"UDTF directory not found: {catalog_dir} or {udtf_dir}. Make sure generate_udtfs() was called first."
                )

        # Find all *_udtf.py files
        udtf_files: dict[str, Path] = {}
        for file_path in udtf_dir.glob("*_udtf.py"):
            # Extract view external_id from filename: "{view_external_id}_udtf.py"
            view_id = file_path.stem.replace("_udtf", "")
            udtf_files[view_id] = file_path

        if not udtf_files:
            raise FileNotFoundError(f"No UDTF files found in {udtf_dir}. Make sure generate_udtfs() was called first.")

        return udtf_files

    def _ensure_catalog_exists(self) -> None:
        """Create catalog if it doesn't exist.

        Checks if the catalog exists, and creates it if it doesn't.
        """
        if not self.workspace_client:
            return

        from databricks.sdk.errors import NotFound

        try:
            # Try to get the catalog - if it exists, we're done
            self.workspace_client.catalogs.get(self.catalog)
        except NotFound:
            # Catalog doesn't exist, create it
            try:
                self.workspace_client.catalogs.create(
                    name=self.catalog,
                    comment="Catalog for CDF Data Model UDTFs and Views",
                )
            except (RuntimeError, ValueError) as e:
                # If creation fails, it might be a permission issue or catalog already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.catalogs.get(self.catalog)
                except NotFound:
                    # Re-raise the original creation error
                    raise RuntimeError(
                        f"Failed to create catalog '{self.catalog}'. "
                        f"Ensure you have CREATE_CATALOG permission. "
                        f"Original error: {e}"
                    ) from e

    def _ensure_schema_exists(self) -> None:
        """Create schema if it doesn't exist.

        Checks if the schema exists in the catalog, and creates it if it doesn't.
        """
        if not self.workspace_client:
            return

        # First ensure the catalog exists
        self._ensure_catalog_exists()

        from databricks.sdk.errors import NotFound

        try:
            # Try to get the schema - if it exists, we're done
            full_name = f"{self.catalog}.{self.schema}"
            self.workspace_client.schemas.get(full_name)
        except NotFound:
            # Schema doesn't exist, create it
            try:
                self.workspace_client.schemas.create(
                    name=self.schema,
                    catalog_name=self.catalog,
                    comment="Schema for CDF Data Model UDTFs and Views",
                )
            except (RuntimeError, ValueError) as e:
                # If creation fails, it might be a permission issue or schema already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.schemas.get(full_name)
                except NotFound:
                    # Re-raise the original creation error
                    raise RuntimeError(
                        f"Failed to create schema {full_name}. "
                        f"Ensure you have CREATE_SCHEMA permission on catalog '{self.catalog}'. "
                        f"Original error: {e}"
                    ) from e

    def _get_data_model_from_files(self) -> DataModel:
        """Extract data model identifier from generated files."""
        # Placeholder implementation
        # Real implementation would parse generated files to extract data model info
        raise NotImplementedError("_get_data_model_from_files not yet implemented")

    def _extract_udtf_class_from_ast(
        self,
        udtf_code: str,
        udtf_file: Path,
    ) -> type:
        """Extract UDTF class from code using AST parsing (pygen-main style).

        Uses AST to find the class definition without executing decorators.
        Then executes with a mock decorator to get the class object.

        Args:
            udtf_code: UDTF Python code as string
            udtf_file: Path to UDTF file (for error messages)

        Returns:
            UDTF class object (not decorated)
        """
        import ast

        # CRITICAL: Remove @udtf decorator BEFORE exec() to prevent PySpark validation
        # The decorator causes validation errors during exec() even with mocks
        # We only need the class, not the decorated version, for AST extraction
        udtf_code_for_exec = self._remove_udtf_decorator(udtf_code)

        # Parse AST to find the class
        try:
            tree = ast.parse(udtf_code_for_exec)
        except SyntaxError as e:
            raise ValueError(f"Invalid Python syntax in {udtf_file}: {e}") from e

        # Find UDTF class (class with 'eval' method - 'analyze' is optional)
        udtf_class_name: str | None = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if this class has eval method (analyze is optional)
                has_eval = any(isinstance(item, ast.FunctionDef) and item.name == "eval" for item in node.body)
                if has_eval:
                    udtf_class_name = node.name
                    break

        if not udtf_class_name:
            raise ValueError(f"No UDTF class found in {udtf_file}. Expected a class with 'eval' method.")

        # Execute code WITHOUT decorator to get the class (pygen-main style: execute but control environment)
        # Create a mock udtf decorator that returns the class unchanged
        def _mock_udtf(*args, **kwargs):
            """Mock udtf decorator that returns the class unchanged."""

            def decorator(cls):
                return cls

            return decorator

        # Prepare namespace with mock decorator (pygen-main pattern: control execution environment)
        namespace: dict[str, object] = {
            "__builtins__": __builtins__,
        }

        # Add mock pyspark.sql.functions.udtf
        # We need to inject into sys.modules so that 'from pyspark.sql.functions import udtf' uses our mock
        # Also mock pyspark.sql.connect to prevent PySpark Connect validation during exec()
        import sys

        # Create mock modules
        mock_functions = type(sys)("functions")
        mock_functions.udtf = _mock_udtf  # type: ignore[attr-defined]
        mock_sql = type(sys)("sql")
        mock_sql.functions = mock_functions  # type: ignore[attr-defined]

        # Mock pyspark.sql.connect to prevent PySpark Connect validation
        # PySpark Connect's udtf decorator imports from pyspark.sql.connect.udtf
        # We need to mock this entire module to prevent validation
        mock_connect = type(sys)("connect")
        mock_connect_udtf = type(sys)("udtf")

        # Create a mock _create_py_udtf that just returns the class unchanged
        def _mock_create_py_udtf(
            cls: type,
            returnType: object | None = None,
            name: str | None = None,
            eval_type: object | None = None,
            deterministic: bool | None = None,
        ) -> type:
            """Mock _create_py_udtf that bypasses validation."""
            return cls

        mock_connect_udtf._create_py_udtf = _mock_create_py_udtf
        mock_connect.udtf = mock_connect_udtf
        mock_sql.connect = mock_connect

        # Also mock pyspark.sql.udtf (the module containing _validate_udtf_handler)
        # This is critical - _validate_udtf_handler is called during UserDefinedTableFunction.__init__
        mock_pyspark_sql_udtf = type(sys)("udtf")

        # Mock _validate_udtf_handler to do nothing (bypass validation)
        def _mock_validate_udtf_handler(func, returnType):
            """Mock _validate_udtf_handler that bypasses validation."""
            pass  # Do nothing, bypass validation

        mock_pyspark_sql_udtf._validate_udtf_handler = _mock_validate_udtf_handler
        mock_sql.udtf = mock_pyspark_sql_udtf  # Inject into pyspark.sql

        mock_pyspark = type(sys)("pyspark")
        mock_pyspark.sql = mock_sql

        # Store original modules (if they exist) to restore later
        original_modules = {}
        for module_name in [
            "pyspark",
            "pyspark.sql",
            "pyspark.sql.functions",
            "pyspark.sql.connect",
            "pyspark.sql.connect.udtf",
            "pyspark.sql.udtf",
        ]:
            if module_name in sys.modules:
                original_modules[module_name] = sys.modules[module_name]

        # Inject into sys.modules so imports use our mocks
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.sql"] = mock_sql
        sys.modules["pyspark.sql.functions"] = mock_functions
        sys.modules["pyspark.sql.connect"] = mock_connect
        sys.modules["pyspark.sql.connect.udtf"] = mock_connect_udtf
        sys.modules["pyspark.sql.udtf"] = mock_pyspark_sql_udtf  # Inject mock for pyspark.sql.udtf

        # Also add to namespace for direct access
        namespace["pyspark"] = mock_pyspark
        # Add udtf directly to namespace so 'from pyspark.sql.functions import udtf' uses our mock
        namespace["udtf"] = _mock_udtf

        # Execute code WITHOUT decorator - no validation will occur
        try:
            exec(udtf_code_for_exec, namespace)
        finally:
            # ALWAYS restore original modules first (even if they were None)
            for module_name, original_module in original_modules.items():
                sys.modules[module_name] = original_module

            # Then remove any mocks that weren't there before
            # Use a list to avoid modification during iteration
            modules_to_remove = []
            for module_name in [
                "pyspark.sql.udtf",
                "pyspark.sql.connect.udtf",
                "pyspark.sql.connect",
                "pyspark.sql.functions",
                "pyspark.sql",
                "pyspark",
            ]:
                if module_name not in original_modules and module_name in sys.modules:
                    current_module = sys.modules[module_name]
                    # Check if it's one of our mocks by object identity
                    if (
                        current_module is mock_pyspark
                        or current_module is mock_sql
                        or current_module is mock_functions
                        or current_module is mock_connect
                        or current_module is mock_connect_udtf
                        or current_module is mock_pyspark_sql_udtf
                    ):
                        modules_to_remove.append(module_name)

            # Remove our mocks to force re-import on next access
            for module_name in modules_to_remove:
                del sys.modules[module_name]

            # Force re-import of real PySpark modules to ensure they're available
            # This prevents "cannot import name 'proto' from 'connect'" errors at query time
            import importlib

            # Re-import in reverse order (dependencies first)
            for module_name in reversed(modules_to_remove):
                try:
                    importlib.import_module(module_name)
                except Exception:
                    # If re-import fails, that's okay - next access will import the real module
                    pass

        # Get the class (should be unchanged due to mock decorator)
        if udtf_class_name not in namespace:
            raise ValueError(f"Class {udtf_class_name} not found in namespace after execution")

        udtf_class = namespace[udtf_class_name]

        if not isinstance(udtf_class, type):
            raise ValueError(
                f"Expected {udtf_class_name} to be a class, got {type(udtf_class).__name__}. "
                "The @udtf decorator may have wrapped it. Check mock decorator implementation."
            )

        return udtf_class

    def _cleanup_pyspark_mocks(self) -> None:
        """Force cleanup of any remaining PySpark mocks from sys.modules.

        This should be called after all UDTF registrations are complete
        to ensure PySpark modules are restored for query execution.
        """
        import importlib
        import sys

        pyspark_modules = [
            "pyspark.sql.udtf",
            "pyspark.sql.connect.udtf",
            "pyspark.sql.connect",
            "pyspark.sql.functions",
            "pyspark.sql",
            "pyspark",
        ]

        # Check if any of these are mock objects (don't have __file__ or __path__)
        modules_to_reload = []
        for module_name in pyspark_modules:
            if module_name in sys.modules:
                mod = sys.modules[module_name]
                # Real modules have __file__ or __path__ attribute
                if not (hasattr(mod, "__file__") or hasattr(mod, "__path__")):
                    # This is likely a mock - remove it
                    del sys.modules[module_name]
                    modules_to_reload.append(module_name)

        # Force re-import of real modules in reverse order (dependencies first)
        for module_name in reversed(modules_to_reload):
            try:
                importlib.import_module(module_name)
            except Exception:
                pass  # Will be imported on next access

    def _spark_type_to_code_string(self, spark_type: DataType) -> str:
        """Convert PySpark DataType to code string for use in decorator returnType.

        Args:
            spark_type: PySpark DataType

        Returns:
            Code string like "StringType()", "ArrayType(StringType())", etc.
        """
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            TimestampType,
        )

        if isinstance(spark_type, StringType):
            return "StringType()"
        elif isinstance(spark_type, IntegerType):
            return "IntegerType()"
        elif isinstance(spark_type, LongType):
            return "LongType()"
        elif isinstance(spark_type, DoubleType):
            return "DoubleType()"
        elif isinstance(spark_type, BooleanType):
            return "BooleanType()"
        elif isinstance(spark_type, DateType):
            return "DateType()"
        elif isinstance(spark_type, TimestampType):
            return "TimestampType()"
        elif isinstance(spark_type, ArrayType):
            element_code = self._spark_type_to_code_string(spark_type.elementType)
            contains_null = "True" if spark_type.containsNull else "False"
            return f"ArrayType({element_code}, containsNull={contains_null})"
        else:
            # Default fallback
            return "StringType()"

    def _inject_return_type_into_decorator(self, udtf_code: str, udtf_class: type, debug: bool = False) -> str:
        """Inject returnType into @udtf decorator to satisfy PySpark validation.

        This method is no longer used since we removed Arrow mode.
        Kept for backward compatibility but does nothing.

        Args:
            udtf_code: Original UDTF Python code
            udtf_class: UDTF class object (must have outputSchema() method)
            debug: Enable debug output

        Returns:
            Unmodified UDTF code (scalar mode doesn't need decorator injection)
        """
        # Scalar mode doesn't need decorator injection - return as-is
        return udtf_code

    def _remove_udtf_decorator(self, udtf_code: str) -> str:
        """Remove @udtf decorator from code for catalog registration.

        Unity Catalog uses SQL RETURNS TABLE, so decorator is not needed.
        The decorator causes ImportError when Unity Catalog executes the UDTF.

        Args:
            udtf_code: UDTF Python code with @udtf decorator

        Returns:
            UDTF code with @udtf decorator removed
        """
        lines = udtf_code.split("\n")
        result_lines = []
        skip_next_empty = False

        for i, line in enumerate(lines):
            # Skip lines containing @udtf decorator
            if "@udtf(" in line or line.strip().startswith("@udtf"):
                skip_next_empty = True
                continue

            # Skip empty line immediately after decorator
            if skip_next_empty and not line.strip():
                continue
            skip_next_empty = False

            # Skip import line for udtf
            if "from pyspark.sql.functions import udtf" in line:
                continue

            result_lines.append(line)

        return "\n".join(result_lines)

    def _parse_udtf_params_from_class(
        self,
        udtf_class: type,
        debug: bool = False,
    ) -> list[FunctionParameterInfo]:
        """Parse UDTF function parameters directly from a UDTF class.

        Uses analyze() method if available, otherwise falls back to eval() method.
        For catalog-registered UDTFs without analyze(), parses from eval() signature.

        Args:
            udtf_class: UDTF class (should have analyze() or eval() method)
            debug: If True, prints parameter details

        Returns:
            List of FunctionParameterInfo objects matching the UDTF function signature
        """
        import inspect

        # Try analyze() first, fall back to eval() for catalog-registered UDTFs
        if hasattr(udtf_class, "analyze"):
            method = udtf_class.analyze
            method_sig = inspect.signature(method)
        elif hasattr(udtf_class, "eval"):
            method = udtf_class.eval
            method_sig = inspect.signature(method)
        else:
            raise ValueError(f"UDTF class {udtf_class.__name__} must have either an analyze() or eval() method")

        input_params: list[FunctionParameterInfo] = []
        position = 0

        if debug:
            method_name = "analyze" if hasattr(udtf_class, "analyze") else "eval"
            print(f"\n[DEBUG] === Parsing parameters from {udtf_class.__name__}.{method_name}() ===")

        # Add secret parameters first (same as view UDTFs)
        secret_params = [
            ("client_id", "STRING", "string"),
            ("client_secret", "STRING", "string"),
            ("tenant_id", "STRING", "string"),
            ("cdf_cluster", "STRING", "string"),
            ("project", "STRING", "string"),
        ]

        if debug:
            print("[DEBUG] Secret parameters (5):")

        for param_name, sql_type, _spark_type_name in secret_params:
            spark_type = StringType()
            # Unity Catalog expects DataType JSON, not StructField JSON
            type_json_value = TypeConverter.spark_to_datatype_json(spark_type)
            if debug:
                print(f"  [{position}] {param_name}: type_text='{sql_type}', type_name=STRING")
            input_params.append(
                FunctionParameterInfo(
                    name=param_name,
                    type_text=sql_type,
                    type_name=ColumnTypeName.STRING,
                    type_json=type_json_value,
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                )
            )
            position += 1

        # Add other parameters from method signature (excluding self and secret params)
        if debug:
            print("[DEBUG] UDTF-specific parameters:")

        secret_param_names = {p[0] for p in secret_params}
        for param_name, param in method_sig.parameters.items():
            # Skip 'self' parameter (only in eval())
            if param_name == "self":
                continue
            if param_name in secret_param_names:
                continue  # Already added

            # Infer type from annotation or default value
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else None
            default_value = param.default if param.default != inspect.Parameter.empty else None

            # Special handling for timestamp parameters (start, end, before) - register as TIMESTAMP
            # These accept SQL TIMESTAMP, relative time strings, ISO 8601, or milliseconds
            if param_name in ("start", "end", "before"):
                from pyspark.sql.types import TimestampType

                param_spark_type = TimestampType()
            # For scalar mode, infer type from annotation or default to STRING
            elif param_type is None or param_type is type(None):
                param_spark_type = StringType()
            elif hasattr(param_type, "__origin__"):  # Union types like str | None
                # Extract the non-None type
                args = getattr(param_type, "__args__", ())
                non_none_types = [a for a in args if a is not type(None)]
                if non_none_types:
                    param_type = non_none_types[0]
                else:
                    param_type = str
                param_spark_type = TypeConverter.python_type_to_spark(param_type)  # type: ignore[assignment]
            else:
                param_spark_type = TypeConverter.python_type_to_spark(param_type)  # type: ignore[assignment]

            # Convert PySpark type to SQL type info
            sql_type, type_name = TypeConverter.spark_to_sql_type_info(param_spark_type)
            # Unity Catalog expects DataType JSON, not StructField JSON
            type_json_value = TypeConverter.spark_to_datatype_json(param_spark_type)
            if debug:
                print(
                    f"  [{position}] {param_name}: type_text='{sql_type}', "
                    f"type_name={type_name}, default={default_value}"
                )

            input_params.append(
                FunctionParameterInfo(
                    name=param_name,
                    type_text=sql_type,
                    type_name=type_name,
                    type_json=type_json_value,
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                    parameter_default="NULL",  # Make all non-secret params optional in SQL
                )
            )
            position += 1

        return input_params

    def _parse_return_type_from_class(
        self,
        udtf_class: type,
    ) -> str:
        """Parse UDTF return type directly from a UDTF class.

        This is used for time series UDTFs that don't have a corresponding view.
        Uses the outputSchema() method to get the return type.

        Args:
            udtf_class: UDTF class (must have outputSchema() method)

        Returns:
            SQL return type string (e.g., "TABLE(col1 STRING, col2 INT)")
        """
        if not hasattr(udtf_class, "outputSchema"):
            raise ValueError(f"UDTF class {udtf_class.__name__} must have an outputSchema() method")

        struct_type = udtf_class.outputSchema()
        return str(TypeConverter.struct_type_to_ddl(struct_type))  # type: ignore[no-any-return]

    def _parse_return_params_from_class(
        self,
        udtf_class: type,
        debug: bool = False,
    ) -> list[FunctionParameterInfo]:
        """Parse structured return parameters directly from a UDTF class.

        This is used for time series UDTFs that don't have a corresponding view.
        Uses the outputSchema() method to get the return columns.

        Args:
            udtf_class: UDTF class (must have outputSchema() method)
            debug: If True, prints parameter details

        Returns:
            List of FunctionParameterInfo objects for the UDTF return columns
        """
        if not hasattr(udtf_class, "outputSchema"):
            raise ValueError(f"UDTF class {udtf_class.__name__} must have an outputSchema() method")

        struct_type = udtf_class.outputSchema()
        return_params: list[FunctionParameterInfo] = []

        if debug:
            print(f"[DEBUG] Parsing return parameters from {udtf_class.__name__}:")
            print(f"[DEBUG] Using StructType with {len(struct_type.fields)} fields")

        for position, field in enumerate(struct_type.fields):
            sql_type, type_name = TypeConverter.spark_to_sql_type_info(field.dataType)
            # Unity Catalog expects DataType JSON, not StructField JSON
            type_json_value = TypeConverter.spark_to_datatype_json(field.dataType)

            if debug:
                print(
                    f"  [{position}] {field.name}: type_text='{sql_type}', "
                    f"type_name={type_name}, nullable={field.nullable}"
                )

            return_params.append(
                FunctionParameterInfo(
                    name=field.name,
                    type_text=sql_type,
                    type_name=type_name,
                    type_json=type_json_value,
                    position=position,
                    # Return parameters use COLUMN type, not PARAM
                    # parameter_mode should be None for return columns
                    parameter_mode=None,
                    parameter_type=FunctionParameterType.COLUMN,
                )
            )

        return return_params

    def _parse_udtf_params(
        self,
        view_id: str,
        debug: bool = False,
        udtf_file: Path | None = None,
    ) -> list[FunctionParameterInfo]:
        """Parse UDTF function parameters.

        The UDTF function signature includes:
        1. Secret parameters (client_id, client_secret, tenant_id, cdf_cluster, project)
           These are passed via SECRET() calls in the view SQL
        2. View property parameters (from the data model view)

        Args:
            view_id: View external_id
            debug: If True, prints parameter details

        Returns:
            List of FunctionParameterInfo objects matching the UDTF function signature
        """
        # Get the view from code_generator's data model
        view = self._get_view_by_id(view_id)

        if not view:
            if udtf_file is None:
                raise ValueError(f"View '{view_id}' not found in data model")
            udtf_code = udtf_file.read_text()
            udtf_class = self._extract_udtf_class_from_ast(udtf_code, udtf_file)
            return self._parse_udtf_params_from_class(udtf_class, debug=debug)

        input_params: list[FunctionParameterInfo] = []
        position = 0

        if debug:
            print(f"\n[DEBUG] === Parsing parameters for {view_id} ===")

        # Add secret parameters first (matching the view SQL template)
        # These are passed via SECRET() calls when the view is queried
        # The notebook loads secrets from TOML and stores them in Secret Manager
        # IMPORTANT: type_json must use Spark StructField JSON format
        # This format was discovered by inspecting system.ai.python_exec function
        # which uses this exact structure. The "name" field must match the parameter name.
        secret_params = [
            ("client_id", "STRING", "string"),
            ("client_secret", "STRING", "string"),
            ("tenant_id", "STRING", "string"),
            ("cdf_cluster", "STRING", "string"),
            ("project", "STRING", "string"),
        ]

        if debug:
            print("[DEBUG] Secret parameters (5):")

        for param_name, sql_type, _spark_type_name in secret_params:
            # Unity Catalog expects DataType JSON, not StructField JSON
            spark_type = StringType()  # All secret params are strings
            type_json_value = TypeConverter.spark_to_datatype_json(spark_type)
            if debug:
                print(
                    f"  [{position}] {param_name}: type_text='{sql_type}', "
                    f"type_name=STRING, type_json='{type_json_value}'"
                )
            input_params.append(
                FunctionParameterInfo(
                    name=param_name,
                    type_text=sql_type,
                    type_name=ColumnTypeName.STRING,
                    type_json=type_json_value,  # Spark StructField JSON format
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                )
            )
            position += 1

        if debug:
            print(f"[DEBUG] View property parameters ({len(view.properties)}):")

        # Add view property parameters
        for prop_name, prop in view.properties.items():
            property_type, is_relationship, is_multi = self._get_property_type(prop)

            if is_relationship:
                # Represent relationships as STRING (external_id or JSON list for multi).
                sql_type = "STRING"
                type_name = ColumnTypeName.STRING
                spark_type = StringType()
                # Unity Catalog expects DataType JSON, not StructField JSON
                type_json_value = TypeConverter.spark_to_datatype_json(spark_type)
                if debug:
                    relation_kind = "multi relation (JSON list)" if is_multi else "single relation (external_id)"
                    print(
                        f"  [{position}] {prop_name}: type_text='STRING', "
                        f"type_name=ColumnTypeName.STRING ({relation_kind})"
                    )
            else:
                # Use PySpark as source of truth - build Spark type first, then derive SQL type
                prop_spark_type = TypeConverter.cdf_to_spark(property_type, is_array=is_multi)  # type: ignore[assignment]
                sql_type, type_name = TypeConverter.spark_to_sql_type_info(prop_spark_type)
                # Unity Catalog expects DataType JSON, not StructField JSON
                type_json_value = TypeConverter.spark_to_datatype_json(prop_spark_type)
                if debug:
                    print(
                        f"  [{position}] {prop_name}: type_text='{sql_type}', "
                        f"type_name={type_name}, type_json='{type_json_value}'"
                    )

            input_params.append(
                FunctionParameterInfo(
                    name=prop_name,
                    type_text=sql_type,
                    type_name=type_name,
                    type_json=type_json_value,
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                    parameter_default="NULL",  # Makes view property parameters optional
                )
            )
            position += 1

        if debug:
            print(f"[DEBUG] Total parameters: {len(input_params)}")

        return input_params

    def _get_view_by_id(self, view_id: str) -> dm.View | None:
        """Get view from code_generator's data model by external_id."""
        if not self.code_generator:
            raise ValueError("code_generator must be set")

        # Get data model from code_generator (similar to how pygen-main accesses data models)
        data_model = (
            self.code_generator._data_model[0]
            if isinstance(self.code_generator._data_model, list)
            else self.code_generator._data_model
        )

        # Find view by external_id
        for view in data_model.views:
            if view.external_id == view_id:
                return view  # type: ignore[no-any-return]

        return None

    def _get_property_type(self, prop: ViewProperty) -> tuple[object, bool, bool]:
        """Safely extract property type, handling relationship properties and array types.

        Args:
            prop: Property object from view.properties

        Returns:
            Tuple of (property_type, is_relationship, is_multi)
            - property_type: The actual type object, or None for relationship properties
            - is_relationship: True if this is a relationship property
            - is_multi: True if this is a multi-relationship or array type (list/array), False for single
        """
        # Check connection definitions first (matching pygen-main's pattern)
        if isinstance(prop, MultiReverseDirectRelation):
            return (None, True, True)  # is_relationship=True, is_multi=True (ARRAY<STRING>)
        elif isinstance(prop, SingleReverseDirectRelation):
            return (None, True, False)  # is_relationship=True, is_multi=False (STRING)

        # Check if property has a .type attribute (for MappedProperty)
        if not isinstance(prop, dm.MappedProperty):
            # Default to single relationship if we can't determine
            return (None, True, False)

        property_type = prop.type

        # Check if it's a DirectRelation (matching pygen-main's pattern)
        if isinstance(property_type, dm.DirectRelation):
            # Check is_list to determine if it's multi or single
            is_multi = property_type.is_list if hasattr(property_type, "is_list") else False
            return (property_type, True, is_multi)

        # Check if it's an array type (MappedProperty with is_list=True)
        # This handles properties like tags, aliases, files, activities, timeSeries
        is_multi = False
        if hasattr(property_type, "is_list"):
            is_multi = property_type.is_list

        return (property_type, False, is_multi)  # Not a relationship, but may be an array

    def _property_type_to_sql_type(self, property_type: object) -> tuple[str, ColumnTypeName, str]:
        """Convert CDF property type to SQL type and Spark type name.

        DEPRECATED: Use _property_type_to_spark_type() + _spark_type_to_sql_type_info() instead.
        This method is kept for backward compatibility but should not be used in new code.

        Uses TypeConverter for all type conversions, following cursor rules.

        Args:
            property_type: CDF property type (e.g., dm.Text, dm.Int32, etc.)

        Returns:
            Tuple of (sql_type_string, ColumnTypeName, spark_type_name)
            - sql_type_string: SQL type like "STRING", "INT", "DOUBLE"
            - ColumnTypeName: Databricks SDK enum
            - spark_type_name: Lowercase Spark type name for StructField JSON: "string", "double", etc.
        """
        # Convert CDF property type to PySpark DataType using TypeConverter
        spark_type = TypeConverter.cdf_to_spark(property_type, is_array=False)

        # Convert PySpark DataType to SQL type info using TypeConverter
        sql_type, column_type_name = TypeConverter.spark_to_sql_type_info(spark_type)

        # Get lowercase Spark type name from PySpark DataType
        spark_type_name = self._get_spark_type_name(spark_type)

        return (sql_type, column_type_name, spark_type_name)

    def _get_spark_type_name(self, spark_type: DataType) -> str:
        """Get lowercase Spark type name from PySpark DataType for StructField JSON.

        Args:
            spark_type: PySpark DataType

        Returns:
            Lowercase Spark type name (e.g., "string", "long", "double")
        """
        if isinstance(spark_type, StringType):
            return "string"
        elif isinstance(spark_type, LongType):
            return "long"
        elif isinstance(spark_type, DoubleType):
            return "double"
        elif isinstance(spark_type, BooleanType):
            return "boolean"
        elif isinstance(spark_type, DateType):
            return "date"
        elif isinstance(spark_type, TimestampType):
            return "timestamp"
        elif isinstance(spark_type, ArrayType):
            # For arrays, return "array" (element type is in elementType field)
            return "array"
        else:
            # Default fallback
            return "string"

    def _build_output_schema(self, view_id: str, debug: bool = False) -> StructType:
        """Build the output schema StructType (matching generated UDTF's outputSchema()).

        This creates the exact same StructType that the generated UDTF code uses,
        ensuring consistency between registration and generated code. Uses the same
        logic as UDTFField to determine types and nullability.

        Args:
            view_id: View external_id
            debug: If True, includes debug columns in the schema (matching template behavior)

        Returns:
            PySpark StructType matching the UDTF's outputSchema()
        """
        view = self._get_view_by_id(view_id)
        if not view:
            raise ValueError(f"View '{view_id}' not found in data model")

        fields = []
        for prop_name, prop in view.properties.items():
            # Use UDTFField.from_property to get the same logic as generated code
            # This ensures we skip the same properties (e.g., MultiReverseDirectRelation)
            udtf_field = UDTFField.from_property(prop_name, prop)
            if udtf_field is None:
                continue  # Skipped property (e.g., MultiReverseDirectRelation)

            # Build PySpark type directly from property (same logic as UDTFField._get_spark_type)
            # This is more reliable than parsing strings
            property_type, is_relationship, is_multi = self._get_property_type(prop)

            if is_relationship:
                # Relationships are always StringType (external_id references)
                field_spark_type = StringType()
            else:
                # Build Spark type from property type
                field_spark_type = TypeConverter.cdf_to_spark(property_type, is_array=is_multi)  # type: ignore[assignment]

            fields.append(StructField(udtf_field.name, field_spark_type, nullable=udtf_field.nullable))

        # Add space and external_id fields (matching template outputSchema)
        fields.append(StructField("space", StringType(), nullable=False))
        fields.append(StructField("external_id", StringType(), nullable=False))

        # Add timestamp fields (matching template outputSchema)
        from pyspark.sql.types import TimestampType

        fields.append(StructField("createdTime", TimestampType(), nullable=True))
        fields.append(StructField("lastUpdatedTime", TimestampType(), nullable=True))
        fields.append(StructField("deletedTime", TimestampType(), nullable=True))

        # Add debug columns when debug=True (matching template outputSchema)
        if debug:
            from pyspark.sql.types import LongType

            fields.extend(
                [
                    StructField("_debug_auth_status", StringType(), nullable=True),
                    StructField("_debug_api_status", StringType(), nullable=True),
                    StructField("_debug_api_items_count", LongType(), nullable=True),
                    StructField("_debug_execution_mode", StringType(), nullable=True),
                    StructField("_debug_rows_yielded", LongType(), nullable=True),
                    StructField("_debug_error", StringType(), nullable=True),
                    StructField("_debug_schema_info", StringType(), nullable=True),
                    StructField("_debug_column_count", LongType(), nullable=True),
                    StructField("_debug_column_names", StringType(), nullable=True),
                    # Array processing debug columns
                    StructField("_debug_tags_raw", StringType(), nullable=True),
                    StructField("_debug_tags_extracted", StringType(), nullable=True),
                    StructField("_debug_tags_normalized", StringType(), nullable=True),
                    StructField("_debug_tags_types", StringType(), nullable=True),
                    StructField("_debug_aliases_raw", StringType(), nullable=True),
                    StructField("_debug_aliases_extracted", StringType(), nullable=True),
                    StructField("_debug_aliases_normalized", StringType(), nullable=True),
                    StructField("_debug_aliases_types", StringType(), nullable=True),
                ]
            )

        return StructType(fields)

    def validate_schema_consistency(self, view_id: str, registered_function: FunctionInfo) -> tuple[bool, str | None]:
        """Validate that registered schema matches generated UDTF schema.

        Args:
            view_id: View external_id
            registered_function: FunctionInfo from Unity Catalog

        Returns:
            Tuple of (is_consistent, error_message)
        """
        # Build expected schema from view properties (same as generated code)
        expected_schema = self._build_output_schema(view_id)

        # Parse registered schema from full_data_type or return_params
        try:
            registered_schema = self._extract_schema_from_function(registered_function)
        except (AttributeError, KeyError, ValueError) as e:
            return False, f"Failed to extract schema from registered function: {e}"

        # Compare schemas
        is_match, error_msg = self._schemas_match(expected_schema, registered_schema)
        if not is_match:
            return False, f"Schema mismatch for {view_id}: {error_msg}"

        return True, None

    def _extract_schema_from_function(self, function_info: FunctionInfo) -> StructType:
        """Extract PySpark StructType from registered function's return_params.

        This allows us to programmatically compare registered schemas with
        expected schemas.

        Args:
            function_info: FunctionInfo from Unity Catalog

        Returns:
            PySpark StructType extracted from the registered function
        """
        fields = []

        # Use return_params if available (preferred)
        if function_info.return_params and function_info.return_params.parameters:
            for param in function_info.return_params.parameters:
                # Parse type_json back to PySpark DataType
                if param.type_json is None:
                    continue
                spark_type = self._parse_type_json_to_spark_type(param.type_json)
                # Get nullable from type_json or default to True
                nullable = True
                try:
                    import json

                    type_data = json.loads(param.type_json)
                    nullable = type_data.get("nullable", True)
                except (json.JSONDecodeError, KeyError, AttributeError):
                    pass
                fields.append(StructField(param.name, spark_type, nullable=nullable))
        # Fallback: parse from full_data_type DDL string
        elif function_info.full_data_type:
            registered_schema = self._parse_ddl_to_struct_type(function_info.full_data_type)
            return registered_schema

        return StructType(fields)

    def _parse_type_json_to_spark_type(self, type_json: str) -> DataType:
        """Parse Spark StructField JSON back to PySpark DataType.

        Example: '{"name":"col","type":"string","nullable":true,"metadata":{}}'
        → StringType()

        Args:
            type_json: JSON string in Spark StructField format

        Returns:
            PySpark DataType object
        """
        import json

        try:
            data = json.loads(type_json)
        except json.JSONDecodeError:
            # If parsing fails, default to StringType
            return StringType()

        type_name = data.get("type", "string")

        # Handle nested type objects (for arrays, structs, etc.)
        if isinstance(type_name, dict):
            # This is a complex type (array, struct, etc.)
            complex_type = type_name.get("type", "string")
            if complex_type == "array":
                # Handle array types
                element_type_data = type_name.get("elementType", {})
                if isinstance(element_type_data, dict):
                    # Recursively parse element type
                    element_type_json = json.dumps(element_type_data)
                    element_type = self._parse_type_json_to_spark_type(element_type_json)
                elif isinstance(element_type_data, str):
                    # Simple string element type like "string"
                    if element_type_data == "string":
                        element_type = StringType()
                    elif element_type_data == "long":
                        element_type = LongType()
                    elif element_type_data == "double":
                        element_type = DoubleType()
                    elif element_type_data == "boolean":
                        element_type = BooleanType()
                    else:
                        # Fallback: assume string
                        element_type = StringType()
                else:
                    # Fallback: assume string array
                    element_type = StringType()
                contains_null = type_name.get("containsNull", True)
                return ArrayType(element_type, containsNull=contains_null)
            else:
                # Unknown complex type, default to StringType
                return StringType()
        elif type_name == "string":
            return StringType()
        elif type_name == "long":
            return LongType()
        elif type_name == "double":
            return DoubleType()
        elif type_name == "boolean":
            return BooleanType()
        elif type_name == "date":
            return DateType()
        elif type_name == "timestamp":
            return TimestampType()
        else:
            # Default fallback
            return StringType()

    def _parse_ddl_to_struct_type(self, ddl_string: str) -> StructType:
        """Parse SQL DDL string back to PySpark StructType.

        Example: "TABLE(name STRING, age INT, tags ARRAY<STRING>)"
        → StructType([StructField("name", StringType()), ...])

        Args:
            ddl_string: SQL DDL string like "TABLE(col1 TYPE, col2 TYPE, ...)"

        Returns:
            PySpark StructType
        """
        import re

        # Remove "TABLE(" prefix and ")" suffix
        if not ddl_string.startswith("TABLE(") or not ddl_string.endswith(")"):
            raise ValueError(f"Invalid DDL format: {ddl_string}")

        columns_str = ddl_string[6:-1]  # Remove "TABLE(" and ")"

        # Parse column definitions: "name STRING, age INT, tags ARRAY<STRING>"
        fields = []
        # Simple regex to split by comma, but handle ARRAY<...> correctly
        # This is a simplified parser - for production, consider a more robust solution
        column_pattern = r"(\w+)\s+([^,]+?)(?=,\s*\w+\s+|$)"
        matches = re.finditer(column_pattern, columns_str)

        for match in matches:
            col_name = match.group(1)
            col_type_str = match.group(2).strip()

            # Convert SQL type string to PySpark DataType
            spark_type = self._sql_type_string_to_spark_type(col_type_str)
            fields.append(StructField(col_name, spark_type, nullable=True))

        return StructType(fields)

    def _sql_type_string_to_spark_type(self, sql_type: str) -> DataType:
        """Convert SQL type string to PySpark DataType.

        Args:
            sql_type: SQL type string like "STRING", "INT", "ARRAY<STRING>"

        Returns:
            PySpark DataType object
        """
        sql_type = sql_type.strip().upper()

        if sql_type == "STRING":
            return StringType()
        elif sql_type == "INT":
            return LongType()
        elif sql_type == "DOUBLE":
            return DoubleType()
        elif sql_type == "BOOLEAN":
            return BooleanType()
        elif sql_type == "DATE":
            return DateType()
        elif sql_type == "TIMESTAMP":
            return TimestampType()
        elif sql_type.startswith("ARRAY<"):
            # Parse ARRAY<element_type>
            element_type_str = sql_type[6:-1]  # Remove "ARRAY<" and ">"
            element_type = self._sql_type_string_to_spark_type(element_type_str)
            return ArrayType(element_type, containsNull=True)
        else:
            # Default fallback
            return StringType()

    def _schemas_match(self, schema1: StructType, schema2: StructType) -> tuple[bool, str | None]:
        """Compare two StructTypes for equality.

        Args:
            schema1: First StructType to compare
            schema2: Second StructType to compare

        Returns:
            Tuple of (is_match, error_message)
        """
        if len(schema1.fields) != len(schema2.fields):
            return False, f"Field count mismatch: {len(schema1.fields)} vs {len(schema2.fields)}"

        for i, (f1, f2) in enumerate(zip(schema1.fields, schema2.fields, strict=False)):
            if f1.name != f2.name:
                return False, f"Field {i} name mismatch: '{f1.name}' vs '{f2.name}'"

            if not self._types_match(f1.dataType, f2.dataType):
                return False, f"Field '{f1.name}' type mismatch: {f1.dataType} vs {f2.dataType}"

            if f1.nullable != f2.nullable:
                return False, f"Field '{f1.name}' nullable mismatch: {f1.nullable} vs {f2.nullable}"

        return True, None

    def _types_match(self, type1: DataType, type2: DataType) -> bool:
        """Compare two PySpark DataTypes for equality.

        Args:
            type1: First DataType to compare
            type2: Second DataType to compare

        Returns:
            True if types match, False otherwise
        """
        # Use PySpark's type equality
        return bool(type1 == type2)  # type: ignore[no-any-return]

    def _parse_return_type(self, view_id: str, udtf_file: Path | None = None, debug: bool = False) -> str:
        """Parse UDTF return type using PySpark StructType.

        Builds the same StructType as the generated UDTF's outputSchema() and converts
        it to SQL DDL, ensuring consistency between registration and generated code.

        Args:
            view_id: View external_id
            udtf_file: Optional UDTF file path (used when view not found in data model)
            debug: If True, includes debug columns in the schema

        Returns:
            SQL return type string (e.g., "TABLE(col1 STRING, col2 INT, col3 ARRAY<STRING>)")
        """
        # Build StructType (same as generated UDTF's outputSchema())
        view = self._get_view_by_id(view_id)
        if view is None:
            if udtf_file is None:
                raise ValueError(f"View '{view_id}' not found in data model")
            udtf_code = udtf_file.read_text()
            udtf_class = self._extract_udtf_class_from_ast(udtf_code, udtf_file)
            return self._parse_return_type_from_class(udtf_class)

        struct_type = self._build_output_schema(view_id, debug=debug)
        return str(TypeConverter.struct_type_to_ddl(struct_type))  # type: ignore[no-any-return]

    def _parse_return_params(self, view_id: str, debug: bool = False) -> list[FunctionParameterInfo]:
        """Parse structured return parameters using PySpark StructType.

        Unity Catalog requires return_params to be populated for TABLE_TYPE functions,
        providing structured metadata for each output column. Uses the same StructType
        as the generated UDTF's outputSchema() to ensure consistency.

        Args:
            view_id: View external_id
            debug: If True, includes debug columns and prints parameter details

        Returns:
            List of FunctionParameterInfo objects for the UDTF return columns
        """
        # Build StructType (same as generated UDTF's outputSchema())
        struct_type = self._build_output_schema(view_id, debug=debug)

        return_params: list[FunctionParameterInfo] = []

        if debug:
            print(f"[DEBUG] Parsing return parameters (output columns) for {view_id}:")
            print(f"[DEBUG] Using StructType with {len(struct_type.fields)} fields")

        for position, field in enumerate(struct_type.fields):
            # Convert PySpark DataType to SQL type info
            sql_type, type_name = TypeConverter.spark_to_sql_type_info(field.dataType)
            # Unity Catalog expects DataType JSON, not StructField JSON
            type_json_value = TypeConverter.spark_to_datatype_json(field.dataType)

            if debug:
                print(
                    f"  [{position}] {field.name}: type_text='{sql_type}', "
                    f"type_name={type_name}, nullable={field.nullable}"
                )

            return_params.append(
                FunctionParameterInfo(
                    name=field.name,
                    type_text=sql_type,
                    type_name=type_name,
                    type_json=type_json_value,
                    position=position,
                    # Return parameters use COLUMN type, not PARAM
                    # parameter_mode should be None for return columns
                    parameter_mode=None,
                    parameter_type=FunctionParameterType.COLUMN,
                )
            )

        return return_params

    def _extract_and_register_foreign_keys(
        self,
        view_id: str,
        warehouse_id: str | None = None,
        debug: bool = False,
    ) -> None:
        """Extract relationship properties and register foreign key constraints.

        For each DirectRelation or MultiReverseDirectRelation property in a view,
        registers an informational foreign key constraint documenting that the STRING
        column (external_id) references another view's external_id column.

        Relationship properties are represented as STRING type (external_id references)
        in the UDTF signature and return schema. This method adds informational
        constraints to document these relationships for data governance and discoverability.

        Args:
            view_id: View external_id to process
            warehouse_id: Optional SQL warehouse ID
            debug: If True, prints constraint details

        Example:
            If a view has a property "user_id" that is a DirectRelation to "Users" view,
            this will register:
            ALTER VIEW catalog.schema.view_name
            ADD CONSTRAINT fk_view_name_user_id
            FOREIGN KEY (user_id) REFERENCES catalog.schema.Users(external_id) NOT ENFORCED
        """
        if not self.udtf_registry:
            return

        view = self._get_view_by_id(view_id)
        if not view:
            return

        if debug:
            print(f"[DEBUG] Extracting foreign key relationships for {view_id}")

        # Process each property to find relationships
        for prop_name, prop in view.properties.items():
            property_type, is_relationship, is_multi = self._get_property_type(prop)

            if not is_relationship:
                continue

            # Note: For MultiReverseDirectRelation (arrays), we still create a foreign key constraint
            # The constraint documents that the array elements (external_ids) reference another view
            # However, SQL foreign key constraints don't directly support arrays, so we'll skip
            # FK constraints for multi-relationships for now, or document them differently
            if is_multi:
                if debug:
                    print(
                        f"[DEBUG] Skipping FK constraint for {view_id}.{prop_name} "
                        f"(MultiReverseDirectRelation - arrays not supported in FK constraints)"
                    )
                continue

            # Try to extract the referenced view external_id
            referenced_view_id = None

            # For DirectRelation, the source view is in property_type.source
            if property_type is not None and isinstance(property_type, dm.DirectRelation):
                if hasattr(property_type, "source"):
                    source = property_type.source
                    if hasattr(source, "external_id"):
                        referenced_view_id = source.external_id
                    elif isinstance(source, str):
                        # Sometimes source is just the external_id string
                        referenced_view_id = source

            # For MultiReverseDirectRelation/SingleReverseDirectRelation, check the property itself
            # These don't have .type, but may have source/destination attributes
            elif hasattr(prop, "source"):
                source = prop.source
                if hasattr(source, "external_id"):
                    referenced_view_id = source.external_id
            elif hasattr(prop, "destination"):
                destination = prop.destination
                if hasattr(destination, "external_id"):
                    referenced_view_id = destination.external_id

            if referenced_view_id:
                if debug:
                    print(f"[DEBUG] Found relationship: {view_id}.{prop_name} -> {referenced_view_id}.external_id")

                # Register foreign key constraint
                try:
                    self.udtf_registry.register_foreign_key_constraint(
                        catalog=self.catalog,
                        schema=self.schema,
                        view_name=view_id,
                        column_name=prop_name,
                        referenced_catalog=self.catalog,
                        referenced_schema=self.schema,
                        referenced_view=referenced_view_id,
                        referenced_column="external_id",
                        warehouse_id=warehouse_id,
                        debug=debug,
                    )
                except (RuntimeError, ValueError) as e:
                    if debug:
                        print(f"[WARNING] Failed to register FK for {view_id}.{prop_name}: {e}")
            elif debug:
                print(
                    f"[DEBUG] Relationship property {view_id}.{prop_name} found but could not determine referenced view"
                )
