"""generate_udtf_notebook and UDTFGenerator - High-level APIs for UDTF generation and registration."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Semaphore
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    pass

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.views import (
    MultiReverseDirectRelation,
    SingleReverseDirectRelation,
    ViewProperty,
)

from cognite.databricks.models import (
    RegisteredUDTFResult,
    UDTFRegistrationResult,
)
from cognite.databricks.secret_manager import SecretManagerHelper
from cognite.databricks.type_converter import TypeConverter
from cognite.databricks.udtf_registry import UDTFRegistry
from cognite.databricks.utils import to_udtf_function_name
from cognite.pygen_spark import SparkUDTFGenerator
from cognite.pygen_spark.fields import UDTFField
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
    from pyspark.sql.types import (
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
DataModel = Union[DataModelIdentifier, dm.DataModel[dm.View]]


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
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udtf
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

    # Execute the code in a temporary namespace to get the UDTF class
    namespace: dict[str, object] = {}
    exec(udtf_code, namespace)

    # Find the UDTF class (it should be the only class defined in the file with eval and analyze methods)
    udtf_classes = [
        obj
        for name, obj in namespace.items()
        if isinstance(obj, type) and hasattr(obj, "eval") and hasattr(obj, "analyze")
    ]

    if not udtf_classes:
        raise ValueError(
            f"No UDTF class found in {udtf_file_path}. " "Expected a class with 'eval' and 'analyze' methods."
        )

    if len(udtf_classes) > 1:
        raise ValueError(f"Multiple UDTF classes found in {udtf_file_path}. Expected exactly one.")

    udtf_class = udtf_classes[0]

    # Determine function name
    if function_name is None:
        # Extract from class name: SmallboatUDTF -> small_boat_udtf
        class_name = udtf_class.__name__
        # Remove "UDTF" suffix if present
        base_name = class_name[:-4] if class_name.endswith("UDTF") else class_name
        # Use pygen-main's to_snake for consistent conversion
        function_name = to_udtf_function_name(base_name)

    # Verify analyze method exists
    if not hasattr(udtf_class, "analyze"):
        raise RuntimeError(f"analyze method not found in {udtf_class.__name__}! " "Required for PySpark Connect.")

    # Wrap the class with udtf() - DO NOT pass returnType when analyze method exists
    udtf_wrapped = udtf()(udtf_class)  # type: ignore[arg-type]

    # Register the wrapped version
    spark_session.udtf.register(function_name, udtf_wrapped)  # type: ignore[attr-defined]

    print(f"✓ UDTF registered successfully: {function_name}")
    print(f"✓ Class: {udtf_class.__name__}")
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
    The view calls the UDTF with SECRET parameters for credentials and allows
    optional parameters for time series-specific inputs (space, external_id, etc.).

    Args:
        udtf_name: Name of the UDTF function (e.g., "time_series_datapoints_udtf")
        view_name: Optional view name. If None, uses default from TimeSeriesUDTFRegistry
        secret_scope: Databricks Secret Manager scope name
        catalog: Optional catalog name. If None, uses placeholder "{{ catalog }}"
        schema: Optional schema name. If None, uses placeholder "{{ schema }}"
        udtf_params: Optional list of parameter names. If None, uses default from TimeSeriesUDTFRegistry

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
        #     space => NULL,
        #     external_id => NULL,
        #     start => NULL,
        #     end => NULL,
        #     aggregates => NULL,
        #     granularity => NULL,
        #     client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
        #     client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
        #     tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
        #     cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
        #     project => SECRET('cdf_sailboat_sailboat', 'project')
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

    # Build SQL
    sql_lines = [
        f"CREATE OR REPLACE VIEW {catalog_schema_prefix}{view_name} AS",
        f"SELECT * FROM {catalog_schema_prefix}{udtf_name}(",
    ]

    # Add UDTF-specific parameters first (with default NULL)
    if udtf_params is not None:
        for param in udtf_params:
            sql_lines.append(f"    {param} => NULL,")

    # Add SECRET parameters for credentials
    sql_lines.append(f"    client_id     => SECRET('{secret_scope}', 'client_id'),")
    sql_lines.append(f"    client_secret => SECRET('{secret_scope}', 'client_secret'),")
    sql_lines.append(f"    tenant_id     => SECRET('{secret_scope}', 'tenant_id'),")
    sql_lines.append(f"    cdf_cluster   => SECRET('{secret_scope}', 'cdf_cluster'),")
    sql_lines.append(f"    project       => SECRET('{secret_scope}', 'project')")
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
        except Exception:
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
            except Exception:
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
    except Exception:
        pass

    # Try to get actual values from generator
    try:
        actual_output_dir = str(generator.code_generator.output_dir)
    except Exception:
        actual_output_dir = "/Workspace/Users/<your_email>/udtf"

    # Try to get catalog and schema (not used, but kept for potential future use)
    try:
        _ = generator.catalog
        _ = generator.schema
    except Exception:
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


def generate_udtf_notebook(
    data_model: DataModel,
    client: CogniteClient,
    workspace_client: WorkspaceClient | None = None,
    catalog: str = "main",
    schema: str | None = None,
    output_dir: Path | str | None = None,
    warehouse_id: str | None = None,
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
    except Exception as e:
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

    def _register_single_udtf_and_view(
        self,
        view_id: str,
        udtf_file: Path,
        view_sql: str | None,
        secret_scope: str,
        dependencies: list[str] | None,
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
            dependencies: Python package dependencies
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
                    view_id, udtf_file, view_sql, secret_scope, dependencies, warehouse_id, if_exists, debug
                )
            finally:
                rate_limiter.release()
        else:
            return self._register_single_udtf_and_view_impl(
                view_id, udtf_file, view_sql, secret_scope, dependencies, warehouse_id, if_exists, debug
            )

    def _register_single_udtf_and_view_impl(
        self,
        view_id: str,
        udtf_file: Path,
        view_sql: str | None,
        secret_scope: str,
        dependencies: list[str] | None,
        warehouse_id: str | None,
        if_exists: str,
        debug: bool,
    ) -> RegisteredUDTFResult:
        """Implementation of single UDTF registration (without rate limiter)."""
        udtf_code = udtf_file.read_text()

        # Check if this is a time series UDTF (no view, parse from class)
        from cognite.databricks.models import time_series_udtf_registry

        is_time_series_udtf = time_series_udtf_registry.get_config(view_id) is not None

        if is_time_series_udtf:
            # For time series UDTFs, parse directly from the class
            # Execute the code to get the class
            namespace: dict[str, object] = {}
            exec(udtf_code, namespace)

            # Find the UDTF class
            udtf_classes = [
                obj
                for name, obj in namespace.items()
                if isinstance(obj, type) and hasattr(obj, "eval") and hasattr(obj, "analyze")
            ]
            if not udtf_classes:
                raise ValueError(f"No UDTF class found in {udtf_file}")
            udtf_class = udtf_classes[0]

            # Parse from class
            input_params = self._parse_udtf_params_from_class(udtf_class, debug=debug)
            return_type = self._parse_return_type_from_class(udtf_class)
            return_params = self._parse_return_params_from_class(udtf_class, debug=debug)

            comment = f"Auto-generated UDTF for {udtf_class.__name__}"
        else:
            # For Data Model view UDTFs, parse from view (existing logic)
            input_params = self._parse_udtf_params(view_id, debug=debug)
            return_type = self._parse_return_type(view_id)
            return_params = self._parse_return_params(view_id, debug=debug)
            comment = f"Auto-generated UDTF for {view_id} View"

        if debug:
            print(f"\n[DEBUG] Registering {view_id}: return_type={return_type}")

        function_info = self.udtf_registry.register_udtf(
            catalog=self.catalog,
            schema=self.schema,
            function_name=to_udtf_function_name(view_id),
            udtf_code=udtf_code,
            input_params=input_params,
            return_type=return_type,
            return_params=return_params,
            dependencies=dependencies,
            comment=comment,
            if_exists=if_exists,
            debug=debug,
        )

        # Track view registration status
        view_registered = False
        view_name = None

        # Only create result if function_info is not None (skipped functions return None)
        if function_info is not None:
            # Validate schema consistency (optional, but recommended)
            # Skip for time series UDTFs (they don't have views to validate against)
            if debug and not is_time_series_udtf:
                is_consistent, error_msg = self.validate_schema_consistency(view_id, function_info)
                if is_consistent:
                    print(f"[DEBUG] Schema validation passed for {view_id}")
                else:
                    print(f"[WARNING] Schema validation failed for {view_id}: {error_msg}")

            # Register View (for both Data Model views and time series UDTF views)
            if view_sql:
                # View SQL should already have catalog and schema from generate_views
                # But if placeholders are still present, replace them as fallback
                if "{{ catalog }}" in view_sql or "{{ schema }}" in view_sql:
                    view_sql = view_sql.replace("{{ catalog }}", self.catalog).replace("{{ schema }}", self.schema)

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

                    self.udtf_registry.register_view(
                        catalog=self.catalog,
                        schema=self.schema,
                        view_name=actual_view_name,
                        view_sql=view_sql,
                        comment=view_comment,
                        warehouse_id=warehouse_id,
                        debug=debug,
                    )
                    view_registered = True
                    view_name = f"{self.catalog}.{self.schema}.{actual_view_name}"
                except Exception as e:
                    if debug:
                        print(f"[DEBUG] Failed to register view {view_id}: {e}")

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
                    except Exception as e:
                        if debug:
                            print(f"[DEBUG] Failed to register foreign keys for {view_id}: {e}")

        if function_info is None:
            raise ValueError(f"Failed to register UDTF for view {view_id}")

        return RegisteredUDTFResult(
            view_id=view_id,
            function_info=function_info,
            view_name=view_name,
            udtf_file_path=udtf_file,
            view_registered=view_registered,
        )

    def register_udtfs_and_views(
        self,
        data_model: DataModel | None = None,
        secret_scope: str | None = None,
        dependencies: list[str] | None = None,
        warehouse_id: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
        max_workers: int = 5,
        max_parallel_requests: int | None = None,
    ) -> UDTFRegistrationResult:
        """Generate and register UDTFs and Views for a Data Model.

        Uses parallel execution with ThreadPoolExecutor (similar to cognite-sdk-scala's approach)
        for efficient registration of multiple UDTFs. Optional rate limiting via Semaphore.

        Args:
            data_model: Optional DataModel identifier (DataModelId or DataModel object).
                       If None, uses the data model from code_generator initialization.
            secret_scope: Secret Manager scope name. If None, auto-generates from data model:
                         `cdf_{space}_{external_id}` (e.g., "cdf_sp_pygen_power_windturbine")
            dependencies: Optional Python package dependencies (DBR 18.1+).
                         If None, uses fallback mode for pre-DBR 18.1 (requires pre-installed packages).
                         Example: ["cognite-sdk>=7.90.1", "cognite-pygen>=1.2.29"]
                         Note: PyPI package name uses hyphens
            warehouse_id: Optional SQL warehouse ID for view registration.
                         If None, uses the warehouse_id from __init__ or tries to find a default warehouse.
            if_exists: What to do if UDTF already exists:
                      - "skip": Skip registration and return existing function (default)
                      - "replace": Delete and recreate the function
                      - "error": Raise ResourceAlreadyExists error
            debug: If True, prints detailed information about parameters and SQL being sent.
            max_workers: Maximum number of parallel worker threads for registration (default: 5).
                        Similar to cognite-sdk-scala's ThreadPoolExecutor approach.
            max_parallel_requests: Optional rate limiting via Semaphore (similar to RateLimitingBackend).
                                  If None, no rate limiting is applied. If set, limits concurrent API calls.

        Returns:
            UDTFRegistrationResult with structured information about registered UDTFs and views.
            Access individual results via result['view_id'] or result.get('view_id').
        """
        if not self.workspace_client:
            raise ValueError("WorkspaceClient must be set before registration")

        # Ensure schema exists before registering functions
        self._ensure_schema_exists()

        # Auto-generate scope name from data model if not provided
        if secret_scope is None:
            if data_model:
                if isinstance(data_model, dm.DataModel):
                    model_id = data_model.as_id()
                else:
                    # Type narrowing
                    model_id = data_model  # type: ignore[assignment]
                secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
            else:
                raise ValueError("secret_scope must be provided if data_model is None")

        # If data_model not provided, use the one from code_generator initialization
        if data_model:
            # Generate UDTF code (will use the provided data_model)
            udtf_result = self.code_generator.generate_udtfs(data_model)
            udtf_files = udtf_result.generated_files

            # Also generate time series UDTF files using the same generator instance
            try:
                ts_result = self.code_generator.generate_time_series_udtfs()
                udtf_files.update(ts_result.generated_files)
            except Exception as e:
                print(f"[WARNING] Failed to generate time series UDTF files: {e}")

            # Generate View SQL
            view_sql_result = self.code_generator.generate_views(
                data_model=data_model,
                secret_scope=secret_scope,
                catalog=self.catalog,
                schema=self.schema,
            )
            view_sqls = view_sql_result.view_sqls

            # Also generate time series UDTF view SQL using Pydantic registry
            try:
                from cognite.databricks.models import time_series_udtf_registry

                for udtf_name in time_series_udtf_registry.get_all_udtf_names():
                    if udtf_name in udtf_files:  # Only if UDTF file was generated
                        config = time_series_udtf_registry.get_config(udtf_name)
                        if config:
                            view_sql = generate_time_series_udtf_view_sql(
                                udtf_name=udtf_name,
                                view_name=config.view_name,
                                secret_scope=secret_scope,
                                catalog=self.catalog,
                                schema=self.schema,
                                udtf_params=config.parameters,
                            )
                            view_sqls[udtf_name] = view_sql  # Use udtf_name as key for consistency
            except Exception as e:
                print(f"[WARNING] Failed to generate time series UDTF view SQL: {e}")
        else:
            # Use the data model from code_generator initialization
            # UDTF code was already generated in __init__, just get the files
            udtf_files = self._find_generated_udtf_files()

            # Also generate time series UDTF files using the same generator instance
            try:
                ts_result = self.code_generator.generate_time_series_udtfs()
                udtf_files.update(ts_result.generated_files)
            except Exception as e:
                print(f"[WARNING] Failed to generate time series UDTF files: {e}")

            # Generate View SQL using the data model from code_generator
            view_sql_result = self.code_generator.generate_views(
                data_model=None,  # Use the one from __init__
                secret_scope=secret_scope,
                catalog=self.catalog,
                schema=self.schema,
            )
            view_sqls = view_sql_result.view_sqls

            # Also generate time series UDTF view SQL using Pydantic registry
            try:
                from cognite.databricks.models import time_series_udtf_registry

                for udtf_name in time_series_udtf_registry.get_all_udtf_names():
                    if udtf_name in udtf_files:  # Only if UDTF file was generated
                        config = time_series_udtf_registry.get_config(udtf_name)
                        if config:
                            view_sql = generate_time_series_udtf_view_sql(
                                udtf_name=udtf_name,
                                view_name=config.view_name,
                                secret_scope=secret_scope,
                                catalog=self.catalog,
                                schema=self.schema,
                                udtf_params=config.parameters,
                            )
                            view_sqls[udtf_name] = view_sql  # Use udtf_name as key for consistency
            except Exception as e:
                print(f"[WARNING] Failed to generate time series UDTF view SQL: {e}")

        registered_udtfs: list[RegisteredUDTFResult] = []

        if debug:
            print(f"\n{'='*60}")
            print("[DEBUG] Starting UDTF and View registration")
            print(f"[DEBUG] Catalog: {self.catalog}")
            print(f"[DEBUG] Schema: {self.schema}")
            print(f"[DEBUG] Secret scope: {secret_scope}")
            print(f"[DEBUG] if_exists: {if_exists}")
            print(f"[DEBUG] Max workers: {max_workers}")
            print(f"[DEBUG] Max parallel requests: {max_parallel_requests or 'unlimited'}")
            print(f"[DEBUG] UDTFs to register: {list(udtf_files.keys())}")
            print(f"{'='*60}\n")

        # Create rate limiter if specified (similar to cognite-sdk-scala's RateLimitingBackend)
        rate_limiter = Semaphore(max_parallel_requests) if max_parallel_requests else None

        # Register UDTFs in parallel using ThreadPoolExecutor (similar to cognite-sdk-scala approach)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[Future[RegisteredUDTFResult], str] = {
                executor.submit(
                    self._register_single_udtf_and_view,
                    view_id,
                    udtf_file,
                    view_sqls.get(view_id),
                    secret_scope,
                    dependencies,
                    warehouse_id or self.warehouse_id,
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
                        status = "✓" if result.function_info and result.view_registered else "⚠"
                        print(f"[DEBUG] {status} Completed registration for {view_id}")
                except Exception as e:
                    # Handle errors per view (don't fail entire registration)
                    error_result = RegisteredUDTFResult(
                        view_id=view_id,
                        function_info=None,  # type: ignore[arg-type]
                        view_name=None,
                        udtf_file_path=udtf_files.get(view_id),
                        view_registered=False,
                    )
                    registered_udtfs.append(error_result)
                    print(f"[ERROR] Failed to register {view_id}: {e}")
                    if debug:
                        import traceback

                        traceback.print_exc()

        return UDTFRegistrationResult(
            registered_udtfs=registered_udtfs,
            catalog=self.catalog,
            schema_name=self.schema,
            total_count=len(registered_udtfs),
        )

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
        - DBR < 18.1 environments where dependencies can be installed via %pip

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
            from pyspark.sql import SparkSession
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
        if data_model:
            # Generate UDTF code (will use the provided data_model)
            udtf_result = self.code_generator.generate_udtfs(data_model)
            udtf_files = udtf_result.generated_files
        else:
            # Use the data model from code_generator initialization
            # UDTF code was already generated in __init__, just get the files
            udtf_files = self._find_generated_udtf_files()

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
            except Exception as e:
                print(f"[ERROR] Failed to register {view_id} as {function_name}: {e}")
                # Continue with other UDTFs even if one fails
                continue

        print(f"\n✓ Registered {len(registered_functions)} UDTF(s) for session-scoped use:")
        for view_id, func_name in registered_functions.items():
            print(f"  - {view_id} -> {func_name}")

        return registered_functions

    def _find_generated_udtf_files(self) -> dict[str, Path]:
        """Find generated UDTF files in output directory.

        Returns:
            Dict mapping view external_id to Path of UDTF file
        """
        if not self.code_generator:
            raise ValueError("code_generator must be set to find generated files")

        # Files are stored in: output_dir / top_level_package / "{view_external_id}_udtf.py"
        udtf_dir = self.code_generator.output_dir / self.code_generator.top_level_package

        if not udtf_dir.exists():
            raise FileNotFoundError(
                f"UDTF directory not found: {udtf_dir}. " "Make sure generate_udtfs() was called first."
            )

        # Find all *_udtf.py files
        udtf_files: dict[str, Path] = {}
        for file_path in udtf_dir.glob("*_udtf.py"):
            # Extract view external_id from filename: "{view_external_id}_udtf.py"
            view_id = file_path.stem.replace("_udtf", "")
            udtf_files[view_id] = file_path

        if not udtf_files:
            raise FileNotFoundError(
                f"No UDTF files found in {udtf_dir}. " "Make sure generate_udtfs() was called first."
            )

        return udtf_files

    def _ensure_catalog_exists(self) -> None:
        """Create catalog if it doesn't exist.

        Checks if the catalog exists, and creates it if it doesn't.
        """
        if not self.workspace_client:
            return

        try:
            # Try to get the catalog - if it exists, we're done
            self.workspace_client.catalogs.get(self.catalog)
        except Exception:
            # Catalog doesn't exist, create it
            try:
                self.workspace_client.catalogs.create(
                    name=self.catalog,
                    comment="Catalog for CDF Data Model UDTFs and Views",
                )
            except Exception as e:
                # If creation fails, it might be a permission issue or catalog already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.catalogs.get(self.catalog)
                except Exception:
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

        try:
            # Try to get the schema - if it exists, we're done
            full_name = f"{self.catalog}.{self.schema}"
            self.workspace_client.schemas.get(full_name)
        except Exception:
            # Schema doesn't exist, create it
            try:
                self.workspace_client.schemas.create(
                    name=self.schema,
                    catalog_name=self.catalog,
                    comment="Schema for CDF Data Model UDTFs and Views",
                )
            except Exception as e:
                # If creation fails, it might be a permission issue or schema already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.schemas.get(full_name)
                except Exception:
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

    def _parse_udtf_params_from_class(
        self,
        udtf_class: type,
        debug: bool = False,
    ) -> list[FunctionParameterInfo]:
        """Parse UDTF function parameters directly from a UDTF class.

        This is used for time series UDTFs that don't have a corresponding view.
        Uses inspect to read the analyze() method signature.

        Args:
            udtf_class: UDTF class (must have analyze() method)
            debug: If True, prints parameter details

        Returns:
            List of FunctionParameterInfo objects matching the UDTF function signature
        """
        import inspect

        if not hasattr(udtf_class, "analyze"):
            raise ValueError(f"UDTF class {udtf_class.__name__} must have an analyze() method")

        # Get analyze method signature
        analyze_sig = inspect.signature(udtf_class.analyze)

        input_params: list[FunctionParameterInfo] = []
        position = 0

        if debug:
            print(f"\n[DEBUG] === Parsing parameters from {udtf_class.__name__} ===")

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
            type_json_value = TypeConverter.spark_to_type_json(spark_type, param_name, nullable=True)
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

        # Add other parameters from analyze() method (excluding self and secret params)
        if debug:
            print("[DEBUG] UDTF-specific parameters:")

        secret_param_names = {p[0] for p in secret_params}
        for param_name, param in analyze_sig.parameters.items():
            if param_name in secret_param_names:
                continue  # Already added

            # Infer type from annotation or default value
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else None
            default_value = param.default if param.default != inspect.Parameter.empty else None

            # Map Python types to PySpark types using TypeConverter
            if param_type is None or param_type is type(None):
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
            type_json_value = TypeConverter.spark_to_type_json(
                param_spark_type, param_name, nullable=(default_value is None)
            )
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
                    parameter_default="NULL" if default_value is None else None,
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
        return TypeConverter.struct_type_to_ddl(struct_type)

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
            type_json_value = TypeConverter.spark_to_type_json(field.dataType, field.name, nullable=field.nullable)

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
                    # Note: return parameters don't usually have mode/type set the same as input
                )
            )

        return return_params

    def _parse_udtf_params(self, view_id: str, debug: bool = False) -> list[FunctionParameterInfo]:
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
            raise ValueError(f"View '{view_id}' not found in data model")

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
            # Use PySpark StructField to generate correct type_json format
            spark_type = StringType()  # All secret params are strings
            type_json_value = TypeConverter.spark_to_type_json(spark_type, param_name, nullable=True)
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
                if is_multi:
                    # TODO: MultiReverseDirectRelation (ARRAY<STRING>) support is temporarily disabled
                    # The Databricks API rejects all array type_json formats we've tried.
                    # Skipping these properties for now until we find the correct format.
                    if debug:
                        print(
                            f"  [SKIP] {prop_name}: MultiReverseDirectRelation - "
                            f"skipping (ARRAY type_json format not supported by API yet)"
                        )
                    continue
                else:
                    # DirectRelation or SingleReverseDirectRelation: STRING (single external_id reference)
                    sql_type = "STRING"
                    type_name = ColumnTypeName.STRING
                    spark_type = StringType()
                    type_json_value = TypeConverter.spark_to_type_json(spark_type, prop_name, nullable=True)
                    if debug:
                        print(
                            f"  [{position}] {prop_name}: type_text='STRING', "
                            f"type_name=ColumnTypeName.STRING (single relationship - external_id reference)"
                        )
            else:
                # Use PySpark as source of truth - build Spark type first, then derive SQL type
                prop_spark_type = TypeConverter.cdf_to_spark(property_type, is_array=is_multi)  # type: ignore[assignment]
                sql_type, type_name = TypeConverter.spark_to_sql_type_info(prop_spark_type)
                type_json_value = TypeConverter.spark_to_type_json(prop_spark_type, prop_name, nullable=True)
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
        if not hasattr(prop, "type"):
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

        Args:
            property_type: CDF property type (e.g., dm.Text, dm.Int32, etc.)

        Returns:
            Tuple of (sql_type_string, ColumnTypeName, spark_type_name)
            - sql_type_string: SQL type like "STRING", "INT", "DOUBLE"
            - ColumnTypeName: Databricks SDK enum
            - spark_type_name: Lowercase Spark type name for StructField JSON: "string", "double", etc.
        """
        # dm is already imported at module level

        # Map CDF property types to SQL types and Spark type names (lowercase)
        # These type names are used in Spark StructField JSON format
        if isinstance(property_type, dm.Int32 | dm.Int64):
            return ("INT", ColumnTypeName.INT, "long")
        elif isinstance(property_type, dm.Boolean):
            return ("BOOLEAN", ColumnTypeName.BOOLEAN, "boolean")
        elif isinstance(property_type, dm.Float32 | dm.Float64):
            return ("DOUBLE", ColumnTypeName.DOUBLE, "double")
        elif isinstance(property_type, dm.Date):
            return ("DATE", ColumnTypeName.DATE, "date")
        elif isinstance(property_type, dm.Timestamp):
            return ("TIMESTAMP", ColumnTypeName.TIMESTAMP, "timestamp")
        elif isinstance(property_type, dm.Text):
            return ("STRING", ColumnTypeName.STRING, "string")
        elif isinstance(property_type, dm.DirectRelation):
            # Direct relations are typically represented as strings (external_id references)
            return ("STRING", ColumnTypeName.STRING, "string")
        else:
            # Default to STRING for unknown types
            return ("STRING", ColumnTypeName.STRING, "string")

    def _build_output_schema(self, view_id: str) -> StructType:
        """Build the output schema StructType (matching generated UDTF's outputSchema()).

        This creates the exact same StructType that the generated UDTF code uses,
        ensuring consistency between registration and generated code. Uses the same
        logic as UDTFField to determine types and nullability.

        Args:
            view_id: View external_id

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
        except Exception as e:
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
                except Exception:
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

        if type_name == "string":
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
        elif type_name == "array":
            # Handle array types
            element_type_data = data.get("elementType", {})
            if isinstance(element_type_data, dict):
                # Recursively parse element type
                element_type_json = json.dumps(element_type_data)
                element_type = self._parse_type_json_to_spark_type(element_type_json)
            else:
                # Fallback: assume string array
                element_type = StringType()
            contains_null = data.get("containsNull", True)
            return ArrayType(element_type, containsNull=contains_null)
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
        return type1 == type2

    def _parse_return_type(self, view_id: str) -> str:
        """Parse UDTF return type using PySpark StructType.

        Builds the same StructType as the generated UDTF's outputSchema() and converts
        it to SQL DDL, ensuring consistency between registration and generated code.

        Args:
            view_id: View external_id

        Returns:
            SQL return type string (e.g., "TABLE(col1 STRING, col2 INT, col3 ARRAY<STRING>)")
        """
        # Build StructType (same as generated UDTF's outputSchema())
        struct_type = self._build_output_schema(view_id)
        # Convert to SQL DDL
        return TypeConverter.struct_type_to_ddl(struct_type)

    def _parse_return_params(self, view_id: str, debug: bool = False) -> list[FunctionParameterInfo]:
        """Parse structured return parameters using PySpark StructType.

        Unity Catalog requires return_params to be populated for TABLE_TYPE functions,
        providing structured metadata for each output column. Uses the same StructType
        as the generated UDTF's outputSchema() to ensure consistency.

        Args:
            view_id: View external_id
            debug: If True, prints parameter details

        Returns:
            List of FunctionParameterInfo objects for the UDTF return columns
        """
        # Build StructType (same as generated UDTF's outputSchema())
        struct_type = self._build_output_schema(view_id)

        return_params: list[FunctionParameterInfo] = []

        if debug:
            print(f"[DEBUG] Parsing return parameters (output columns) for {view_id}:")
            print(f"[DEBUG] Using StructType with {len(struct_type.fields)} fields")

        for position, field in enumerate(struct_type.fields):
            # Convert PySpark DataType to SQL type info
            sql_type, type_name = TypeConverter.spark_to_sql_type_info(field.dataType)
            # Generate type_json using PySpark StructField
            type_json_value = TypeConverter.spark_to_type_json(field.dataType, field.name, nullable=field.nullable)

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
                    # Note: return parameters don't usually have mode/type set the same as input
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
                except Exception as e:
                    if debug:
                        print(f"[WARNING] Failed to register FK for {view_id}.{prop_name}: {e}")
            elif debug:
                print(
                    f"[DEBUG] Relationship property {view_id}.{prop_name} found "
                    f"but could not determine referenced view"
                )
