# cognite-databricks

A helper SDK for Databricks that provides Unity Catalog SQL UDTF registration utilities, Secret Manager integration, and Databricks-specific tooling for scalar UDTFs.

**Note**: This is the initial release (0.1.0) of cognite-databricks.

## Overview

`cognite-databricks` is a **Databricks-specific helper SDK** that extends `pygen-spark` with Unity Catalog SQL registration, Secret Manager integration, and Databricks-specific utilities. It focuses on serverless-compatible scalar UDTF execution for SQL Warehouses.

**Package Purpose:**
- **Databricks-Specific Features**: Unity Catalog SQL registration, Secret Manager integration, and Databricks-specific utilities
- **Uses pygen-spark for Code Generation**: All UDTF code generation (both Data Model and Time Series UDTFs) is done by `pygen-spark` using template-based generation
- **Generic Components**: Generic utilities (`TypeConverter`, `CDFConnectionConfig`, `to_udtf_function_name`) are provided by `pygen-spark` and re-exported from `cognite.databricks` for backward compatibility
- **Notebook-Friendly API**: Aligned with `cognite.pygen`'s notebook workflow

It provides high-level APIs for:

- **UDTF Registration**: Register persistent UDTFs in Unity Catalog via SQL
- **Secret Manager Integration**: Manage OAuth2 credentials securely
- **SQL Usage**: Use UDTFs directly in SQL after registration
- **Notebook-Friendly API**: Aligned with `cognite.pygen`'s notebook workflow

## Features

- **Unity Catalog SQL Registration**: Serverless-compatible UDTFs registered via `CREATE FUNCTION` statements
- **One-Line Registration**: Generate and register UDTFs in a single call
- **Secret Manager Integration**: Automatic credential management from TOML files
- **Scalar-Only Execution**: Compatible with SQL Warehouses and serverless execution
- **Type Safety**: Full type hints and IDE support
- **Generic Components**: Uses template-generated UDTFs and generic utilities (`TypeConverter`, `CDFConnectionConfig`, `to_udtf_function_name`) from `cognite-pygen-spark` for generic Spark compatibility. These components are re-exported from `cognite.databricks` for backward compatibility, but the source is `cognite.pygen_spark`.

## Installation

```bash
pip install cognite-databricks
```

## Quick Start

### Notebook-Style (Recommended)

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml

# Load client from TOML file (same pattern as pygen)
client = load_cognite_client_from_toml("config.toml")

# Generate UDTFs for a Data Model
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
)

# Register UDTFs in Unity Catalog (SQL registration)
udtf_result = generator.register_udtfs(
    secret_scope="cdf_sp_pygen_power_windturbine",
    if_exists="replace",
)
print(f"Registered {udtf_result.total_count} UDTF(s)")
```

### Low-Level API

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import UDTFGenerator, SecretManagerHelper
from cognite.pygen import load_cognite_client_from_toml

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Create generator
generator = UDTFGenerator(
    cognite_client=client,
    catalog="main",
    schema="cdf_models",
)

# Set up Secret Manager (one-time setup per data model)
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"

generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project="my-project",  # from config.toml
    cdf_cluster="api.cognitedata.com",  # from config.toml
    client_id="...",  # from config.toml
    client_secret="...",  # from config.toml
    tenant_id="...",  # from config.toml
)

# Register UDTFs for catalog-based use (scalar-only)
registered = generator.register_session_scoped_udtfs()
```

## Unity Catalog UDTF Registration

Session-scoped registration is the primary mode for scalar-only UDTFs. Register functions in the current Spark session before running SQL queries.

### Register All UDTFs (Recommended)

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client and generate UDTFs
client = load_cognite_client_from_toml("config.toml")
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    output_dir="/Workspace/Users/user@example.com/udtf",
)

# Install dependencies (run in separate cell first)
# %pip install cognite-sdk
# (Restart kernel after installation)

# Register all UDTFs for catalog-based use (includes time series UDTFs automatically)
registered = generator.register_session_scoped_udtfs()
# Returns: {"SmallBoat": "small_boat_udtf", "LargeBoat": "large_boat_udtf", 
#           "time_series_datapoints": "time_series_datapoints_udtf", ...}

# Use in SQL (always use SECRET() for credentials)
# SELECT * FROM small_boat_udtf(
#     client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
#     client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
#     tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
#     cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
#     project => SECRET('cdf_sailboat_sailboat', 'project'),
#     name => 'MyBoat',
#     description => NULL
# ) LIMIT 10;
```

### Register Single UDTF

```python
from cognite.databricks import generate_udtf_notebook, register_udtf_from_file

# Generate UDTFs
generator = generate_udtf_notebook(data_model_id, client, ...)

# Register a single UDTF from generated file
register_udtf_from_file(
    "/Workspace/Users/user@example.com/udtf/sailboat_sailboat_v1/SmallBoat_udtf.py",
    function_name="small_boat_udtf"
)
```

### Session Scope Notes

Session-scoped registration is the supported mode for scalar-only UDTFs. Functions are temporary and must be registered at the start of each notebook/job before running SQL queries.

## Requirements

**Note:** This document uses PyPI package names for references:
- **PyPI:** `cognite-pygen` (repository: `pygen`; import: `cognite.pygen`)
- **PyPI:** `cognite-pygen-spark` (repository: `pygen-spark`; import: `cognite.pygen_spark`)

- Python 3.9+
- `cognite-pygen-spark` (PyPI package name; import: `cognite.pygen_spark`)
- `cognite-sdk-python` (dependency)
- `databricks-sdk` (dependency)

## Package Structure

```
cognite-databricks/
├── cognite/
│   └── databricks/
│       ├── __init__.py            # Exports generate_udtf_notebook, UDTFGenerator, etc.
│       ├── udtf_registry.py        # UDTF registration helpers
│       ├── secret_manager.py      # Secret Manager helpers
│       ├── view_generator.py       # View generation and registration
│       ├── generator.py            # generate_udtf_notebook helper function
│       └── utils.py                # Utility functions
├── pyproject.toml
└── README.md
```

## Core Components

### `generate_udtf_notebook`

High-level function for notebook workflows, aligned with `pygen.generate_sdk_notebook`:

```python
from cognite.databricks import generate_udtf_notebook

generator = generate_udtf_notebook(
    data_model_id,
    client,
    catalog="main",
    schema="cdf_models",
)
```

### `UDTFGenerator`

Main class for orchestrating UDTF generation and registration:

```python
from cognite.databricks import UDTFGenerator

generator = UDTFGenerator(
    workspace_client=workspace_client,
    cognite_client=client,
    catalog="main",
    schema="cdf_models",
)
```

**Key Methods:**
- `register_session_scoped_udtfs()`: Register UDTFs for catalog-based use (scalar-only)
- `register_udtf_from_file()`: Register a single generated UDTF file in the current session

### `register_udtf_from_file`

Standalone function for registering a single UDTF from a generated Python file for catalog-based use:

```python
from cognite.databricks import register_udtf_from_file

register_udtf_from_file(
    "/path/to/SmallBoat_udtf.py",
    function_name="small_boat_udtf"
)
```

### `register_udtf_from_file`

Standalone function for registering a single UDTF from a generated Python file for catalog-based use. Useful when you only need to register one UDTF or want more control over the registration process.

```python
from cognite.databricks import register_udtf_from_file

register_udtf_from_file(
    "/path/to/SmallBoat_udtf.py",
    function_name="small_boat_udtf"
)
```

### `SecretManagerHelper`

Helper for managing OAuth2 credentials in Databricks Secret Manager:

```python
from cognite.databricks import SecretManagerHelper

secret_helper = SecretManagerHelper(workspace_client)
secret_helper.set_cdf_credentials(
    scope_name="cdf_sp_pygen_power_windturbine",
    project="my-project",
    cdf_cluster="api.cognitedata.com",
    client_id="...",
    client_secret="...",
    tenant_id="...",
)
```

## Development

### Setup

```bash
git clone <repository-url>
cd cognite-databricks
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```


## Related Packages

- **[cognite-pygen-spark](https://github.com/cognitedata/pygen-spark)** (PyPI: `cognite-pygen-spark`): Generic Spark UDTF code generation library that works with any Spark cluster. Provides template-based UDTF generation, type conversion utilities (`TypeConverter`), connection configuration (`CDFConnectionConfig`), and utility functions. `cognite-databricks` uses `pygen-spark` for all code generation.
- **[cognite-pygen](https://github.com/cognitedata/pygen)** (PyPI: `cognite-pygen`): Base code generation library for CDF Data Models
- **[cognite-sdk-python](https://github.com/cognitedata/cognite-sdk-python)**: Python SDK for CDF APIs

### Import Paths for Generic Components

Generic components (`TypeConverter`, `CDFConnectionConfig`, `to_udtf_function_name`) are provided by `pygen-spark` and re-exported from `cognite-databricks` for backward compatibility:

```python
# Preferred: Import directly from pygen-spark (source)
from cognite.pygen_spark import TypeConverter, CDFConnectionConfig, to_udtf_function_name

# Backward compatible: Still works (re-exported from pygen-spark)
from cognite.databricks import TypeConverter, CDFConnectionConfig, to_udtf_function_name
```

**Note**: These components are generic Spark utilities and work with any Spark cluster, not just Databricks. They were moved from `cognite-databricks` to `pygen-spark` to make them available for standalone Spark clusters.

## Documentation

For detailed documentation, see:

- **[Documentation Index](docs/index.md)**: Complete guide for catalog-based scalar-only UDTF registration
- **[Unity Catalog UDTF Registration](docs/session_scoped/index.md)**: Session-scoped workflow and SQL usage
- **[Technical Plan - CDF Databricks Integration (UDTF-Based)](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md)**: Architecture and design details

## License

[License information]

## Contributing

[Contributing guidelines]

