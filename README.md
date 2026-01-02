# cognite-databricks

A helper SDK for Databricks that provides UDTF registration utilities, Secret Manager integration, and Unity Catalog View generation for CDF Data Models.

## Overview

`cognite-databricks` simplifies the process of registering CDF Data Models as discoverable Unity Catalog Views in Databricks. It provides high-level APIs for:

- **UDTF Registration**: Register Python UDTFs in Unity Catalog
- **Secret Manager Integration**: Manage OAuth2 credentials securely
- **View Generation**: Create Unity Catalog Views with Secret injection
- **Notebook-Friendly API**: Aligned with `cognite.pygen`'s notebook workflow

## Features

- **Two Registration Modes**:
  - **Unity Catalog**: Permanent, discoverable UDTFs with governance (production)
  - **Session-Scoped**: Quick testing and development without Unity Catalog (development)
- **One-Line Registration**: Generate and register UDTFs and Views with a single function call
- **Secret Manager Integration**: Automatic credential management from TOML files
- **Unity Catalog Integration**: Native support for Unity Catalog function and view registration
- **DBR 18.1+ Support**: Custom dependency support for UDTFs
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

# Register UDTFs and Views in Unity Catalog
# Includes both data model UDTFs and time series UDTFs (all template-generated)
# Scope name auto-generated from data model: cdf_{space}_{external_id}
result = generator.register_udtfs_and_views(
    secret_scope=None,  # Auto-generated if None
    dependencies=["cognite-sdk>=6.0.0"],
)
print(f"Registered {result.total_count} UDTF(s) including time series UDTFs")
```

### Low-Level API

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import UDTFGenerator, SecretManagerHelper
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")
workspace_client = WorkspaceClient()

# Create generator
generator = UDTFGenerator(
    workspace_client=workspace_client,
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

# Register UDTFs and Views
# For DBR 18.1+: dependencies are bundled with UDTF
# For pre-DBR 18.1: set dependencies=None and pre-install packages on cluster
registered = generator.register_udtfs_and_views(
    data_model=data_model_id,
    secret_scope=secret_scope,
    dependencies=["cognite-sdk>=6.0.0"],  # DBR 18.1+ only
)
```

## Session-Scoped UDTF Registration

For development, testing, or DBR < 18.1 environments, you can register UDTFs for session-scoped use without Unity Catalog registration. This allows you to test UDTFs quickly using `%pip install cognite-sdk` in a notebook.

### Register All UDTFs (Recommended)

```python
from databricks.sdk import WorkspaceClient
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Create WorkspaceClient (auto-detects credentials in Databricks)
workspace_client = WorkspaceClient()

# Load client and generate UDTFs
client = load_cognite_client_from_toml("config.toml")
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,  # Include this for full functionality
    output_dir="/Workspace/Users/user@example.com/udtf",
    catalog="main",  # Optional but recommended
    schema="sailboat_sailboat_v1",  # Optional but recommended
)

# Install dependencies (run in separate cell first)
# %pip install cognite-sdk
# (Restart kernel after installation)

# Register all UDTFs for session-scoped use (includes time series UDTFs automatically)
registered = generator.register_session_scoped_udtfs()
# Returns: {"SmallBoat": "smallboat_udtf", "LargeBoat": "largeboat_udtf", 
#           "time_series_datapoints": "time_series_datapoints_udtf", ...}

# Use in SQL (always use SECRET() for credentials)
# SELECT * FROM smallboat_udtf(
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
    function_name="smallboat_udtf"
)
```

### When to Use Session-Scoped vs Unity Catalog

| Feature | Session-Scoped | Unity Catalog |
|---------|---------------|---------------|
| **Registration** | Per-session, temporary | Permanent, catalog-wide |
| **Dependencies** | Install via `%pip` | Bundled (DBR 18.1+) or pre-installed |
| **Use Case** | Development, testing, prototyping | Production, governance, discovery |
| **DBR Version** | All versions | All versions (with limitations) |
| **Searchable** | No | Yes (via Databricks Search) |
| **Permissions** | Session-level | Unity Catalog permissions |

**Recommendation**: Use session-scoped registration for development and testing, then register in Unity Catalog for production use.

## Requirements

**Note:** This document uses PyPI package names for references:
- **PyPI:** `cognite-pygen` (repository: `pygen`; import: `cognite.pygen`)
- **PyPI:** `cognite-pygen-spark` (repository: `pygen-spark`; import: `cognite.pygen_spark`)

- Python 3.9+
- `cognite-pygen-spark` (PyPI package name; import: `cognite.pygen_spark`)
- `cognite-sdk-python` (dependency)
- `databricks-sdk` (dependency)
- **Databricks Runtime 18.1+** (for custom dependencies in UDTFs)
  - **Pre-DBR 18.1**: Works with pre-installed packages (see [Pre-DBR 18.1 Usage](#pre-dbr-181-usage) below)

## Package Structure

```
cognite-databricks/
├── cognite/
│   └── databricks/
│       ├── __init__.py            # Exports generate_udtf_notebook, UDTFGenerator, etc.
│       ├── udtf_registry.py        # UDTF registration in Unity Catalog
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
- `register_udtfs_and_views()`: Register all UDTFs and Views in Unity Catalog (production)
- `register_session_scoped_udtfs()`: Register UDTFs for session-scoped use (development/testing)

### `register_udtf_from_file`

Standalone function for registering a single UDTF from a generated Python file for session-scoped use:

```python
from cognite.databricks import register_udtf_from_file

register_udtf_from_file(
    "/path/to/SmallBoat_udtf.py",
    function_name="smallboat_udtf"
)
```

### `UDTFRegistry`

Utility for registering Python UDTFs in Unity Catalog:

```python
from cognite.databricks import UDTFRegistry

registry = UDTFRegistry(workspace_client)
function_info = registry.register_udtf(
    catalog="main",
    schema="cdf",
    function_name="pump_view_udtf",
    udtf_code=udtf_code,
    input_params=[...],
    return_type="TABLE(...)",
    dependencies=["cognite-sdk>=6.0.0"],  # DBR 18.1+
)
```

### `register_udtf_from_file`

Standalone function for registering a single UDTF from a generated Python file for session-scoped use. Useful when you only need to register one UDTF or want more control over the registration process.

```python
from cognite.databricks import register_udtf_from_file

register_udtf_from_file(
    "/path/to/SmallBoat_udtf.py",
    function_name="smallboat_udtf"
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

## Pre-DBR 18.1 Usage

For Databricks Runtime versions prior to 18.1, custom dependencies are not supported. You must pre-install required packages on your cluster:

### Step 1: Install Packages on Cluster

```bash
# In a Databricks notebook cell
%pip install cognite-sdk>=6.0.0 cognite-pygen-spark>=0.1.0
```

Or configure cluster libraries via the Databricks UI.

### Step 2: Register UDTFs Without Dependencies

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml

client = load_cognite_client_from_toml("config.toml")
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
generator = generate_udtf_notebook(data_model_id, client)

# Omit dependencies parameter or set to None
generator.register_udtfs_and_views(
    secret_scope=None,  # Auto-generated
    dependencies=None,  # Required for pre-DBR 18.1
)
```

**Note:** The code automatically detects when `dependencies=None` and uses the fallback registration path that works on all DBR versions.

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

- **[Documentation Index](docs/index.md)**: Complete guide covering both session-scoped and catalog-based UDTF registration
- **[Session-Scoped UDTF Registration](docs/session_scoped/index.md)**: Development and testing workflow
- **[Catalog-Based UDTF Registration](docs/catalog_based/index.md)**: Production deployment with Unity Catalog
- **[Technical Plan - CDF Databricks Integration (UDTF-Based)](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md)**: Architecture and design details

## License

[License information]

## Contributing

[Contributing guidelines]

