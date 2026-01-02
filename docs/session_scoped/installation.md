# Installation

## Prerequisites

- **Databricks Workspace**: Access to a Databricks workspace with an active Spark session
- **Python Dependencies**: Ability to install packages via `%pip` in Databricks notebooks
- **CDF Credentials**: Access to CDF credentials (client_id, client_secret, tenant_id, cdf_cluster, project)
- **CDF Data Model**: A CDF Data Model with Views (for Data Model UDTFs) or Time Series (for Time Series UDTFs)

## Install Required Packages

### Option 1: Install from PyPI (Recommended for Production)

In a Databricks notebook, install the required dependencies from PyPI:

```python
%pip install cognite-sdk cognite-databricks
```

**Important**: After installing packages, restart the Python kernel when prompted. This ensures all dependencies are properly loaded.

### Option 2: Install from Wheel Files (For Testing Local Builds)

If you're testing locally built wheel files, install them directly:

```python
%pip install --force-reinstall \
  /Workspace/Users/user@example.com/wheels/cognite_pygen_spark-0.1.0-py3-none-any.whl \
  /Workspace/Users/user@example.com/wheels/cognite_databricks-0.1.0-py3-none-any.whl
```

**Note**: Replace the paths with your actual wheel file locations. After installation, restart the Python kernel.

## Verify Installation

```python
from cognite.databricks import generate_udtf_notebook, register_udtf_from_file
from cognite.pygen import load_cognite_client_from_toml

# Verify imports work
print("✓ All imports successful")
```

## Next Steps

Once installation is complete, proceed to [Registration](./registration.md) to register your UDTFs.


