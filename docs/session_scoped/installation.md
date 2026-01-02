# Installation

## Prerequisites

- **Databricks Workspace**: Access to a Databricks workspace with an active Spark session
- **Python Dependencies**: Ability to install packages via `%pip` in Databricks notebooks
- **CDF Credentials**: Access to CDF credentials (client_id, client_secret, tenant_id, cdf_cluster, project)
- **CDF Data Model**: A CDF Data Model with Views (for Data Model UDTFs) or Time Series (for Time Series UDTFs)

## Install Required Packages

In a Databricks notebook, install the required dependencies:

```python
%pip install cognite-sdk cognite-databricks
```

**Important**: After installing packages, restart the Python kernel when prompted. This ensures all dependencies are properly loaded.

## Verify Installation

```python
from cognite.databricks import generate_udtf_notebook, register_udtf_from_file
from cognite.pygen import load_cognite_client_from_toml

# Verify imports work
print("✓ All imports successful")
```

## Next Steps

Once installation is complete, proceed to [Registration](./registration.md) to register your UDTFs.

