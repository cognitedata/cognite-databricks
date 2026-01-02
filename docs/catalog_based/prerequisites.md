# Prerequisites

## Databricks Runtime 18.1+

Databricks Runtime 18.1+ is recommended for custom dependencies in UDTFs. This allows you to specify Python package dependencies directly when registering UDTFs.

For DBR < 18.1, dependencies must be pre-installed on the cluster.

## Unity Catalog Access

You need permissions to create and manage Unity Catalog resources:

- **CREATE_CATALOG**: Permission to create catalogs (or use existing catalog)
- **CREATE_SCHEMA**: Permission to create schemas within catalogs
- **CREATE_FUNCTION**: Permission to register UDTFs
- **CREATE_TABLE**: Permission to create Views

## Secret Manager Access

You need permissions to create secret scopes and store secrets:

- **CREATE_SECRET_SCOPE**: Permission to create secret scopes
- **WRITE_SECRET**: Permission to store secrets in scopes

## WorkspaceClient

Access to Databricks Workspace API is required. The `WorkspaceClient` automatically detects credentials in Databricks notebooks:

```python
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()  # Auto-detects credentials
```

## CDF Credentials

Access to CDF credentials is required:

- **client_id**: OAuth2 client ID
- **client_secret**: OAuth2 client secret
- **tenant_id**: Azure AD tenant ID
- **cdf_cluster**: CDF cluster name (e.g., "westeurope-1")
- **project**: CDF project name

These credentials are typically stored in a TOML file and then transferred to Databricks Secret Manager.

## CDF Data Model

A CDF Data Model with Views (for Data Model UDTFs) or Time Series (for Time Series UDTFs) is required.

## Next Steps

Once prerequisites are met, proceed to [Secret Manager Setup](./secret_manager.md) to configure secure credential storage.


