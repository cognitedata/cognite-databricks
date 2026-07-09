# Prerequisites

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
- **cdf_cluster**: CDF cluster name (e.g., `westeurope-1`) — look up your cluster and **Cognite API URL** in the [multi-tenant clusters table](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters)
- **project**: CDF project name

These credentials are typically stored in a TOML file and then transferred to Databricks Secret Manager.

### Example CDF config TOML (no secrets)

Use the following structure. Replace placeholders with your real values and **do not commit** the file with secrets. In Databricks, store it under a path like `/Workspace/Users/<your-user>/config.toml`.

```toml
# Example CDF configuration for cognite-databricks.
# Copy, replace placeholders, and do not commit secrets.

[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "westeurope-1"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
```

An example file with this content is in the repo at `docs/catalog_based/example_config.toml`. Copy it and fill in your values.

### Base URL and `base_url` in TOML

Every customer must know their **Cognite API URL** before writing TOML. Most multi-tenant clusters use `{cluster}.cognitedata.com` — set `cdf_cluster` only. Add `base_url` when the published URL differs (e.g. `europe-west1-1` → `api.cognitedata.com`) or for dedicated / PSaaS / Private Link. See [CDF base URL and TOML deployment](../private_link_psaas.md).

Example: `docs/catalog_based/example_config_private_link.toml`

## CDF Data Model

A CDF Data Model with Views (for Data Model UDTFs) or Time Series (for Time Series UDTFs) is required.

## Next Steps

Once prerequisites are met, proceed to [Secret Manager Setup](./secret_manager.md) to configure secure credential storage.


