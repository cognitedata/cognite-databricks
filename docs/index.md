# Cognite Databricks Integration Documentation

## Overview

`cognite-databricks` provides two approaches for registering and using User-Defined Table Functions (UDTFs) to query CDF data from Databricks:

1. **[Session-Scoped Registration](./session_scoped/index.md)**: Temporary registration for development and testing
2. **[Catalog-Based Registration](./catalog_based/index.md)**: Permanent registration in Unity Catalog for production

## Choosing the Right Approach

### Use Session-Scoped Registration When:

- ✅ **Developing and Testing**: Quickly test UDTFs before committing to Unity Catalog
- ✅ **Prototyping**: Experiment with different configurations and queries
- ✅ **DBR < 18.1**: Using Databricks Runtime versions that support `%pip` installations
- ✅ **Temporary Analysis**: Running ad-hoc queries without permanent registration
- ✅ **Learning**: Getting familiar with UDTFs and CDF data access patterns

**Key Characteristics:**
- UDTFs are registered within a single Spark session
- No Unity Catalog access required
- Credentials passed directly in SQL queries (or via Secret Manager)
- Automatically cleaned up when session ends
- Faster setup, ideal for development

### Use Catalog-Based Registration When:

- ✅ **Production Deployments**: Permanent registration with Unity Catalog governance
- ✅ **Data Discovery**: Views are indexed and searchable in the Databricks UI
- ✅ **Access Control**: Need fine-grained permissions (GRANT/REVOKE)
- ✅ **Enterprise Security**: Credentials stored securely in Databricks Secret Manager
- ✅ **Team Collaboration**: Shared, discoverable data assets across teams
- ✅ **DBR 18.1+**: Using Databricks Runtime 18.1+ with custom dependency support

**Key Characteristics:**
- UDTFs and Views registered in Unity Catalog
- Permanent, discoverable, and governable
- Credentials managed via Secret Manager (no credentials in SQL)
- Views provide simplified query interface
- Requires Unity Catalog access and Secret Manager setup

## Quick Start

### Session-Scoped (Development)

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml

client = load_cognite_client_from_toml("config.toml")
generator = generate_udtf_notebook(data_model_id, client)
generator.register_session_scoped_udtfs()
```

See [Session-Scoped Documentation](./session_scoped/index.md) for complete guide.

### Catalog-Based (Production)

```python
from cognite.databricks import generate_udtf_notebook, SecretManagerHelper
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()
generator = generate_udtf_notebook(data_model_id, client, workspace_client=workspace_client)
result = generator.register_udtfs_and_views(secret_scope="cdf_scope")
```

See [Catalog-Based Documentation](./catalog_based/index.md) for complete guide.

## Documentation Structure

### Session-Scoped UDTF Registration
- [Installation](./session_scoped/installation.md)
- [Registration](./session_scoped/registration.md)
- [Querying](./session_scoped/querying.md)
- [Filtering](./session_scoped/filtering.md)
- [Joining](./session_scoped/joining.md)
- [Time Series](./session_scoped/time_series.md)
- [Troubleshooting](./session_scoped/troubleshooting.md)

### Catalog-Based UDTF Registration
- [Prerequisites](./catalog_based/prerequisites.md)
- [Secret Manager](./catalog_based/secret_manager.md)
- [Registration](./catalog_based/registration.md)
- [Views](./catalog_based/views.md)
- [Querying](./catalog_based/querying.md)
- [Filtering](./catalog_based/filtering.md)
- [Joining](./catalog_based/joining.md)
- [Time Series](./catalog_based/time_series.md)
- [Governance](./catalog_based/governance.md)
- [Troubleshooting](./catalog_based/troubleshooting.md)

## Examples

All examples are available in the `examples/` directory:

- **Session-Scoped Examples**: `examples/session_scoped/`
- **Catalog-Based Examples**: `examples/catalog_based/`

## Related Resources

- [README](../README.md): Package overview and installation
- [Technical Plan](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md): Architecture and design details
- [pygen-spark Documentation](https://github.com/cognitedata/pygen-spark): UDTF code generation library

