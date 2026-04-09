# Cognite Databricks Integration Documentation

## Start here (recommended)

**All customers:** begin with the **catalog-based quickstart** — install, generate UDTFs from your CDF data model, store credentials in **Databricks Secret Manager**, and register **Unity Catalog** functions and views.

- **[Quickstart (step-by-step)](./catalog_based/quickstart.md)** — full walkthrough with explained code blocks
- **[Quickstart notebook](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/quickstart.ipynb)** — same flow in a Databricks notebook (markdown + inline comments per cell)

**Prerequisites:** [Catalog-based prerequisites](./catalog_based/prerequisites.md)

---

## Overview

`cognite-databricks` provides two approaches for registering and using User-Defined Table Functions (UDTFs):

1. **[Catalog-based registration](./catalog_based/index.md)** — **default path** for most customers: permanent UDTFs and Views in **Unity Catalog**, credentials via **Secret Manager**.
2. **[Session-scoped registration](./session_scoped/index.md)** — temporary registration in a **single Spark session** for development and testing.

## Choosing the right approach

### Use session-scoped when

- Developing and testing UDTFs before committing to Unity Catalog
- Prototyping without Unity Catalog persistence
- Temporary analysis; learning UDTF patterns

**Characteristics:** functions live only for the session; no permanent catalog objects; often simpler credentials for ad-hoc use.

### Use catalog-based when

- Production deployments with governance
- Data discovery and searchable Views in the Databricks UI
- Fine-grained access control (GRANT/REVOKE)
- Team collaboration on shared SQL assets

**Characteristics:** UDTFs and Views in Unity Catalog; secrets in Secret Manager; Views hide `SECRET()` details from analysts.

---

## Documentation structure

### Catalog-based (start here)

- [Quickstart](./catalog_based/quickstart.md)
- [Overview](./catalog_based/index.md)
- [Prerequisites](./catalog_based/prerequisites.md)
- [Secret Manager](./catalog_based/secret_manager.md)
- [Registration](./catalog_based/registration.md)
- [Views](./catalog_based/views.md)
- [Querying](./catalog_based/querying.md)
- [Filtering](./catalog_based/filtering.md)
- [Joining](./catalog_based/joining.md)
- [Time Series](./catalog_based/time_series.md)
- [SQL-Native Time Series (Alpha)](./catalog_based/time_series_sql.md)
- [Governance](./catalog_based/governance.md)
- [Troubleshooting](./catalog_based/troubleshooting.md)

### Session-scoped

- [Overview](./session_scoped/index.md)
- [Installation](./session_scoped/installation.md)
- [Registration](./session_scoped/registration.md)
- [Querying](./session_scoped/querying.md)
- [Filtering](./session_scoped/filtering.md)
- [Joining](./session_scoped/joining.md)
- [Time Series](./session_scoped/time_series.md)
- [Troubleshooting](./session_scoped/troubleshooting.md)

## Examples

- **Catalog-based quickstart:** [quickstart.ipynb](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/quickstart.ipynb)
- **Session-scoped:** `examples/session_scoped/`
- **Other catalog examples:** `examples/catalog_based/`

## Package architecture

`cognite-databricks` extends `pygen-spark` with Databricks-specific features:

- **Code generation**: `cognite-pygen-spark` templates for Data Model and time series UDTFs
- **Databricks-specific**: Unity Catalog SQL registration and Secret Manager integration

**Import paths for generic components:**

```python
from cognite.pygen_spark import TypeConverter, CDFConnectionConfig, to_udtf_function_name
# Or: from cognite.databricks import ...
```

## Related resources

- [README](../README.md): Package overview and installation
- [pygen-spark](https://github.com/cognitedata/pygen-spark): Generic Spark UDTF code generation
