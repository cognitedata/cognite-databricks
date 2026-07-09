# Catalog-Based UDTF Registration

## Introduction

Catalog-based UDTF registration registers User-Defined Table Functions (UDTFs) and Views in Unity Catalog, providing permanent, discoverable, and governable access to CDF data. This approach is ideal for:

- **Production Deployments**: Permanent registration with Unity Catalog governance
- **Data Discovery**: Views are indexed and searchable in the Databricks UI
- **Access Control**: Unity Catalog permissions (GRANT/REVOKE) for fine-grained access control
- **Enterprise Security**: Credentials stored securely in Databricks Secret Manager
- **Team Collaboration**: Shared, discoverable data assets across teams

Unity Catalog provides a three-level hierarchy: `catalog.schema.object` where UDTFs and Views are registered as objects within schemas, which are organized within catalogs.

## Overview

This documentation covers the complete workflow for using catalog-based UDTFs and Views:

1. **[Deployment concepts](./deployment.md)**: Base URL, TOML, and how to verify deployment — **read first**
2. **[Quickstart](./quickstart.md)**: **Run this** — zero to registered UDTFs and Views (same flow as [quickstart.ipynb on GitHub](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/quickstart.ipynb))
3. **[Prerequisites](./prerequisites.md)**: System requirements and access permissions
4. **[Secret Manager](./secret_manager.md)**: Set up secure credential storage
5. **[Registration](./registration.md)**: Register UDTFs and Views in Unity Catalog
6. **[Views](./views.md)**: Understand Views and their benefits
7. **[Querying](./querying.md)**: Query Views and UDTFs directly
8. **[Filtering](./filtering.md)**: Filter data using WHERE clauses with predicate pushdown
9. **[Joining](./joining.md)**: Join data from different Views based on `external_id` and `space`
10. **[Time Series](./time_series.md)**: Work with template-generated time series UDTFs in Unity Catalog (same template-based generation as Data Model UDTFs)
11. **[SQL-Native Time Series (Alpha)](./time_series_sql.md)**: SQL-native time series UDTF with pushdown hints (experimental)
12. **[Governance](./governance.md)**: Set up Unity Catalog permissions
13. **[Troubleshooting](./troubleshooting.md)**: Common issues and solutions

## Quick Links

### Quickstart

- [Deployment concepts](./deployment.md): Base URL and TOML — read before the quickstart
- [Catalog-based quickstart](./quickstart.md): Step-by-step guide (explained code blocks)
- [Quickstart notebook](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/quickstart.ipynb): **Recommended notebook** — markdown sections + inline comments per step (install → generate → Secret Manager → register UDTFs and Views)

### Examples

- [Registration and Views](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/registration_and_views.ipynb): Secret Manager setup, UDTF/View registration, Unity Catalog verification
- [Querying Views](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/querying_views.ipynb): Query Views (no credentials) and UDTFs directly
- [Filtering Views](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/filtering_views.ipynb): Filter Views with WHERE clauses
- [Joining Views](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/joining_views.ipynb): Join Views and join Views with time series UDTFs

### Related Documentation

- [Session-Scoped UDTF Registration](../session_scoped/index.md): For development and testing
- Technical Plan: CDF Databricks Integration (UDTF-Based)


