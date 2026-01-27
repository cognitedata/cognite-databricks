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

1. **[Quickstart](./quickstart.md)**: Get from zero to registered UDTFs and Views in one notebook (catalog-based)
2. **[Prerequisites](./prerequisites.md)**: System requirements and access permissions
3. **[Secret Manager](./secret_manager.md)**: Set up secure credential storage
4. **[Registration](./registration.md)**: Register UDTFs and Views in Unity Catalog
5. **[Views](./views.md)**: Understand Views and their benefits
6. **[Querying](./querying.md)**: Query Views and UDTFs directly
7. **[Filtering](./filtering.md)**: Filter data using WHERE clauses with predicate pushdown
8. **[Joining](./joining.md)**: Join data from different Views based on `external_id` and `space`
9. **[Time Series](./time_series.md)**: Work with template-generated time series UDTFs in Unity Catalog (same template-based generation as Data Model UDTFs)
10. **[SQL-Native Time Series (Alpha)](./time_series_sql.md)**: SQL-native time series UDTF with pushdown hints (experimental)
11. **[Governance](./governance.md)**: Set up Unity Catalog permissions
12. **[Troubleshooting](./troubleshooting.md)**: Common issues and solutions

## Quick Links

### Quickstart

- [Catalog-Based Quickstart](./quickstart.md): End-to-end flow based on the init quickstart notebook (install → generate → Secret Manager → register UDTFs and Views)

### Examples

- [Registration and Views](../../examples/catalog_based/registration_and_views.ipynb): Secret Manager setup, UDTF/View registration, Unity Catalog verification
- [Querying Views](../../examples/catalog_based/querying_views.ipynb): Query Views (no credentials) and UDTFs directly
- [Filtering Views](../../examples/catalog_based/filtering_views.ipynb): Filter Views with WHERE clauses
- [Joining Views](../../examples/catalog_based/joining_views.ipynb): Join Views and join Views with time series UDTFs

### Related Documentation

- [Session-Scoped UDTF Registration](../session_scoped/index.md): For development and testing
- Technical Plan: CDF Databricks Integration (UDTF-Based)


