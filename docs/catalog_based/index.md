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

1. **[Prerequisites](./prerequisites.md)**: System requirements and access permissions
2. **[Secret Manager](./secret_manager.md)**: Set up secure credential storage
3. **[Registration](./registration.md)**: Register UDTFs and Views in Unity Catalog
4. **[Views](./views.md)**: Understand Views and their benefits
5. **[Querying](./querying.md)**: Query Views and UDTFs directly
6. **[Filtering](./filtering.md)**: Filter data using WHERE clauses with predicate pushdown
7. **[Joining](./joining.md)**: Join data from different Views based on `external_id` and `space`
8. **[Time Series](./time_series.md)**: Work with template-generated time series UDTFs in Unity Catalog (same template-based generation as Data Model UDTFs)
9. **[Governance](./governance.md)**: Set up Unity Catalog permissions
10. **[Troubleshooting](./troubleshooting.md)**: Common issues and solutions

## Quick Links

### Examples

- [Registration and Views](../../examples/catalog_based/registration_and_views.ipynb): Secret Manager setup, UDTF/View registration, Unity Catalog verification
- [Querying Views](../../examples/catalog_based/querying_views.ipynb): Query Views (no credentials) and UDTFs directly
- [Filtering Views](../../examples/catalog_based/filtering_views.ipynb): Filter Views with WHERE clauses
- [Joining Views](../../examples/catalog_based/joining_views.ipynb): Join Views and join Views with time series UDTFs

### Related Documentation

- [Session-Scoped UDTF Registration](../session_scoped/index.md): For development and testing
- Technical Plan: CDF Databricks Integration (UDTF-Based)


