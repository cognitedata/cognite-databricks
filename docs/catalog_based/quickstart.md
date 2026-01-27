# Catalog-Based Quickstart

This quickstart gets you from zero to registered UDTFs and Views in Unity Catalog in a single notebook. It follows the flow: install → load clients → generate UDTFs → store credentials in Secret Manager → register UDTFs → register Views.

**Prerequisites**: Unity Catalog and Secret Manager access, a CDF data model, and CDF credentials in a TOML file. See [Prerequisites](./prerequisites.md) for details. For a redacted example TOML, see [Example CDF config TOML](./prerequisites.md#example-cdf-config-toml-no-secrets).

## 1. Install and load clients

```python
%pip install cognite-databricks

from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId
from databricks.sdk import WorkspaceClient
import toml

# Path to your CDF config — use the structure from docs/catalog_based/example_config.toml
toml_file_path = "/Workspace/Users/<your-user>/config.toml"
client = load_cognite_client_from_toml(toml_file_path)

workspace_client = WorkspaceClient()
```

## 2. Pick a warehouse and generate UDTFs

List warehouses, then create the generator with your data model, catalog, schema, and output directory:

```python
warehouses = list(workspace_client.warehouses.list())
# Use the warehouse your cluster uses, or the first one
w = warehouses[0]

data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="v1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,
    output_dir="/Workspace/Users/<your-user>/udtf",
    catalog="main",           # Your Unity Catalog catalog name
    schema="sailboat_sailboat_v1",  # Or None to auto-generate from data model
    warehouse_id=w.id,
    debug=False,
)
```

## 3. Store CDF credentials in Secret Manager

Derive a secret scope name from the data model and store credentials from your TOML:

```python
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"

toml_content = toml.load(toml_file_path)
cognite_config = toml_content.get("cognite", {})

generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project=cognite_config["project"],
    cdf_cluster=cognite_config["cdf_cluster"],
    client_id=cognite_config["client_id"],
    client_secret=cognite_config["client_secret"],
    tenant_id=cognite_config["tenant_id"],
)
```

The generator’s `secret_helper` creates the scope if it does not exist. For more control, see [Secret Manager](./secret_manager.md).

## 4. Register UDTFs and Views

Register UDTFs first, then Views (Views depend on UDTFs):

```python
udtf_result = generator.register_udtfs(
    secret_scope=secret_scope,
    if_exists="replace",  # "skip" | "replace" | "error"
    debug=False,
)

view_result = generator.register_views(
    secret_scope=secret_scope,
    if_exists="replace",
    debug=False,
)
```

Alternatively, use a single call:

```python
result = generator.register_udtfs_and_views(
    secret_scope=secret_scope,
    if_exists="replace",
    debug=False,
)
```

**Note**: `register_udtfs_and_views()` requires Databricks Runtime 18.1+. For earlier runtimes or step-by-step control, use `register_udtfs()` and `register_views()` in separate cells.

## 5. Verify in Unity Catalog

In the Databricks UI: **Catalog Explorer** → your catalog → your schema. You should see the UDTFs (functions) and Views. Views are searchable from the workspace search.

## Next steps

- [Prerequisites](./prerequisites.md): Requirements and permissions
- [Secret Manager](./secret_manager.md): Scope naming and manual setup
- [Registration](./registration.md): Registration options and behavior
- [Views](./views.md): Querying Views without credentials in SQL
- [Registration and Views example](../../examples/catalog_based/registration_and_views.ipynb): Full notebook in the repo
