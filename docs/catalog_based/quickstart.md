# Quickstart (Unity Catalog) — start here

This guide is the **recommended starting path for all customers**: one flow from **install** → **CDF + Databricks clients** → **generate UDTFs** → **Secret Manager** → **register UDTFs and Views** in **Unity Catalog**.

**Notebook (copy-paste friendly):** [quickstart.ipynb on GitHub](https://github.com/cognitedata/cognite-databricks/blob/main/examples/catalog_based/quickstart.ipynb) — same steps as below, with **markdown explanations** and **inline comments** in each code cell. (Absolute URL so the link works on [Read the Docs](https://cognite-cognite-databricks.readthedocs-hosted.com/en/latest/catalog_based/quickstart.html) as well as in the repo.)

**Prerequisites:** [Prerequisites](./prerequisites.md) (Unity Catalog, Secret Manager, CDF data model, TOML with `[cognite]` credentials).

---

## 1. Install

Install the package from PyPI. In a Databricks notebook:

```python
# Installs cognite-databricks (Unity Catalog registration, Secret Manager, notebook helpers).
%pip install --upgrade cognite-databricks
```

Restart the kernel if the installer tells you to.

---

## 2. Imports and CDF client

- **`load_cognite_client_from_toml`**: builds a Cognite client from TOML — same idea as `cognite-pygen` notebooks. Used to **load the data model** and talk to CDF.
- **TOML during provisioning**: you read credentials from a file to **seed** Secret Manager. **End users querying Views do not use this file**; SQL uses `SECRET()`.

```python
from cognite.databricks import generate_udtf_notebook
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

import toml

# Path to your CDF config in the workspace (see example structure in prerequisites).
toml_file_path = "/Workspace/Users/<your-email>/config/credentials.toml"

# CDF API client — validates config and fetches the data model for codegen.
client = load_cognite_client_from_toml(toml_file_path)
```

---

## 3. Databricks workspace and SQL warehouse

- **`WorkspaceClient`**: authenticates to Databricks (PAT/OAuth in the notebook environment) for **SQL** and **Secrets** APIs.
- **Warehouse**: `CREATE FUNCTION` / view DDL runs **through a SQL warehouse**. Pick one your principal can use (often your team’s default SQL warehouse).

```python
# Uses notebook-attached Databricks credentials.
workspace_client = WorkspaceClient()

# Inspect available Pro SQL warehouses (name + id).
warehouses = list(workspace_client.warehouses.list())
for w in warehouses:
    print(f"Name: {w.name}, ID: {w.id}")

if not warehouses:
    raise RuntimeError("No SQL warehouses found. Create one in Databricks SQL.")

# Pick explicitly — avoid using the last loop variable by mistake.
warehouse = warehouses[0]
# Or: next(w for w in warehouses if w.name == "Analytics WH")

print(f"Using warehouse: {warehouse.name} ({warehouse.id})")
```

---

## 4. Generate UDTF Python files

- **`generate_udtf_notebook`**: uses **cognite-pygen-spark** templates to write **Python UDTF modules** under `output_dir`.
- **`catalog` / `schema`**: Unity Catalog destination for functions and views.
- **`workspace_client`**: **required** for catalog-based registration and Secret Manager helpers.

```python
# CDF data model to expose in Databricks (space, external id, version).
data_model_id = DataModelId(space="cdf_cdm", external_id="CogniteCore", version="v1")

generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,
    output_dir="/Workspace/Users/<your-email>/udtf_generated",
    catalog="my_catalog",  # Your Unity Catalog name
    schema="CDF_CogniteCore_v1",  # Or None to auto-derive from the data model
    warehouse_id=warehouse.id,
    debug=False,
)
```

---

## 5. Persist CDF credentials in Secret Manager

- **Scope naming**: `cdf_{space}_{external_id.lower()}` aligns with generated SQL that references `SECRET('cdf_...', 'client_id')`, etc.
- **`set_cdf_credentials`**: creates the scope if missing; stores **project**, **cdf_cluster**, **client_id**, **client_secret**, **tenant_id**.

```python
# Stable secret scope name for this data model.
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"

# Load the same TOML again and push values into Databricks Secret Manager.
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

More detail: [Secret Manager](./secret_manager.md).

---

## 6. Register UDTFs, then Views

- **`register_udtfs`**: creates **Unity Catalog functions** from generated files.
- **`register_views`**: creates **views** over those UDTFs so analysts can query without embedding secrets.
- **`if_exists`**: `replace` for iterative setup; `skip` / `error` for stricter pipelines.
- **`debug=True`**: verbose logging of registration steps.

```python
# Register functions first (views depend on them).
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

### Optional: single call (Runtime 18.1+)

```python
result = generator.register_udtfs_and_views(
    secret_scope=secret_scope,
    if_exists="replace",
    debug=False,
)
```

**Note:** `register_udtfs_and_views()` requires **Databricks Runtime 18.1+**. For older runtimes or step-by-step debugging, keep **two calls** as above.

---

## 7. Verify and continue

- **Databricks UI**: Catalog Explorer → your **catalog** → **schema** → functions and views.
- **Docs**: [Querying](./querying.md), [Registration](./registration.md), [Views](./views.md).

## Next steps

- [Prerequisites](./prerequisites.md) — permissions and example TOML
- [Registration and Views example](../../examples/catalog_based/registration_and_views.ipynb) — deeper walkthrough
- [Session-scoped workflow](../session_scoped/index.md) — temporary session UDTFs for development (no Unity Catalog persistence)
