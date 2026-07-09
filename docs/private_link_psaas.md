# Private Link and PSaaS Setup

This guide explains how to connect **cognite-databricks** and **cognite-pygen-spark** to CDF when your project uses **Private Link** or **PSaaS** (Private Software as a Service) endpoints instead of the standard public CDF URL.

For background on Private Link provisioning, see the Cognite docs:

- [Configure Private Link on Azure](https://docs.cognite.com/cdf/access/guides/configure_private_link_azure)
- [Configure Private Link on AWS](https://docs.cognite.com/cdf/access/guides/configure_private_link_aws)

## Overview

| URL type | Example | Used for |
| --- | --- | --- |
| **Public cluster URL** | `https://westeurope-1.cognitedata.com` | OAuth scopes and token audience (Azure AD app registration) |
| **Private Link URL** | `https://p123.plink.westeurope-1.cognitedata.com` | Actual CDF API traffic from your network |

Standard tooling assumes API requests go to `https://{cdf_cluster}.cognitedata.com`. Private Link customers receive a different **base URL** from Cognite (format: `pNNN.plink.<cluster>.cognitedata.com`).

**cognite-pygen 1.3.0+** adds optional `base_url` support in TOML (and `--cdf-url` on the CLI) so provisioning tools can reach CDF through Private Link while OAuth still uses the public cluster name.

## Requirements

| Package | Minimum version | Role |
| --- | --- | --- |
| `cognite-pygen` | **1.3.0** | `load_cognite_client_from_toml()` reads `base_url` from TOML |
| `cognite-pygen-spark` | **0.3.1** | UDTF code generation (used by cognite-databricks) |
| `cognite-databricks` | **0.3.1** | Databricks registration; depends on pygen ≥ 1.3.0 |

Install or upgrade in a Databricks notebook:

```python
%pip install --upgrade "cognite-databricks>=0.3.1" "cognite-pygen>=1.3.0"
```

## TOML configuration

Add an optional `base_url` to the `[cognite]` section. Keep `cdf_cluster` as the **public cluster name** (not the Private Link hostname).

```toml
# Private Link / PSaaS example — do not commit secrets.
[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "westeurope-1"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
base_url = "https://p123.plink.westeurope-1.cognitedata.com"
```

| Field | Required | Description |
| --- | --- | --- |
| `cdf_cluster` | Yes | Public cluster name (e.g. `westeurope-1`). Used for OAuth scopes: `https://{cdf_cluster}.cognitedata.com/.default` |
| `base_url` | No | Full Private Link URL (with `https://`). Overrides where the Cognite client sends API requests |
| `project`, `tenant_id`, `client_id`, `client_secret` | Yes | Same as standard setups |

A redacted example file is in the repo: [`docs/catalog_based/example_config_private_link.toml`](./catalog_based/example_config_private_link.toml).

### How `load_cognite_client_from_toml` applies `base_url`

`cognite-pygen` loads credentials, creates the client with `default_oauth_client_credentials()` (using `cdf_cluster` for OAuth), then overrides the API endpoint:

```python
base_url = toml_content.pop("base_url", None)
client = CogniteClient.default_oauth_client_credentials(**toml_content)
if base_url:
    client.config.base_url = base_url
```

Omitting `base_url` preserves the default public URL behavior.

---

## cognite-databricks (Unity Catalog)

Private Link affects **provisioning** (loading the data model, generating UDTFs, registering in Unity Catalog) when your Databricks workspace reaches CDF only through Private Link.

### 1. Create your TOML file

Store the file in the workspace, for example:

`/Workspace/Users/<your-email>/config/credentials.toml`

Use the [Private Link TOML example](./catalog_based/example_config_private_link.toml) as a template.

### 2. Load the CDF client (provisioning)

Same as the [catalog quickstart](./catalog_based/quickstart.md), but the TOML must include `base_url`:

```python
from cognite.pygen import load_cognite_client_from_toml

toml_file_path = "/Workspace/Users/<your-email>/config/credentials.toml"

# Uses base_url from TOML when present — required for Private Link during codegen.
client = load_cognite_client_from_toml(toml_file_path)
```

Verify connectivity before generating UDTFs:

```python
# Quick sanity check — should succeed against your Private Link endpoint.
client.iam.token.inspect()
```

### 3. Generate and register UDTFs

Continue the [quickstart](./catalog_based/quickstart.md) flow (`generate_udtf_notebook`, Secret Manager, `register_udtfs`, `register_views`). No API changes are required beyond the TOML `base_url` for the provisioning client.

### 4. Store credentials in Secret Manager

`set_cdf_credentials()` stores the **public** `cdf_cluster` name (for OAuth scopes at query time). It does not store `base_url`:

```python
import toml

toml_content = toml.load(toml_file_path)
cognite_config = toml_content["cognite"]

generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project=cognite_config["project"],
    cdf_cluster=cognite_config["cdf_cluster"],  # public cluster name
    client_id=cognite_config["client_id"],
    client_secret=cognite_config["client_secret"],
    tenant_id=cognite_config["tenant_id"],
)
```

### Query-time behavior (UDTFs)

Generated UDTFs (from **cognite-pygen-spark** templates) build API URLs as `https://{cdf_cluster}.cognitedata.com` using the `cdf_cluster` value passed from SQL / Secret Manager. OAuth scopes use the same public cluster pattern.

| Phase | `base_url` support | Notes |
| --- | --- | --- |
| **Provisioning** (TOML → `load_cognite_client_from_toml`) | Yes | Use `base_url` in TOML |
| **Query time** (UDTF `eval()` via `SECRET('…', 'cdf_cluster')`) | Public URL only today | Works when workers can reach the public CDF endpoint |

If your Spark workers can only reach CDF through Private Link at query time, contact your Cognite team — runtime `base_url` in Secret Manager and UDTF templates is on the roadmap. Until then, ensure network routing from Databricks to the appropriate CDF endpoint matches how UDTFs resolve URLs.

---

## cognite-pygen-spark (generic Spark)

For standalone Spark clusters (no Databricks), use the same TOML shape and **cognite-pygen** client loading during [generation](https://github.com/cognitedata/pygen-spark/blob/main/docs/guide/generation.md).

```python
from pathlib import Path

from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from cognite.pygen_spark import SparkUDTFGenerator

# TOML must include base_url for Private Link.
client = load_cognite_client_from_toml("config.toml")

generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=DataModelId(space="my_space", external_id="MyModel", version="1"),
    top_level_package="cognite_udtfs",
)
result = generator.generate_udtfs()
```

See the [pygen-spark Private Link guide](https://github.com/cognitedata/pygen-spark/blob/main/docs/guide/private_link_psaas.md) for session registration and querying notes.

### `CDFConnectionConfig` (pygen-spark)

`CDFConnectionConfig.from_toml()` loads standard fields but does **not** yet read `base_url`. For Private Link, prefer `load_cognite_client_from_toml()` for client creation, or build the client manually:

```python
from cognite.client import CogniteClient
from cognite.pygen import load_cognite_client_from_toml

client = load_cognite_client_from_toml("config.toml")
```

---

## pygen CLI (`--cdf-url`)

When using the **pygen** CLI directly (without Databricks), pass the Private Link URL:

```bash
pygen generate \
  --space my_space \
  --external-id MyModel \
  --version 1 \
  --tenant-id <tenant-id> \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --cdf-cluster westeurope-1 \
  --cdf-url https://p123.plink.westeurope-1.cognitedata.com \
  --cdf-project my-project
```

`--cdf-cluster` remains the public cluster name; `--cdf-url` overrides the API `base_url`.

---

## Troubleshooting

### `403` — Traffic from this source is forbidden

You are hitting the **public** CDF endpoint from a network that must use Private Link. Add `base_url` to your TOML (provisioning) or verify network routing (query time).

### `TypeError` — unexpected keyword argument `base_url`

Upgrade **cognite-pygen** to 1.3.0 or later:

```python
%pip install --upgrade "cognite-pygen>=1.3.0"
```

### OAuth succeeds but API calls fail (or vice versa)

Confirm:

- `cdf_cluster` is the **public** cluster name (used for scopes).
- `base_url` is the full Private Link URL including `https://`.
- Your Azure AD app is registered against the correct CDF resource (see Cognite Private Link setup docs).

### Provisioning works; UDTF queries fail

Provisioning uses `load_cognite_client_from_toml` (`base_url` aware). UDTF queries use `cdf_cluster` from secrets and the public URL pattern — see [Query-time behavior](#query-time-behavior-udtfs) above.

---

## Related documentation

- [Catalog-based quickstart](./catalog_based/quickstart.md)
- [Prerequisites](./catalog_based/prerequisites.md)
- [Secret Manager](./catalog_based/secret_manager.md)
- [pygen-spark Private Link guide](https://github.com/cognitedata/pygen-spark/blob/main/docs/guide/private_link_psaas.md)
- [cognite-pygen 1.3.0 release](https://github.com/cognitedata/pygen/releases/tag/1.3.0)
