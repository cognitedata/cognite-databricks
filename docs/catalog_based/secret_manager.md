# Secret Manager Setup

## Creating Secret Scopes

Secret scopes are used to store CDF credentials securely. The scope name is typically auto-generated from the data model:

```python
from cognite.databricks import SecretManagerHelper
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()

# Create Secret Manager helper
secret_helper = SecretManagerHelper(workspace_client)

# Create scope (auto-generated name: cdf_{space}_{external_id})
secret_scope = "cdf_sailboat_sailboat"
secret_helper.create_scope_if_not_exists(secret_scope)
```

## Storing Credentials from TOML

Credentials from your TOML file can be stored in Secret Manager:

```python
from cognite.pygen import load_cognite_client_from_toml
from cognite.databricks import SecretManagerHelper
from databricks.sdk import WorkspaceClient

# Load credentials from TOML
client = load_cognite_client_from_toml("config.toml")

# Extract credentials (you'll need to read the TOML file directly)
import tomli  # or tomllib in Python 3.11+
with open("config.toml", "rb") as f:
    config = tomli.load(f)

cognite_config = config["cognite"]

# Store in Secret Manager
workspace_client = WorkspaceClient()
secret_helper = SecretManagerHelper(workspace_client)

secret_scope = "cdf_sailboat_sailboat"
secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project=cognite_config["project"],
    cdf_cluster=cognite_config["cdf_cluster"],
    client_id=cognite_config["client_id"],
    client_secret=cognite_config["client_secret"],
    tenant_id=cognite_config["tenant_id"],
)

print(f"✓ Credentials stored in scope: {secret_scope}")
```

## Auto-Generated Scope Names

When using `register_udtfs_and_views()`, the secret scope name is auto-generated from the data model:

```python
# Auto-generated scope name: cdf_{space}_{external_id}
# Example: DataModelId(space="sailboat", external_id="sailboat", version="1")
#          -> scope: "cdf_sailboat_sailboat"
```

You can override this by providing a custom `secret_scope` parameter:

```python
generator.register_udtfs_and_views(
    secret_scope="my_custom_scope"  # Override auto-generated name
)
```

## Next Steps

After setting up Secret Manager, proceed to [Registration](./registration.md) to register UDTFs and Views.


