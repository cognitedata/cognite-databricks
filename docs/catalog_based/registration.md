# Registration

## Complete Registration (Recommended)

The easiest way to register UDTFs and Views is using `register_udtfs_and_views()`:

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId
from databricks.sdk import WorkspaceClient

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")
workspace_client = WorkspaceClient()

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

# Generate UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,
    catalog="main",  # Unity Catalog catalog name
    schema=None,  # Auto-generated: "sailboat_sailboat_1"
    output_dir="/Workspace/Users/user@example.com/udtf",
)

# Register UDTFs and Views in Unity Catalog
# Secret scope auto-generated: cdf_sailboat_sailboat
result = generator.register_udtfs_and_views(
    secret_scope=None,  # Auto-generated from data model
    dependencies=["cognite-sdk>=7.90.1"],  # DBR 18.1+ custom dependencies
    if_exists="skip",  # Skip if already exists
    debug=False,
)

print(f"✓ Registered {result.total_count} UDTF(s) and View(s)")
print(f"  Catalog: {result.catalog}")
print(f"  Schema: {result.schema_name}")
```

## Individual UDTF and View Registration

For more control, you can register individual UDTFs and Views:

```python
from cognite.databricks import UDTFGenerator
from cognite.databricks import UDTFRegistry, SecretManagerHelper
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()
registry = UDTFRegistry(workspace_client)
secret_helper = SecretManagerHelper(workspace_client)

# Create secret scope and store credentials
secret_scope = "cdf_sailboat_sailboat"
secret_helper.create_scope_if_not_exists(secret_scope)
secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project="your-project",
    cdf_cluster="westeurope-1",
    client_id="your-client-id",
    client_secret="your-client-secret",
    tenant_id="your-tenant-id",
)

# Register a single UDTF
function_info = registry.register_udtf(
    catalog="main",
    schema="sailboat_sailboat_1",
    function_name="smallboat_udtf",
    udtf_code=udtf_code,  # Python code for the UDTF
    input_params=input_params,  # Function parameter definitions
    return_type="TABLE(external_id STRING, name STRING, ...)",
    return_params=return_params,  # Structured return column metadata
    dependencies=["cognite-sdk>=7.90.1"],  # DBR 18.1+
    comment="UDTF for SmallBoat view",
    if_exists="skip",
)

# Register a View
view_sql = f"""
CREATE OR REPLACE VIEW main.sailboat_sailboat_1.smallboat AS
SELECT * FROM main.sailboat_sailboat_1.smallboat_udtf(
    client_id => SECRET('{secret_scope}', 'client_id'),
    client_secret => SECRET('{secret_scope}', 'client_secret'),
    tenant_id => SECRET('{secret_scope}', 'tenant_id'),
    cdf_cluster => SECRET('{secret_scope}', 'cdf_cluster'),
    project => SECRET('{secret_scope}', 'project')
)
"""

registry.register_view(
    catalog="main",
    schema="sailboat_sailboat_1",
    view_name="smallboat",
    view_sql=view_sql,
    comment="View for SmallBoat data",
)
```

## Dependencies Handling

### DBR 18.1+ (Custom Dependencies)

For Databricks Runtime 18.1+, you can specify custom Python package dependencies:

```python
result = generator.register_udtfs_and_views(
    dependencies=[
        "cognite-sdk>=7.90.1",
        "cognite-pygen>=1.2.29",
    ],
)
```

**Note**: PyPI package names use hyphens (e.g., `cognite-sdk`), while import names use underscores (e.g., `cognite.client`).

### Pre-DBR 18.1 (Pre-installed Packages)

For DBR < 18.1, dependencies must be pre-installed on the cluster. Omit the `dependencies` parameter:

```python
# Dependencies must be pre-installed on cluster
result = generator.register_udtfs_and_views(
    dependencies=None,  # Uses fallback mode
)
```

## Next Steps

After registration, learn about [Views](./views.md) and how to query them.


