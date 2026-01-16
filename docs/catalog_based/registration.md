# Registration

## Split Registration Workflow (Recommended)

For better robustness and clarity, use the two-cell workflow pattern:

**Cell 1: Register UDTFs**

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

# Register UDTFs only (no views)
secret_scope = "cdf_sailboat_sailboat"  # Or auto-generated

udtf_result = generator.register_udtfs(
    secret_scope=secret_scope,
    if_exists="skip",
    debug=False,
)

print(f"✓ Registered {udtf_result.total_count} UDTF(s)")
print(f"  Catalog: {udtf_result.catalog}")
print(f"  Schema: {udtf_result.schema_name}")

# Print registered UDTFs
for udtf in udtf_result.registered_udtfs:
    print(f"  - {udtf.view_id}_udtf")
```

**Cell 2: Register Views (with Pre-Test Validation)**

```python
# Cell 2: Register Views (with pre-test validation)
# This cell will verify all UDTFs exist before creating views

view_result = generator.register_views(
    secret_scope=secret_scope,
    if_exists="skip",
    debug=False,
)

print(f"\n✓ Registered {view_result.total_count} View(s)")

# Print registered views
for view in view_result.registered_views:
    if view.view_registered:
        print(f"  ✓ {view.view_name}")
    else:
        print(f"  ✗ {view.view_id}: {view.error_message}")
```

**Benefits of Split Workflow:**
- **Robustness**: Pre-test validation ensures all UDTFs exist before creating views
- **Clarity**: Clear separation of concerns - UDTF registration vs view registration
- **Error Recovery**: Can retry view registration without re-registering UDTFs
- **Better Error Messages**: Clear, actionable error messages when UDTFs are missing

## Complete Registration (Convenience Method)

For convenience, you can still use `register_udtfs_and_views()` in a single call:

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

# Register UDTFs and Views in Unity Catalog (convenience method)
# Secret scope auto-generated: cdf_sailboat_sailboat
result = generator.register_udtfs_and_views(
    secret_scope=None,  # Auto-generated from data model
    if_exists="skip",  # Skip if already exists
    debug=False,
)

print(f"✓ Registered {result.total_count} UDTF(s) and View(s)")
print(f"  Catalog: {result.catalog}")
print(f"  Schema: {result.schema_name}")
```

**Note**: `register_udtfs_and_views()` is a convenience wrapper that calls `register_udtfs()` then `register_views()` sequentially with a delay for Unity Catalog propagation. For production use, consider using the split workflow in separate notebook cells.

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

## Next Steps

After registration, learn about [Views](./views.md) and how to query them.


