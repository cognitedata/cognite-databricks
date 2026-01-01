# Troubleshooting

## Common Issues and Solutions

### Issue: "CREATE_CATALOG permission denied"

**Solution**: Ensure you have `CREATE_CATALOG` permission or use an existing catalog:

```python
# Use existing catalog
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,
    catalog="main",  # Use existing catalog
)
```

### Issue: "CREATE_SCHEMA permission denied"

**Solution**: Ensure you have `CREATE_SCHEMA` permission on the catalog:

```sql
-- Grant CREATE_SCHEMA permission
GRANT CREATE_SCHEMA ON CATALOG main TO `user@example.com`;
```

### Issue: "SECRET() function not found"

**Solution**: Ensure the secret scope exists and contains the required secrets:

```python
# Verify secret scope exists
from cognite.databricks import SecretManagerHelper

secret_helper = SecretManagerHelper(workspace_client)
scope = secret_helper.create_scope_if_not_exists("cdf_sailboat_sailboat")

# Verify secrets are stored
secrets = workspace_client.secrets.list_secrets(scope="cdf_sailboat_sailboat")
print("Stored secrets:", [s.key for s in secrets])
```

### Issue: View returns no results

**Possible Causes:**
1. **Incorrect credentials**: Verify SECRET() values are correct
2. **No matching data**: Check that filters match existing data in CDF
3. **Time series doesn't exist**: Verify the time series external_id exists in CDF

**Debug Steps:**
```python
# Test credentials
from cognite.pygen import load_cognite_client_from_toml
client = load_cognite_client_from_toml("config.toml")

# Test data model query
from cognite.client.data_classes.data_modeling.ids import DataModelId
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")
views = client.data_modeling.views.list(data_model_id)
print(f"Found {len(views)} views")
```

### Issue: "Function already exists" error

**Solution**: Use `if_exists="skip"` to skip existing functions or `if_exists="replace"` to overwrite:

```python
result = generator.register_udtfs_and_views(
    if_exists="skip",  # Skip if already exists (default)
    # or
    if_exists="replace",  # Delete and recreate
)
```

### Issue: View creation fails with "end-of-input" error

**Solution**: This is often a false positive. The view may still be created successfully. Verify the view exists:

```python
# Check if view exists
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()
view = workspace_client.tables.get("main.sailboat_sailboat_1.smallboat")
if view and view.table_type == "VIEW":
    print("✓ View exists despite error message")
```

## Getting Help

If you encounter issues not covered here:

1. **Check the logs**: Look for error messages in the notebook output or Databricks logs
2. **Verify credentials**: Ensure CDF credentials are correct and have proper permissions
3. **Test with simple queries**: Start with basic queries before adding complex filters or joins
4. **Review the Technical Plan**: See the Technical Plan document for detailed architecture and implementation details

## Next Steps

After successfully registering UDTFs and Views in Unity Catalog:

1. **Set Up Permissions**: Configure Unity Catalog permissions for your teams (see [Governance](./governance.md))
2. **Create Documentation**: Document your Views for your users
3. **Monitor Usage**: Track View usage and performance in Databricks
4. **Optimize Queries**: Use predicate pushdown and filtering to improve performance

For more information, see:
- [Session-Scoped UDTF Registration](../session_scoped/index.md)
- Technical Plan: CDF Databricks Integration (UDTF-Based)

