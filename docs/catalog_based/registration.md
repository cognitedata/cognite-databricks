# Registration (Unity Catalog, Scalar Only)

This project now uses Unity Catalog SQL registration for **serverless-compatible** UDTFs. The UDTFs run in **scalar mode** (row-by-row) to ensure compatibility with SQL Warehouses.

## Register UDTFs (Unity Catalog)

Use catalog-based registration to create persistent UDTFs:

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

# Generate UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    catalog="main",
    schema="cdf_models",
    output_dir="/Workspace/Users/user@example.com/udtf",
)

# Register UDTFs in Unity Catalog (SQL registration)
udtf_result = generator.register_udtfs(
    secret_scope="cdf_sailboat_sailboat",
    if_exists="replace",
)

print(f"âœ“ Registered {udtf_result.total_count} UDTF(s)")
```

## SQL Usage (Serverless Compatible)

After registration, UDTFs are available in SQL Warehouses:

```sql
SELECT * FROM main.cdf_models.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
```

## Notes

- Registration uses SQL `CREATE FUNCTION` under the hood (Unity Catalog)
- Execution is **scalar-only** (row-by-row), which is required for serverless SQL compatibility
- Functions are persistent in Unity Catalog and visible to SQL Warehouses and BI tools
