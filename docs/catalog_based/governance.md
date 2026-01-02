# Governance

## Unity Catalog Permissions

Unity Catalog provides fine-grained access control through permissions:

```sql
-- Grant SELECT permission on a View
GRANT SELECT ON VIEW main.sailboat_sailboat_1.smallboat TO `analysts@company.com`;

-- Grant SELECT permission on a schema (all Views in schema)
GRANT SELECT ON SCHEMA main.sailboat_sailboat_1 TO `analysts@company.com`;

-- Grant SELECT permission on a catalog (all schemas and Views)
GRANT SELECT ON CATALOG main TO `analysts@company.com`;

-- Revoke permissions
REVOKE SELECT ON VIEW main.sailboat_sailboat_1.smallboat FROM `analysts@company.com`;
```

## GRANT/REVOKE Examples

### Grant Access to a Specific View

```sql
-- Grant SELECT permission to a user
GRANT SELECT ON VIEW main.sailboat_sailboat_1.smallboat TO `user@example.com`;

-- Grant SELECT permission to a group
GRANT SELECT ON VIEW main.sailboat_sailboat_1.smallboat TO `analysts@company.com`;

-- Grant SELECT permission to a service principal
GRANT SELECT ON VIEW main.sailboat_sailboat_1.smallboat TO `service-principal@company.com`;
```

### Grant Access to a Schema

```sql
-- Grant SELECT permission on all Views in a schema
GRANT SELECT ON SCHEMA main.sailboat_sailboat_1 TO `analysts@company.com`;

-- Grant USAGE permission (required to access objects in schema)
GRANT USAGE ON SCHEMA main.sailboat_sailboat_1 TO `analysts@company.com`;
```

### Grant Access to a Catalog

```sql
-- Grant SELECT permission on all objects in a catalog
GRANT SELECT ON CATALOG main TO `analysts@company.com`;

-- Grant USAGE permission (required to access catalog)
GRANT USAGE ON CATALOG main TO `analysts@company.com`;
```

### Revoke Permissions

```sql
-- Revoke SELECT permission from a user
REVOKE SELECT ON VIEW main.sailboat_sailboat_1.smallboat FROM `user@example.com`;

-- Revoke SELECT permission from a group
REVOKE SELECT ON SCHEMA main.sailboat_sailboat_1 FROM `analysts@company.com`;

-- Revoke SELECT permission from a catalog
REVOKE SELECT ON CATALOG main FROM `analysts@company.com`;
```

## Next Steps

- Learn about [Querying](./querying.md) Views
- See [Troubleshooting](./troubleshooting.md) for common issues


