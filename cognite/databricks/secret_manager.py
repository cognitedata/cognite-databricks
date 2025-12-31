"""SecretManagerHelper - Helper for managing Databricks Secret Manager scopes and secrets."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import SecretScope


class SecretManagerHelper:
    """Helper for managing Databricks Secret Manager scopes and secrets."""

    def __init__(self, workspace_client: WorkspaceClient) -> None:
        """Initialize Secret Manager helper.

        Args:
            workspace_client: Databricks WorkspaceClient instance
        """
        self.workspace_client = workspace_client

    def create_scope_if_not_exists(self, scope_name: str) -> SecretScope:
        """Create a Secret Manager scope if it doesn't exist.

        Args:
            scope_name: Name of the scope to create

        Returns:
            The created or existing SecretScope
        """
        # Check if scope exists by listing all scopes
        existing_scopes = list(self.workspace_client.secrets.list_scopes())
        for scope in existing_scopes:
            if scope.name == scope_name:
                return scope
        
        # Scope doesn't exist, create it (scope is positional parameter)
        self.workspace_client.secrets.create_scope(scope_name)
        
        # Return the newly created scope (need to fetch it)
        # Note: create_scope doesn't return the scope, so we list again
        scopes = list(self.workspace_client.secrets.list_scopes())
        for scope in scopes:
            if scope.name == scope_name:
                return scope
        raise RuntimeError(f"Failed to create scope: {scope_name}")

    def set_cdf_credentials(
        self,
        scope_name: str,
        project: str,
        cdf_cluster: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ) -> None:
        """Store CDF credentials in Secret Manager as plain text.

        These credentials typically come from the TOML file (same format as used by
        load_cognite_client_from_toml()). The values are stored in Secret Manager
        as plain text (Databricks encrypts them at rest) and referenced in View SQL
        via SECRET() function.

        Note: Secrets should be stored as plain text, not base64-encoded. The SDK
        expects plain text values (e.g., tenant_id as GUID, cdf_cluster as cluster name).

        Args:
            scope_name: Secret Manager scope name
            project: CDF project name (from TOML: [cognite].project) - plain text
            cdf_cluster: CDF cluster name (from TOML: [cognite].cdf_cluster) - plain text, e.g., "westeurope-1"
            client_id: OAuth2 client ID (from TOML: [cognite].client_id) - plain text
            client_secret: OAuth2 client secret (from TOML: [cognite].client_secret) - plain text
            tenant_id: Azure AD tenant ID (from TOML: [cognite].tenant_id) - plain text GUID, e.g., "dbf2ec1b-2fbc-4106-9371-017d78d6df71"
        """
        self.create_scope_if_not_exists(scope_name)

        secrets = {
            "project": project,
            "cdf_cluster": cdf_cluster,
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        }

        for key, value in secrets.items():
            # put_secret signature: put_secret(scope: str, key: str, *, string_value: str)
            # scope and key are positional parameters
            self.workspace_client.secrets.put_secret(
                scope_name,  # positional
                key,  # positional
                string_value=value,  # keyword
            )

