"""Unit tests for SecretManagerHelper."""

from __future__ import annotations

from unittest.mock import MagicMock

from cognite.databricks.secret_manager import SecretManagerHelper


class TestSecretManagerHelper:
    """Tests for SecretManagerHelper class."""

    def test_init(self, mock_workspace_client: MagicMock) -> None:
        """Test helper initialization."""
        helper = SecretManagerHelper(workspace_client=mock_workspace_client)
        assert helper.workspace_client == mock_workspace_client

    def test_store_secrets(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test storing secrets."""
        from databricks.sdk.service.workspace import SecretScope

        helper = SecretManagerHelper(workspace_client=mock_workspace_client)

        # Mock list_scopes to return empty initially, then return created scope
        created_scope = SecretScope(name="test_scope")
        mock_workspace_client.secrets.list_scopes.side_effect = [
            [],  # First call: scope doesn't exist
            [created_scope],  # Second call: scope exists after creation
        ]

        secrets = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "tenant_id": "test_tenant_id",
            "cdf_cluster": "test_cluster",
            "project": "test_project",
        }

        helper.store_secrets(
            secret_scope="test_scope",
            secrets=secrets,
        )

        # Verify secrets were stored
        assert mock_workspace_client.secrets.put_secret.called

    def test_get_secret(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test getting a secret."""
        from databricks.sdk.service.workspace import GetSecretResponse

        helper = SecretManagerHelper(workspace_client=mock_workspace_client)
        # Mock to return GetSecretResponse object with value attribute
        secret_response = GetSecretResponse(key="test_key", value="secret_value")
        mock_workspace_client.secrets.get_secret.return_value = secret_response

        value = helper.get_secret("test_scope", "test_key")
        assert value == "secret_value"

    def test_set_cdf_credentials(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test set_cdf_credentials stores all CDF credentials."""
        from databricks.sdk.service.workspace import SecretScope

        helper = SecretManagerHelper(workspace_client=mock_workspace_client)

        # Mock create_scope_if_not_exists
        mock_scope = SecretScope(name="test_scope")
        mock_workspace_client.secrets.list_scopes.side_effect = [
            [],  # First call: scope doesn't exist
            [mock_scope],  # Second call: scope exists after creation
        ]
        mock_workspace_client.secrets.create_scope.return_value = None

        # Call set_cdf_credentials
        helper.set_cdf_credentials(
            scope_name="test_scope",
            project="test_project",
            cdf_cluster="test_cluster",
            client_id="test_client_id",
            client_secret="test_client_secret",
            tenant_id="test_tenant_id",
        )

        # Verify put_secret was called 5 times (once for each credential)
        assert mock_workspace_client.secrets.put_secret.call_count == 5

        # Verify all credentials were stored
        calls = mock_workspace_client.secrets.put_secret.call_args_list
        stored_keys = {call[0][1] for call in calls}  # Extract key from positional args
        assert stored_keys == {"project", "cdf_cluster", "client_id", "client_secret", "tenant_id"}
