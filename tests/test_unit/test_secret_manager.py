"""Unit tests for SecretManagerHelper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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
        helper = SecretManagerHelper(workspace_client=mock_workspace_client)
        
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
        helper = SecretManagerHelper(workspace_client=mock_workspace_client)
        mock_workspace_client.secrets.get_secret.return_value = "secret_value"
        
        value = helper.get_secret("test_scope", "test_key")
        assert value == "secret_value"

