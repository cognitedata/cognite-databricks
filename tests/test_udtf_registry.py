"""Tests for UDTF registry functionality."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import FunctionInfo

from cognite.databricks.udtf_registry import UDTFRegistry


def test_register_udtf(
    udtf_registry: UDTFRegistry,
    mock_workspace_client: MagicMock,
) -> None:
    """Test UDTF registration."""
    # Mock function not existing
    mock_workspace_client.functions.get.side_effect = NotFound("Function not found")
    
    # Mock successful creation
    created_function = FunctionInfo(
        name="test_udtf",
        catalog_name="test_catalog",
        schema_name="test_schema",
    )
    mock_workspace_client.functions.create.return_value = created_function
    
    result = udtf_registry.register_udtf(
        catalog="test_catalog",
        schema="test_schema",
        function_name="test_udtf",
        udtf_code="class TestUDTF: pass",
        input_params=[],
        return_type="TABLE(id INT)",
        return_params=[],
    )
    
    assert result == created_function
    mock_workspace_client.functions.create.assert_called_once()


def test_secret_manager(
    mock_workspace_client: MagicMock,
) -> None:
    """Test Secret Manager integration."""
    from cognite.databricks.secret_manager import SecretManagerHelper

    helper = SecretManagerHelper(workspace_client=mock_workspace_client)
    
    # Mock scope creation
    mock_workspace_client.secrets.list_scopes.return_value = []
    
    # Test scope creation
    scope = helper.create_scope_if_not_exists("test_scope")
    assert scope is not None
    
    # Test storing secrets
    helper.set_cdf_credentials(
        scope_name="test_scope",
        project="test_project",
        cdf_cluster="test_cluster",
        client_id="test_client_id",
        client_secret="test_client_secret",
        tenant_id="test_tenant_id",
    )
    
    # Verify secrets were stored
    assert mock_workspace_client.secrets.put_secret.called

