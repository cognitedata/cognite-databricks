"""Unit tests for UDTFRegistry."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import FunctionInfo

from cognite.databricks.udtf_registry import UDTFRegistry


class TestUDTFRegistry:
    """Tests for UDTFRegistry class."""

    def test_init(self, mock_workspace_client: MagicMock) -> None:
        """Test registry initialization."""
        registry = UDTFRegistry(workspace_client=mock_workspace_client)
        assert registry.workspace_client == mock_workspace_client

    def test_get_default_warehouse_id(self, udtf_registry: UDTFRegistry) -> None:
        """Test getting default warehouse ID."""
        warehouse_id = udtf_registry._get_default_warehouse_id()
        assert warehouse_id == "test-warehouse-id"

    def test_get_default_warehouse_id_no_warehouses(self, mock_workspace_client: MagicMock) -> None:
        """Test getting default warehouse ID when none exist."""
        mock_workspace_client.warehouses.list.return_value = []
        registry = UDTFRegistry(workspace_client=mock_workspace_client)
        with pytest.raises(ValueError, match="No SQL warehouses found"):
            registry._get_default_warehouse_id()

    def test_register_udtf_skip_if_exists(
        self,
        udtf_registry: UDTFRegistry,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test registering UDTF with skip if exists."""
        # Mock existing function
        existing_function = FunctionInfo(
            name="test_udtf",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        mock_workspace_client.functions.get.return_value = existing_function

        result = udtf_registry.register_udtf(
            catalog="test_catalog",
            schema="test_schema",
            function_name="test_udtf",
            udtf_code="class TestUDTF: pass",
            input_params=[],
            return_type="TABLE(id INT)",
            return_params=[],
            if_exists="skip",
        )

        assert result == existing_function
        # Should not call create
        mock_workspace_client.functions.create.assert_not_called()

    def test_register_udtf_replace(
        self,
        udtf_registry: UDTFRegistry,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test registering UDTF with replace if exists."""
        # Mock existing function
        existing_function = FunctionInfo(
            name="test_udtf",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        mock_workspace_client.functions.get.return_value = existing_function

        new_function = FunctionInfo(
            name="test_udtf",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        mock_workspace_client.functions.create.return_value = new_function

        result = udtf_registry.register_udtf(
            catalog="test_catalog",
            schema="test_schema",
            function_name="test_udtf",
            udtf_code="class TestUDTF: pass",
            input_params=[],
            return_type="TABLE(id INT)",
            return_params=[],
            if_exists="replace",
        )

        # Should delete and create
        mock_workspace_client.functions.delete.assert_called_once()
        mock_workspace_client.functions.create.assert_called_once()
        assert result == new_function

    def test_register_udtf_new_function(
        self,
        udtf_registry: UDTFRegistry,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test registering new UDTF."""
        # Mock no existing function
        from databricks.sdk.errors import NotFound

        mock_workspace_client.functions.get.side_effect = NotFound("Function not found")

        new_function = FunctionInfo(
            name="test_udtf",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        mock_workspace_client.functions.create.return_value = new_function

        result = udtf_registry.register_udtf(
            catalog="test_catalog",
            schema="test_schema",
            function_name="test_udtf",
            udtf_code="class TestUDTF: pass",
            input_params=[],
            return_type="TABLE(id INT)",
            return_params=[],
        )

        mock_workspace_client.functions.create.assert_called_once()
        assert result == new_function
