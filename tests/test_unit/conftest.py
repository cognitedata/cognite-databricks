"""Unit test fixtures for cognite-databricks."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from databricks.sdk import WorkspaceClient

from cognite.databricks.udtf_registry import UDTFRegistry


@pytest.fixture
def sample_view() -> dm.View:
    """Sample view for unit testing."""
    return dm.View(
        space="test_space",
        external_id="SmallBoat",
        version="v1",
        properties={
            "name": dm.Text(),
            "description": dm.Text(),
            "boat_guid": dm.Int64(),
        },
    )


@pytest.fixture
def sample_data_model(sample_view: dm.View) -> dm.DataModel[dm.View]:
    """Sample data model for unit testing."""
    return dm.DataModel(
        space="test_space",
        external_id="test_model",
        version="v1",
        views=[sample_view],
    )


@pytest.fixture
def mock_workspace_client() -> MagicMock:
    """Mock WorkspaceClient for unit testing."""
    mock = MagicMock(spec=WorkspaceClient)
    mock.warehouses.list.return_value = [MagicMock(id="test-warehouse-id", name="test-warehouse")]
    return mock


@pytest.fixture
def udtf_registry(mock_workspace_client: MagicMock) -> UDTFRegistry:
    """UDTFRegistry instance for testing."""
    return UDTFRegistry(workspace_client=mock_workspace_client)

