"""Root test configuration and shared fixtures for cognite-databricks tests."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient, data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client
from databricks.sdk import WorkspaceClient


@pytest.fixture()
def mock_cognite_client() -> Iterable[CogniteClient]:
    """Mock CogniteClient for testing."""
    with monkeypatch_cognite_client() as m:
        yield m


@pytest.fixture
def mock_workspace_client() -> MagicMock:
    """Mock WorkspaceClient for testing."""
    mock = MagicMock(spec=WorkspaceClient)
    # Setup default mock responses
    mock.warehouses.list.return_value = [MagicMock(id="test-warehouse-id", name="test-warehouse")]
    return mock


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for test output."""
    output_dir = tmp_path / "udtf_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def sample_view() -> dm.View:
    """Sample view for testing."""
    return dm.View(
        space="test_space",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "name": dm.Text(),  # type: ignore[dict-item]
            "description": dm.Text(),  # type: ignore[dict-item]
            "boat_guid": dm.Int64(),  # type: ignore[dict-item]
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


@pytest.fixture
def sample_data_model(sample_view: dm.View) -> dm.DataModel[dm.View]:
    """Sample data model for testing."""
    return dm.DataModel(
        space="test_space",
        external_id="test_model",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[sample_view],
    )
