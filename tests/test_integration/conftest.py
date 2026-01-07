"""Integration test fixtures for cognite-databricks."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client
from databricks.sdk import WorkspaceClient

from cognite.databricks.generator import UDTFGenerator


@pytest.fixture()
def mock_cognite_client() -> Iterable[CogniteClient]:
    """Mock CogniteClient for integration testing."""
    with monkeypatch_cognite_client() as m:
        yield m


@pytest.fixture
def mock_workspace_client() -> MagicMock:
    """Mock WorkspaceClient for integration testing."""
    mock = MagicMock(spec=WorkspaceClient)
    # Setup default mock responses for workspace operations
    mock.warehouses.list.return_value = [MagicMock(id="test-warehouse-id", name="test-warehouse")]
    return mock


@pytest.fixture
def sample_sailboat_view() -> dm.View:
    """Sample sailboat view matching notebook data."""
    return dm.View(
        space="sailboat",
        external_id="SmallBoat",
        version="v1",
        properties={
            "name": dm.Text(),
            "description": dm.Text(),
            "boat_guid": dm.Int64(),
            "mmsi_country": dm.Text(),
        },
    )


@pytest.fixture
def sample_nmea_time_series_view() -> dm.View:
    """Sample NMEA time series view matching notebook data."""
    return dm.View(
        space="sailboat",
        external_id="NmeaTimeSeries",
        version="v1",
        properties={
            "mmsi": dm.Text(),
            "value": dm.Float64(),
        },
    )


@pytest.fixture
def sailboat_data_model(
    sample_sailboat_view: dm.View,
    sample_nmea_time_series_view: dm.View,
) -> dm.DataModel[dm.View]:
    """Data model matching notebook structure."""
    return dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        views=[sample_sailboat_view, sample_nmea_time_series_view],
    )


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for integration test output."""
    output_dir = tmp_path / "udtf_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def udtf_generator(
    mock_cognite_client: CogniteClient,
    mock_workspace_client: MagicMock,
    temp_output_dir: Path,
    sailboat_data_model: dm.DataModel[dm.View],
) -> UDTFGenerator:
    """UDTFGenerator instance for integration testing."""
    from cognite.client.data_classes.data_modeling.ids import DataModelId

    data_model_id = DataModelId(
        space="sailboat",
        external_id="sailboat",
        version="v1",
    )

    return UDTFGenerator(
        data_model_id=data_model_id,
        client=mock_cognite_client,
        workspace_client=mock_workspace_client,
        output_dir=temp_output_dir,
        catalog="test_catalog",
        schema="test_schema",
    )

