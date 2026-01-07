"""Unit tests for UDTFGenerator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite.databricks.generator import UDTFGenerator


class TestUDTFGenerator:
    """Tests for UDTFGenerator class."""

    def test_init(
        self,
        mock_cognite_client,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
    ) -> None:
        """Test generator initialization."""
        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")
        
        generator = UDTFGenerator(
            data_model_id=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )
        
        assert generator.output_dir == temp_output_dir
        assert generator.catalog == "test_catalog"
        assert generator.schema == "test_schema"

    def test_generate_udtfs(
        self,
        mock_cognite_client,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test UDTF generation."""
        # Mock data model retrieval
        mock_cognite_client.data_modeling.data_models.retrieve.return_value = sample_data_model
        
        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")
        
        generator = UDTFGenerator(
            data_model_id=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )
        
        result = generator.generate_udtfs()
        assert result is not None
        assert result.total_count > 0

