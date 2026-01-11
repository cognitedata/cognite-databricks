"""Unit tests for UDTFGenerator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite.databricks.generator import generate_udtf_notebook


class TestUDTFGenerator:
    """Tests for UDTFGenerator class."""

    def test_init(
        self,
        mock_cognite_client: CogniteClient,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test generator initialization."""
        from cognite.client.data_classes.data_modeling.data_models import DataModelList

        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")
        # Return DataModelList (list-like object), not single DataModel
        mock_cognite_client.data_modeling.data_models.retrieve.return_value = DataModelList([sample_data_model])  # type: ignore[attr-defined]

        generator = generate_udtf_notebook(
            data_model=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )

        assert generator.catalog == "test_catalog"
        assert generator.schema == "test_schema"
        assert generator.code_generator is not None

    def test_generate_udtfs(
        self,
        mock_cognite_client: CogniteClient,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test UDTF generation."""
        from cognite.client.data_classes.data_modeling.data_models import DataModelList

        # Mock data model retrieval - return DataModelList
        mock_cognite_client.data_modeling.data_models.retrieve.return_value = DataModelList([sample_data_model])  # type: ignore[attr-defined]

        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")

        generator = generate_udtf_notebook(
            data_model=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )

        # Use code_generator to generate UDTFs
        result = generator.code_generator.generate_udtfs()
        assert result is not None
        assert result.total_count > 0
