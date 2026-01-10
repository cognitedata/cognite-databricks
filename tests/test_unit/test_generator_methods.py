"""Unit tests for UDTFGenerator methods."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.databricks.generator import UDTFGenerator


class TestUDTFGeneratorMethods:
    """Tests for UDTFGenerator methods."""

    def test_ensure_catalog_exists_catalog_exists(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_catalog_exists when catalog already exists."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Mock catalog.get to succeed (catalog exists)
        mock_workspace_client.catalogs.get.return_value = MagicMock(name="test_catalog")

        # Should not raise
        generator._ensure_catalog_exists()

        mock_workspace_client.catalogs.get.assert_called_once_with("test_catalog")

    def test_ensure_catalog_exists_create_catalog(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_catalog_exists when catalog doesn't exist."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Mock catalog.get to fail (catalog doesn't exist)
        mock_workspace_client.catalogs.get.side_effect = Exception("Catalog not found")
        # Mock catalog.create to succeed
        mock_workspace_client.catalogs.create.return_value = None

        # Should not raise
        generator._ensure_catalog_exists()

        mock_workspace_client.catalogs.get.assert_called_once_with("test_catalog")
        mock_workspace_client.catalogs.create.assert_called_once()

    def test_ensure_schema_exists_schema_exists(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_schema_exists when schema already exists."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Mock catalog.get to succeed
        mock_workspace_client.catalogs.get.return_value = MagicMock()
        # Mock schema.get to succeed (schema exists)
        mock_workspace_client.schemas.get.return_value = MagicMock()

        # Should not raise
        generator._ensure_schema_exists()

        mock_workspace_client.schemas.get.assert_called_once_with("test_catalog.test_schema")

    def test_ensure_schema_exists_create_schema(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_schema_exists when schema doesn't exist."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Mock catalog.get to succeed
        mock_workspace_client.catalogs.get.return_value = MagicMock()
        # Mock schema.get to fail (schema doesn't exist)
        mock_workspace_client.schemas.get.side_effect = Exception("Schema not found")
        # Mock schema.create to succeed
        mock_workspace_client.schemas.create.return_value = None

        # Should not raise
        generator._ensure_schema_exists()

        mock_workspace_client.schemas.get.assert_called_once_with("test_catalog.test_schema")
        mock_workspace_client.schemas.create.assert_called_once()

    def test_find_generated_udtf_files(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _find_generated_udtf_files finds UDTF files."""
        from cognite.pygen_spark import SparkUDTFGenerator

        # Create a mock UDTF file
        udtf_dir = temp_output_dir / "test_space_test_model_v1" / "cognite_databricks"
        udtf_dir.mkdir(parents=True)
        udtf_file = udtf_dir / "SmallBoat_udtf.py"
        udtf_file.write_text("class SmallBoatUDTF: pass")

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Find files
        files = generator._find_generated_udtf_files()

        # Should find the UDTF file
        assert "SmallBoat" in files or len(files) > 0

    def test_ensure_catalog_exists_no_workspace_client(
        self,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_catalog_exists when workspace_client is None."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=None,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Should return early without raising
        generator._ensure_catalog_exists()

    def test_ensure_schema_exists_no_workspace_client(
        self,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _ensure_schema_exists when workspace_client is None."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
        )

        generator = UDTFGenerator(
            workspace_client=None,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Should return early without raising
        generator._ensure_schema_exists()
