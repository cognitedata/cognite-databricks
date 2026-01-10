"""Advanced unit tests for UDTFGenerator methods to increase coverage."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionInfoRoutineBody,
    FunctionParameterInfo,
    FunctionParameterMode,
    FunctionParameterType,
)
from pyspark.sql.types import (  # type: ignore[import-not-found]
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cognite.databricks.generator import UDTFGenerator
from cognite.databricks.models import RegisteredUDTFResult


class TestUDTFGeneratorAdvanced:
    """Advanced tests for UDTFGenerator methods."""

    def test_get_view_by_id(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _get_view_by_id finds view by external_id."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        view = generator._get_view_by_id("SmallBoat")
        assert view is not None
        assert view.external_id == "SmallBoat"

        # Test non-existent view
        view = generator._get_view_by_id("NonExistent")
        assert view is None

    def test_get_property_type(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _get_property_type correctly identifies property types."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        view = generator._get_view_by_id("SmallBoat")
        assert view is not None

        # Test MappedProperty
        for prop_name, prop in view.properties.items():
            if isinstance(prop, dm.MappedProperty):
                property_type, is_relationship, is_multi = generator._get_property_type(prop)
                assert property_type is not None
                assert isinstance(is_relationship, bool)
                assert isinstance(is_multi, bool)

    def test_build_output_schema(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _build_output_schema builds correct StructType."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        schema = generator._build_output_schema("SmallBoat")
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

        # Test non-existent view
        with pytest.raises(ValueError, match="View 'NonExistent' not found"):
            generator._build_output_schema("NonExistent")

    def test_parse_return_type(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _parse_return_type generates correct SQL DDL."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        return_type = generator._parse_return_type("SmallBoat")
        assert return_type.startswith("TABLE(")
        assert "external_id" in return_type or "name" in return_type

    def test_parse_return_params(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _parse_return_params generates correct return parameters."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        return_params = generator._parse_return_params("SmallBoat", debug=False)
        assert len(return_params) > 0
        assert all(isinstance(p, FunctionParameterInfo) for p in return_params)

    def test_parse_udtf_params_from_class(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _parse_udtf_params_from_class parses parameters from UDTF class."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Create a mock UDTF class with analyze method
        class MockUDTF:
            @staticmethod
            def analyze(instance_id: str, start: str | None = None, end: str | None = None) -> object:
                from pyspark.sql.types import StructType, StructField, StringType

                return StructType([StructField("result", StringType())])

        params = generator._parse_udtf_params_from_class(MockUDTF, debug=False)
        assert len(params) >= 5  # At least 5 secret params
        assert any(p.name == "instance_id" for p in params)

        # Test missing analyze method
        class NoAnalyze:
            pass

        with pytest.raises(ValueError, match="must have an analyze\\(\\) method"):
            generator._parse_udtf_params_from_class(NoAnalyze)

    def test_parse_return_type_from_class(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _parse_return_type_from_class parses return type from UDTF class."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Create a mock UDTF class with outputSchema method
        class MockUDTF:
            @staticmethod
            def outputSchema() -> StructType:
                return StructType([StructField("timestamp", TimestampType()), StructField("value", DoubleType())])

        return_type = generator._parse_return_type_from_class(MockUDTF)
        assert return_type.startswith("TABLE(")
        assert "timestamp" in return_type

        # Test missing outputSchema method
        class NoOutputSchema:
            pass

        with pytest.raises(ValueError, match="must have an outputSchema\\(\\) method"):
            generator._parse_return_type_from_class(NoOutputSchema)

    def test_parse_return_params_from_class(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test _parse_return_params_from_class parses return params from UDTF class."""
        from cognite.pygen_spark import SparkUDTFGenerator

        code_generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )

        generator = UDTFGenerator(
            workspace_client=mock_workspace_client,
            cognite_client=mock_cognite_client,
            catalog="test_catalog",
            schema="test_schema",
            code_generator=code_generator,
        )

        # Create a mock UDTF class with outputSchema method
        class MockUDTF:
            @staticmethod