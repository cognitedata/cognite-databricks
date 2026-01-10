"""Unit tests for generator module functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.databricks.generator import (
    generate_session_scoped_notebook_code,
    generate_time_series_udtf_view_sql,
    generate_udtf_sql_query,
    register_udtf_from_file,
)


class TestRegisterUdtfFromFile:
    """Tests for register_udtf_from_file function."""

    def test_register_udtf_from_file_with_explicit_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test registering UDTF from file with explicit function name."""
        # Create a mock UDTF file
        udtf_file = tmp_path / "TestUDTF.py"
        udtf_code = '''
class TestUDTF:
    def analyze(self, param: str) -> object:
        from pyspark.sql.types import StructType, StructField, StringType
        return StructType([StructField("result", StringType())])
    
    def eval(self, param: str) -> object:
        yield ("test",)
'''
        udtf_file.write_text(udtf_code)

        # Mock SparkSession
        mock_spark_session = MagicMock()
        mock_udtf_register = MagicMock()
        mock_spark_session.udtf.register = mock_udtf_register

        result = register_udtf_from_file(
            str(udtf_file),
            function_name="test_udtf",
            spark_session=mock_spark_session,
        )

        assert result == "test_udtf"
        mock_udtf_register.assert_called_once()

    def test_register_udtf_from_file_auto_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test registering UDTF from file with auto-generated function name."""
        # Create a mock UDTF file
        udtf_file = tmp_path / "SmallBoatUDTF.py"
        udtf_code = '''
class SmallBoatUDTF:
    def analyze(self, param: str) -> object:
        from pyspark.sql.types import StructType, StructField, StringType
        return StructType([StructField("result", StringType())])
    
    def eval(self, param: str) -> object:
        yield ("test",)
'''
        udtf_file.write_text(udtf_code)

        # Mock SparkSession
        mock_spark_session = MagicMock()
        mock_udtf_register = MagicMock()
        mock_spark_session.udtf.register = mock_udtf_register

        result = register_udtf_from_file(
            str(udtf_file),
            function_name=None,
            spark_session=mock_spark_session,
        )

        # Should convert SmallBoatUDTF -> small_boat_udtf
        assert result == "small_boat_udtf"
        mock_udtf_register.assert_called_once()

    def test_register_udtf_from_file_not_found(self) -> None:
        """Test register_udtf_from_file raises FileNotFoundError for missing file."""
        mock_spark_session = MagicMock()

        with pytest.raises(FileNotFoundError, match="UDTF file not found"):
            register_udtf_from_file("/nonexistent/file.py", spark_session=mock_spark_session)

    def test_register_udtf_from_file_no_udtf_class(
        self,
        tmp_path: Path,
    ) -> None:
        """Test register_udtf_from_file raises ValueError when no UDTF class found."""
        udtf_file = tmp_path / "NoUDTF.py"
        udtf_file.write_text("# Just a comment, no UDTF class")

        mock_spark_session = MagicMock()

        with pytest.raises(ValueError, match="No UDTF class found"):
            register_udtf_from_file(str(udtf_file), spark_session=mock_spark_session)

    def test_register_udtf_from_file_multiple_classes(
        self,
        tmp_path: Path,
    ) -> None:
        """Test register_udtf_from_file raises ValueError when multiple UDTF classes found."""
        udtf_file = tmp_path / "MultipleUDTF.py"
        udtf_code = '''
class UDTF1:
    def analyze(self): pass
    def eval(self): pass

class UDTF2:
    def analyze(self): pass
    def eval(self): pass
'''
        udtf_file.write_text(udtf_code)

        mock_spark_session = MagicMock()

        with pytest.raises(ValueError, match="Multiple UDTF classes found"):
            register_udtf_from_file(str(udtf_file), spark_session=mock_spark_session)

    def test_register_udtf_from_file_no_spark_session(self) -> None:
        """Test register_udtf_from_file raises RuntimeError when no SparkSession."""
        with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None):
            with pytest.raises(RuntimeError, match="No active SparkSession found"):
                register_udtf_from_file("/path/to/file.py", spark_session=None)


class TestGenerateTimeSeriesUdtfViewSql:
    """Tests for generate_time_series_udtf_view_sql function."""

    def test_generate_time_series_udtf_view_sql_basic(self) -> None:
        """Test generating time series UDTF view SQL with basic parameters."""
        sql = generate_time_series_udtf_view_sql(
            udtf_name="time_series_datapoints_udtf",
            secret_scope="cdf_sailboat_sailboat",
            view_name="time_series_datapoints",
            catalog="main",
            schema="cdf_models",
        )

        assert "CREATE OR REPLACE VIEW" in sql
        assert "main.cdf_models.time_series_datapoints" in sql
        assert "time_series_datapoints_udtf" in sql
        assert "SECRET('cdf_sailboat_sailboat', 'client_id')" in sql
        assert "SECRET('cdf_sailboat_sailboat', 'project')" in sql

    def test_generate_time_series_udtf_view_sql_with_udtf_params(self) -> None:
        """Test generating time series UDTF view SQL with UDTF-specific parameters."""
        sql = generate_time_series_udtf_view_sql(
            udtf_name="time_series_datapoints_udtf",
            secret_scope="cdf_sailboat_sailboat",
            view_name="time_series_datapoints",
            catalog="main",
            schema="cdf_models",
            udtf_params=["instance_id", "start", "end"],
        )

        assert "instance_id => NULL" in sql
        assert "start => NULL" in sql
        assert "end => NULL" in sql

    def test_generate_time_series_udtf_view_sql_with_placeholders(self) -> None:
        """Test generating time series UDTF view SQL with catalog/schema placeholders."""
        sql = generate_time_series_udtf_view_sql(
            udtf_name="time_series_datapoints_udtf",
            secret_scope="cdf_sailboat_sailboat",
            view_name="time_series_datapoints",
            catalog=None,
            schema=None,
        )

        assert "{{ catalog }}.{{ schema }}." in sql

    def test_generate_time_series_udtf_view_sql_unknown_udtf(self) -> None:
        """Test generating time series UDTF view SQL with unknown UDTF raises ValueError."""
        with pytest.raises(ValueError, match="Unknown time series UDTF"):
            generate_time_series_udtf_view_sql(
                udtf_name="unknown_udtf",
                secret_scope="cdf_sailboat_sailboat",
            )


class TestGenerateUdtfSqlQuery:
    """Tests for generate_udtf_sql_query function."""

    def test_generate_udtf_sql_query_named_parameters(self) -> None:
        """Test generating SQL query with named parameters."""
        sql = generate_udtf_sql_query(
            catalog="f0connectortest",
            schema="sailboat_sailboat_v1",
            function_name="small_boat_udtf",
            secret_scope="cdf_sailboat_sailboat",
            use_named_parameters=True,
            limit=10,
        )

        assert "SELECT * FROM" in sql
        assert "f0connectortest.sailboat_sailboat_v1.small_boat_udtf" in sql
        assert "client_id     => SECRET" in sql
        assert "LIMIT 10" in sql
        assert "NULL" not in sql  # Named parameters don't include NULLs

    def test_generate_udtf_sql_query_positional_parameters(self) -> None:
        """Test generating SQL query with positional parameters."""
        sql = generate_udtf_sql_query(
            catalog="f0connectortest",
            schema="sailboat_sailboat_v1",
            function_name="small_boat_udtf",
            secret_scope="cdf_sailboat_sailboat",
            use_named_parameters=False,
            view_properties=["name", "description"],
            limit=10,
        )

        assert "SELECT * FROM" in sql
        assert "SECRET('cdf_sailboat_sailboat', 'client_id')" in sql
        assert "NULL, -- name" in sql
        assert "NULL -- description" in sql
        assert "LIMIT 10" in sql

    def test_generate_udtf_sql_query_positional_no_properties(self) -> None:
        """Test generating SQL query with positional parameters but no view properties."""
        sql = generate_udtf_sql_query(
            catalog="f0connectortest",
            schema="sailboat_sailboat_v1",
            function_name="small_boat_udtf",
            secret_scope="cdf_sailboat_sailboat",
            use_named_parameters=False,
            view_properties=None,
            limit=5,
        )

        assert "NULL, -- Add property parameters here" in sql
        assert "LIMIT 5" in sql


class TestGenerateSessionScopedNotebookCode:
    """Tests for generate_session_scoped_notebook_code function."""

    def test_generate_session_scoped_notebook_code_basic(
        self,
        mock_cognite_client: CogniteClient,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test generating session-scoped notebook code."""
        from cognite.client.data_classes.data_modeling.data_models import DataModelList
        from cognite.client.data_classes.data_modeling.ids import DataModelId
        from cognite.databricks.generator import generate_udtf_notebook

        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")
        mock_cognite_client.data_modeling.data_models.retrieve.return_value = DataModelList([sample_data_model])  # type: ignore[attr-defined]

        generator = generate_udtf_notebook(
            data_model=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )

        code_snippets = generate_session_scoped_notebook_code(
            generator,
            secret_scope="cdf_test_space_test_model",
        )

        assert "cell1_dependencies" in code_snippets
        assert "cell2_registration" in code_snippets
        assert "cell3_sql_example" in code_snippets
        assert "all_cells" in code_snippets

        # Check cell 1 has pip install
        assert "%pip install cognite-sdk" in code_snippets["cell1_dependencies"]

        # Check cell 2 has registration code
        assert "register_session_scoped_udtfs" in code_snippets["cell2_registration"]

        # Check cell 3 has SQL example
        assert "SELECT * FROM" in code_snippets["cell3_sql_example"]
        assert "SECRET('cdf_test_space_test_model', 'client_id')" in code_snippets["cell3_sql_example"]

    def test_generate_session_scoped_notebook_code_with_view_id(
        self,
        mock_cognite_client: CogniteClient,
        mock_workspace_client: MagicMock,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test generating session-scoped notebook code with specific view_id."""
        from cognite.client.data_classes.data_modeling.data_models import DataModelList
        from cognite.client.data_classes.data_modeling.ids import DataModelId
        from cognite.databricks.generator import generate_udtf_notebook

        data_model_id = DataModelId(space="test_space", external_id="test_model", version="v1")
        mock_cognite_client.data_modeling.data_models.retrieve.return_value = DataModelList([sample_data_model])  # type: ignore[attr-defined]

        generator = generate_udtf_notebook(
            data_model=data_model_id,
            client=mock_cognite_client,
            workspace_client=mock_workspace_client,
            output_dir=temp_output_dir,
            catalog="test_catalog",
            schema="test_schema",
        )

        code_snippets = generate_session_scoped_notebook_code(
            generator,
            secret_scope="cdf_test_space_test_model",
            view_id="SmallBoat",
        )

        # Should include SmallBoat in SQL example
        assert "small_boat" in code_snippets["cell3_sql_example"].lower()
