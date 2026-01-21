"""Unit tests for DBR version detection in UDTFGenerator."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.databricks.generator import UDTFGenerator


class TestDBRVersionDetection:
    """Tests for DBR version detection methods."""

    def test_get_dbr_version_from_spark_conf(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test DBR version detection from Spark conf (Method 1)."""
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

        # Mock Spark session with conf.get returning DBR version
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "18.1.5-scala2.12"
        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        # Generate UDTFs first
        code_generator.generate_udtfs()

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            # This will call _get_dbr_version internally
            # We can't directly test the private method, so we test via register_udtfs_and_views
            # which will call it and check the version
            result = generator.register_udtfs_and_views(
                data_model=sample_data_model,
                secret_scope="test_scope",
                debug=True,
            )
            # Should succeed without version error (18.1 >= 18.1)
            assert result is not None

    def test_get_dbr_version_from_sql_query(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test DBR version detection from SQL query (Method 2)."""
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

        # Mock Spark session where conf.get fails but SQL query succeeds
        mock_spark = MagicMock()
        # First method (conf.get) raises exception
        mock_spark.conf.get.side_effect = Exception("Config not available")
        # Second method (SQL query) succeeds
        mock_row = MagicMock()
        mock_row.__getitem__.return_value = "18.1.5-scala2.12"
        mock_result = MagicMock()
        mock_result.collect.return_value = [mock_row]
        mock_spark.sql.return_value = mock_result

        # Generate UDTFs first
        code_generator.generate_udtfs()

        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            result = generator.register_udtfs_and_views(
                data_model=sample_data_model,
                secret_scope="test_scope",
                debug=True,
            )
            # Should succeed without version error
            assert result is not None
            # Verify SQL query was called
            mock_spark.sql.assert_called_with("SELECT current_version().dbr_version AS dbr_version")

    def test_get_dbr_version_from_env_var(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test DBR version detection from environment variable (Method 3)."""
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

        # Mock Spark session where both conf.get and SQL query fail
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = Exception("Config not available")
        mock_spark.sql.side_effect = Exception("SQL query failed")

        # Generate UDTFs first
        code_generator.generate_udtfs()

        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "18.1.5"}):
                result = generator.register_udtfs_and_views(
                    data_model=sample_data_model,
                    secret_scope="test_scope",
                    debug=True,
                )
                # Should succeed without version error
                assert result is not None

    def test_get_dbr_version_all_methods_fail(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test DBR version detection when all methods fail (local development)."""
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

        # Mock Spark session where all methods fail
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = Exception("Config not available")
        mock_spark.sql.side_effect = Exception("SQL query failed")

        # Generate UDTFs first
        code_generator.generate_udtfs()

        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            with patch.dict(os.environ, {}, clear=True):  # Clear DATABRICKS_RUNTIME_VERSION
                # Should not raise error, but assume DBR 18.1+ for local development
                result = generator.register_udtfs_and_views(
                    data_model=sample_data_model,
                    secret_scope="test_scope",
                    debug=True,
                )
                # Should succeed (assumes DBR 18.1+)
                assert result is not None

    def test_dbr_version_too_old_raises_error(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test that pre-DBR 18.1 versions raise ValueError."""
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

        # Generate UDTFs first
        code_generator.generate_udtfs()

        # Mock Spark session with DBR 17.3 (too old)
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "17.3.5-scala2.12"
        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            with pytest.raises(ValueError, match=r"requires Databricks Runtime 18.1 or later"):
                generator.register_udtfs_and_views(
                    data_model=sample_data_model,
                    secret_scope="test_scope",
                    debug=True,
                )

    def test_dbr_version_non_numeric_skipped(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test that non-DBR version strings (e.g., 'client.4.8') are skipped."""
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

        # Generate UDTFs first
        code_generator.generate_udtfs()

        # Mock Spark session with non-DBR version string
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "client.4.8"
        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            # Should not raise error, just skip version check
            result = generator.register_udtfs_and_views(
                data_model=sample_data_model,
                secret_scope="test_scope",
                debug=True,
            )
            # Should succeed (version check skipped)
            assert result is not None

    def test_sql_query_current_version_dbr_version(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test that SQL query SELECT current_version().dbr_version works correctly."""
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

        # Mock Spark session where conf.get fails but SQL query succeeds
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = Exception("Config not available")

        # Mock SQL result with DBR version
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: "18.1.5-scala2.12" if key == "dbr_version" else None
        mock_result = MagicMock()
        mock_result.collect.return_value = [mock_row]
        mock_spark.sql.return_value = mock_result

        # Generate UDTFs first
        code_generator.generate_udtfs()

        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            result = generator.register_udtfs_and_views(
                data_model=sample_data_model,
                secret_scope="test_scope",
                debug=True,
            )
            # Verify SQL query was called with correct statement
            mock_spark.sql.assert_called_with("SELECT current_version().dbr_version AS dbr_version")
            # Should succeed
            assert result is not None

    def test_dbr_version_with_x_suffix_normalized(
        self,
        mock_workspace_client: MagicMock,
        mock_cognite_client: CogniteClient,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test that DBR versions with '.x' suffix (e.g., '17.3.x') are normalized and correctly detected."""
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

        # Generate UDTFs first
        code_generator.generate_udtfs()

        # Mock Spark session with DBR version "17.3.x" (should be normalized to "17.3" and raise error)
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "17.3.x"
        mock_spark_session = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        with patch("pyspark.sql.SparkSession", mock_spark_session):
            with pytest.raises(ValueError, match=r"requires Databricks Runtime 18.1 or later"):
                generator.register_udtfs_and_views(
                    data_model=sample_data_model,
                    secret_scope="test_scope",
                    debug=True,
                )
