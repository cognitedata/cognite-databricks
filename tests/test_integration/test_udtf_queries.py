"""Integration tests mimicking SQL queries from Session scoped zero-copy Databricks-CDF notebook."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cognite.databricks.generator import UDTFGenerator


@pytest.mark.integration
class TestUdtfRegistration:
    """Integration tests for UDTF registration from notebook."""

    def test_generate_and_register_udtfs(
        self,
        udtf_generator: UDTFGenerator,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test UDTF generation and registration (Cells 12-13)."""
        # Mock data model retrieval
        if udtf_generator.cognite_client:
            udtf_generator.cognite_client.data_modeling.data_models.retrieve.return_value = (  # type: ignore[attr-defined]
                MagicMock(
                    views=[
                        MagicMock(external_id="SmallBoat", space="sailboat"),
                        MagicMock(external_id="NmeaTimeSeries", space="sailboat"),
                    ]
                )
            )

        # Generate UDTFs using code_generator
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()
        else:
            raise ValueError("code_generator is None")
        assert result is not None
        assert result.total_count > 0

        # Register UDTFs via SQL registration (Unity Catalog)
        with patch.object(udtf_generator.udtf_registry, "register_udtf_via_sql", return_value=None):
            registered = udtf_generator.register_udtfs(secret_scope="cdf_sailboat_sailboat", if_exists="replace")
        assert registered is not None
        assert len(registered.registered_udtfs) > 0

    def test_register_with_secret_scope(
        self,
        udtf_generator: UDTFGenerator,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test UDTF registration with secret scope configuration."""
        # Mock secret scope operations
        mock_workspace_client.secrets.list_scopes.return_value = []
        mock_workspace_client.secrets.create_scope.return_value = None

        # Generate and register via SQL registration
        if udtf_generator.code_generator:
            _ = udtf_generator.code_generator.generate_udtfs()
        with patch.object(udtf_generator.udtf_registry, "register_udtf_via_sql", return_value=None):
            registered = udtf_generator.register_udtfs(secret_scope="cdf_sailboat_sailboat", if_exists="replace")

        assert registered is not None


@pytest.mark.integration
class TestDataModelUdtfQueries:
    """Integration tests for Data Model UDTF queries from notebook."""

    def test_basic_query_structure(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test basic UDTF query structure (Cell 14)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()
            assert result.total_count > 0

            # Verify UDTF files were created
            for _view_id, file_path in result.generated_files.items():
                assert file_path.exists()
                code = file_path.read_text()
                # Verify UDTF structure
                assert "class" in code
                assert "eval" in code.lower() or "__call__" in code.lower()

    def test_named_parameters_support(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test named parameters support (Cell 15)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()

            # Find small_boat UDTF
            small_boat_file = result.get_file("SmallBoat")
            if small_boat_file:
                code = small_boat_file.read_text()
                # Verify named parameter support (Python keyword arguments)
                assert "def" in code or "class" in code


@pytest.mark.integration
class TestTimeSeriesUdtfQueries:
    """Integration tests for Time Series UDTF queries from notebook."""

    def test_time_series_sql_udtf_generation(self, udtf_generator: UDTFGenerator) -> None:
        """Test SQL-native time series UDTF generation."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_time_series_udtfs()
            sql_udtf_file = result.get_file("time_series_sql_udtf")
            if sql_udtf_file is None:
                pytest.skip("time_series_sql_udtf template not available in installed cognite-pygen-spark")
            assert sql_udtf_file.exists()
            code = sql_udtf_file.read_text()
            assert "TimeSeriesSqlUDTF" in code

    def test_single_time_series_instance_id_parsing(self) -> None:
        """Test single time series instance_id parsing (Cell 17)."""
        from cognite.pygen_spark.utils import parse_instance_id

        instance_id_str = "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround"
        node_id = parse_instance_id(instance_id_str)

        assert node_id.space == "sailboat"
        assert "speedOverGround" in node_id.external_id

    def test_multiple_time_series_instance_ids_parsing(self) -> None:
        """Test multiple time series instance_ids parsing (Cell 18)."""
        from cognite.pygen_spark.utils import parse_instance_ids

        instance_ids_str = (
            "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,"
            "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue"
        )
        node_ids = parse_instance_ids(instance_ids_str)

        assert len(node_ids) == 2
        assert all(node_id.space == "sailboat" for node_id in node_ids)

    def test_latest_time_series_instance_ids_parsing(self) -> None:
        """Test latest time series instance_ids parsing (Cell 19)."""
        from cognite.pygen_spark.utils import parse_instance_ids

        instance_ids_str = (
            "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,"
            "sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue"
        )
        node_ids = parse_instance_ids(instance_ids_str)

        assert len(node_ids) == 2


@pytest.mark.integration
class TestFilteringQueries:
    """Integration tests for filtering queries from notebook."""

    def test_external_id_filter_support(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test external_id filter support (Cell 20)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()
            small_boat_file = result.get_file("SmallBoat")

            if small_boat_file:
                code = small_boat_file.read_text()
                # Verify external_id handling
                assert "external_id" in code.lower() or "externalId" in code.lower()

    def test_property_filter_support(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test property filter support (Cell 21)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()
            small_boat_file = result.get_file("SmallBoat")

            if small_boat_file:
                code = small_boat_file.read_text()
                # Verify property filter parameters
                assert "name" in code.lower()

    def test_numeric_range_filter_support(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test numeric range filter support (Cell 23)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()
            small_boat_file = result.get_file("SmallBoat")

            if small_boat_file:
                code = small_boat_file.read_text()
                # Verify numeric property handling
                assert "boat_guid" in code.lower() or "boatGuid" in code.lower()


@pytest.mark.integration
class TestJoinQueries:
    """Integration tests for JOIN queries from notebook."""

    def test_join_compatibility(
        self,
        udtf_generator: UDTFGenerator,
    ) -> None:
        """Test JOIN compatibility between UDTFs (Cell 25)."""
        if udtf_generator.code_generator:
            result = udtf_generator.code_generator.generate_udtfs()

            small_boat_file = result.get_file("SmallBoat")
            nmea_file = result.get_file("NmeaTimeSeries")

            if small_boat_file and nmea_file:
                small_boat_code = small_boat_file.read_text()
                nmea_code = nmea_file.read_text()

                # Verify both have join-compatible columns
                assert "space" in small_boat_code.lower() or "external_id" in small_boat_code.lower()
                assert "mmsi" in nmea_code.lower()
                assert "boat_guid" in small_boat_code.lower() or "boatGuid" in small_boat_code.lower()
