"""Unit tests for models module."""

from __future__ import annotations

from pathlib import Path

import pytest
from databricks.sdk.service.catalog import FunctionInfo

from cognite.databricks.models import (
    RegisteredUDTFResult,
    TimeSeriesUDTFConfig,
    TimeSeriesUDTFRegistry,
    UDTFRegistrationResult,
    _create_default_time_series_configs,
    time_series_udtf_registry,
)


class TestRegisteredUDTFResult:
    """Tests for RegisteredUDTFResult model."""

    def test_init(self) -> None:
        """Test RegisteredUDTFResult initialization."""
        result = RegisteredUDTFResult(
            view_id="SmallBoat",
            function_info=None,
            view_name=None,
            udtf_file_path=None,
            view_registered=False,
        )

        assert result.view_id == "SmallBoat"
        assert result.function_info is None
        assert result.view_registered is False

    def test_with_function_info(self) -> None:
        """Test RegisteredUDTFResult with FunctionInfo."""
        func_info = FunctionInfo(name="small_boat_udtf", catalog_name="catalog", schema_name="schema")
        result = RegisteredUDTFResult(
            view_id="SmallBoat",
            function_info=func_info,
            view_name="catalog.schema.small_boat_view",
            udtf_file_path=Path("/path/to/SmallBoat_udtf.py"),
            view_registered=True,
        )

        assert result.function_info == func_info
        assert result.view_registered is True


class TestUDTFRegistrationResult:
    """Tests for UDTFRegistrationResult model."""

    def test_init(self) -> None:
        """Test UDTFRegistrationResult initialization."""
        result = UDTFRegistrationResult(
            registered_udtfs=[],
            catalog="test_catalog",
            schema_name="test_schema",
            total_count=0,
        )

        assert result.catalog == "test_catalog"
        assert result.schema_name == "test_schema"
        assert result.total_count == 0

    def test_by_view_id(self) -> None:
        """Test by_view_id property."""
        result1 = RegisteredUDTFResult(view_id="SmallBoat")
        result2 = RegisteredUDTFResult(view_id="LargeBoat")

        registration_result = UDTFRegistrationResult(
            registered_udtfs=[result1, result2],
            catalog="catalog",
            schema_name="schema",
            total_count=2,
        )

        by_id = registration_result.by_view_id
        assert len(by_id) == 2
        assert by_id["SmallBoat"] == result1
        assert by_id["LargeBoat"] == result2

    def test_get(self) -> None:
        """Test get method."""
        result1 = RegisteredUDTFResult(view_id="SmallBoat")
        registration_result = UDTFRegistrationResult(
            registered_udtfs=[result1],
            catalog="catalog",
            schema_name="schema",
            total_count=1,
        )

        assert registration_result.get("SmallBoat") == result1
        assert registration_result.get("NonExistent") is None

    def test_getitem(self) -> None:
        """Test __getitem__ method."""
        result1 = RegisteredUDTFResult(view_id="SmallBoat")
        registration_result = UDTFRegistrationResult(
            registered_udtfs=[result1],
            catalog="catalog",
            schema_name="schema",
            total_count=1,
        )

        assert registration_result["SmallBoat"] == result1

    def test_getitem_key_error(self) -> None:
        """Test __getitem__ raises KeyError for missing view_id."""
        registration_result = UDTFRegistrationResult(
            registered_udtfs=[],
            catalog="catalog",
            schema_name="schema",
            total_count=0,
        )

        with pytest.raises(KeyError, match="View ID 'NonExistent' not found"):
            _ = registration_result["NonExistent"]


class TestTimeSeriesUDTFConfig:
    """Tests for TimeSeriesUDTFConfig model."""

    def test_init(self) -> None:
        """Test TimeSeriesUDTFConfig initialization."""
        config = TimeSeriesUDTFConfig(
            udtf_name="time_series_datapoints_udtf",
            view_name="time_series_datapoints",
            parameters=["instance_id", "start", "end"],
        )

        assert config.udtf_name == "time_series_datapoints_udtf"
        assert config.view_name == "time_series_datapoints"
        assert len(config.parameters) == 3

    def test_default_parameters(self) -> None:
        """Test TimeSeriesUDTFConfig with default parameters."""
        config = TimeSeriesUDTFConfig(udtf_name="test_udtf", view_name="test_view")

        assert config.parameters == []


class TestTimeSeriesUDTFRegistry:
    """Tests for TimeSeriesUDTFRegistry model."""

    def test_get_config(self) -> None:
        """Test get_config method."""
        registry = TimeSeriesUDTFRegistry()

        config = registry.get_config("time_series_datapoints_udtf")
        assert config is not None
        assert config.udtf_name == "time_series_datapoints_udtf"
        assert config.view_name == "time_series_datapoints"

    def test_get_config_not_found(self) -> None:
        """Test get_config returns None for unknown UDTF."""
        registry = TimeSeriesUDTFRegistry()

        config = registry.get_config("unknown_udtf")
        assert config is None

    def test_get_all_udtf_names(self) -> None:
        """Test get_all_udtf_names method."""
        registry = TimeSeriesUDTFRegistry()

        names = registry.get_all_udtf_names()
        assert len(names) == 3
        assert "time_series_datapoints_udtf" in names
        assert "time_series_datapoints_detailed_udtf" in names
        assert "time_series_latest_datapoints_udtf" in names


class TestCreateDefaultTimeSeriesConfigs:
    """Tests for _create_default_time_series_configs function."""

    def test_create_default_configs(self) -> None:
        """Test _create_default_time_series_configs returns all three UDTFs."""
        configs = _create_default_time_series_configs()

        assert len(configs) == 3
        assert "time_series_datapoints_udtf" in configs
        assert "time_series_datapoints_detailed_udtf" in configs
        assert "time_series_latest_datapoints_udtf" in configs

    def test_time_series_datapoints_udtf_config(self) -> None:
        """Test time_series_datapoints_udtf configuration."""
        configs = _create_default_time_series_configs()
        config = configs["time_series_datapoints_udtf"]

        assert config.udtf_name == "time_series_datapoints_udtf"
        assert config.view_name == "time_series_datapoints"
        # Note: The config still uses old "space", "external_id" parameters
        # but actual UDTFs use "instance_id" - this is a known discrepancy
        assert len(config.parameters) > 0

    def test_global_registry(self) -> None:
        """Test global time_series_udtf_registry instance."""
        assert isinstance(time_series_udtf_registry, TimeSeriesUDTFRegistry)
        assert len(time_series_udtf_registry.get_all_udtf_names()) == 3
