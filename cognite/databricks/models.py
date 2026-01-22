"""Pydantic models for UDTF registration results.

These models replace dictionary return types with structured, type-safe objects.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from cognite.pygen_spark.config import CDFConnectionConfig
from databricks.sdk.service.catalog import FunctionInfo


class RegisteredUDTFResult(BaseModel):
    """Result of registering a single UDTF and its view.

    Similar to how pygen-main structures results, providing type safety
    and better IDE support.

    Args:
        view_id: The external_id of the view
        function_info: The FunctionInfo object from Databricks SDK
        view_name: The full name of the registered view (if registered)
        udtf_file_path: Path to the generated UDTF Python file
        view_registered: Whether the view was successfully registered
    """

    view_id: str
    function_info: FunctionInfo | None = None
    view_name: str | None = None
    udtf_file_path: Path | None = None
    view_registered: bool = False


class UDTFRegistrationResult(BaseModel):
    """Complete result of UDTF and view registration.

    Returns structured data instead of a dictionary, providing:
    - Type safety with Pydantic validation
    - Better IDE autocomplete support
    - Self-documenting structure
    - Backward compatibility via by_view_id property

    Args:
        registered_udtfs: List of registered UDTF results
        catalog: The catalog where UDTFs were registered
        schema_name: The schema where UDTFs were registered
        total_count: Total number of successfully registered UDTFs
    """

    registered_udtfs: list[RegisteredUDTFResult] = Field(default_factory=list)
    catalog: str
    schema_name: str  # Renamed from "schema" to avoid shadowing BaseModel.schema()
    total_count: int = 0

    @property
    def by_view_id(self) -> dict[str, RegisteredUDTFResult]:
        """Convenience property to access by view_id (for backward compatibility).

        Returns:
            Dictionary mapping view_id to RegisteredUDTFResult
        """
        return {r.view_id: r for r in self.registered_udtfs}

    def get(self, view_id: str) -> RegisteredUDTFResult | None:
        """Get result for a specific view_id.

        Args:
            view_id: The external_id of the view

        Returns:
            RegisteredUDTFResult if found, None otherwise
        """
        return self.by_view_id.get(view_id)

    def __getitem__(self, view_id: str) -> RegisteredUDTFResult:
        """Allow dict-like access: result['view_id'].

        Args:
            view_id: The external_id of the view

        Returns:
            RegisteredUDTFResult

        Raises:
            KeyError: If view_id is not found
        """
        result = self.get(view_id)
        if result is None:
            raise KeyError(f"View ID '{view_id}' not found in registration results")
        return result


# Re-export CDFConnectionConfig from pygen-spark for backward compatibility
# The actual implementation is in cognite.pygen_spark.config
__all__ = [
    "CDFConnectionConfig",
    "RegisteredUDTFResult",
    "RegisteredViewResult",
    "TimeSeriesUDTFConfig",
    "TimeSeriesUDTFRegistry",
    "UDTFRegistrationResult",
    "ViewRegistrationResult",
    "time_series_udtf_registry",
]


class TimeSeriesUDTFConfig(BaseModel):
    """Configuration for a time series datapoints UDTF.

    Follows pydantic patterns for type-safe configuration, similar to pygen-main's
    configuration models.

    Args:
        udtf_name: Name of the UDTF function (e.g., "time_series_datapoints_udtf")
        view_name: Name of the view to create (e.g., "time_series_datapoints")
        parameters: List of parameter names (excluding credentials)
    """

    udtf_name: str = Field(..., description="UDTF function name")
    view_name: str = Field(..., description="View name")
    parameters: list[str] = Field(default_factory=list, description="UDTF-specific parameter names")


def _create_default_time_series_configs() -> dict[str, TimeSeriesUDTFConfig]:
    """Create default time series UDTF configurations.

    Returns:
        Dictionary mapping UDTF name to configuration
    """
    return {
        "time_series_datapoints_udtf": TimeSeriesUDTFConfig(
            udtf_name="time_series_datapoints_udtf",
            view_name="time_series_datapoints",
            parameters=["instance_id", "start", "end", "aggregates", "granularity"],
        ),
        "time_series_datapoints_detailed_udtf": TimeSeriesUDTFConfig(
            udtf_name="time_series_datapoints_detailed_udtf",
            view_name="time_series_datapoints_detailed",
            parameters=["instance_id", "start", "end", "aggregates", "granularity"],
        ),
        "time_series_latest_datapoints_udtf": TimeSeriesUDTFConfig(
            udtf_name="time_series_latest_datapoints_udtf",
            view_name="time_series_latest_datapoints",
            parameters=["instance_ids", "before", "include_status"],
        ),
    }


class TimeSeriesUDTFRegistry(BaseModel):
    """Registry of all time series UDTF configurations.

    Provides a centralized, type-safe way to manage time series UDTF configurations,
    following pydantic patterns similar to pygen-main's configuration management.

    Args:
        configs: Dictionary mapping UDTF name to configuration
    """

    configs: dict[str, TimeSeriesUDTFConfig] = Field(
        default_factory=_create_default_time_series_configs,
        description="Time series UDTF configurations",
    )

    def get_config(self, udtf_name: str) -> TimeSeriesUDTFConfig | None:
        """Get configuration for a specific UDTF.

        Args:
            udtf_name: The UDTF function name

        Returns:
            TimeSeriesUDTFConfig if found, None otherwise
        """
        return self.configs.get(udtf_name)

    def get_all_udtf_names(self) -> list[str]:
        """Get all registered UDTF names.

        Returns:
            List of UDTF function names
        """
        return list(self.configs.keys())


# Global registry instance (similar to pygen-main's global_config pattern)
time_series_udtf_registry = TimeSeriesUDTFRegistry()


class RegisteredViewResult(BaseModel):
    """Result for a single view registration.

    Args:
        view_id: The external_id of the view
        view_name: The full name of the registered view (if registered)
        view_registered: Whether the view was successfully registered
        error_message: Error message if registration failed
    """

    view_id: str
    view_name: str | None = None
    view_registered: bool = False
    error_message: str | None = None


class ViewRegistrationResult(BaseModel):
    """Complete result of view registration operation.

    Returns structured data instead of a dictionary, providing:
    - Type safety with Pydantic validation
    - Better IDE autocomplete support
    - Self-documenting structure
    - Convenience properties for access

    Args:
        registered_views: List of registered view results
        catalog: The catalog where views were registered
        schema_name: The schema where views were registered
        total_count: Total number of view registration attempts
    """

    registered_views: list[RegisteredViewResult] = Field(default_factory=list)
    catalog: str
    schema_name: str  # Renamed from "schema" to avoid shadowing BaseModel.schema()
    total_count: int = Field(ge=0)

    @property
    def by_view_id(self) -> dict[str, RegisteredViewResult]:
        """Convenience property for dict-like access.

        Returns:
            Dictionary mapping view_id to RegisteredViewResult
        """
        return {r.view_id: r for r in self.registered_views}

    def get(self, view_id: str) -> RegisteredViewResult | None:
        """Get result for specific view_id.

        Args:
            view_id: The external_id of the view

        Returns:
            RegisteredViewResult if found, None otherwise
        """
        return self.by_view_id.get(view_id)
