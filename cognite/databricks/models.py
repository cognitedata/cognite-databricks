"""Pydantic models for UDTF registration results and CDF connection configuration.

These models replace dictionary return types with structured, type-safe objects.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, field_validator

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
    function_info: FunctionInfo
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


class CDFConnectionConfig(BaseModel):
    """CDF connection configuration aligned with pygen-main's load_cognite_client_from_toml.
    
    This model ensures consistent URL construction across all templates and UDTF implementations,
    matching the behavior of CogniteClient.default_oauth_client_credentials().
    
    The SDK expects cdf_cluster to be just the cluster name (e.g., "greenfield"), and automatically
    constructs:
    - Base URL: https://{cdf_cluster}.cognitedata.com
    - Scopes: [https://{cdf_cluster}.cognitedata.com/.default]
    - Token URL: https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
    
    Args:
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        tenant_id: Azure AD tenant ID
        cdf_cluster: CDF cluster name (e.g., "greenfield", "westeurope-1") - just the name, not full domain
        project: CDF project name
        
    Examples:
        >>> config = CDFConnectionConfig(
        ...     client_id="...",
        ...     client_secret="...",
        ...     tenant_id="...",
        ...     cdf_cluster="greenfield",  # Just cluster name
        ...     project="my-project"
        ... )
        >>> config.base_url
        'https://greenfield.cognitedata.com'
        >>> config.scopes
        ['https://greenfield.cognitedata.com/.default']
    """
    
    client_id: str = Field(..., description="OAuth2 client ID")
    client_secret: str = Field(..., description="OAuth2 client secret")
    tenant_id: str = Field(..., description="Azure AD tenant ID")
    cdf_cluster: str = Field(..., description="CDF cluster name (e.g., 'greenfield')")
    project: str = Field(..., description="CDF project name")
    
    @field_validator("cdf_cluster")
    @classmethod
    def normalize_cluster(cls, v: str) -> str:
        """Normalize cluster name to ensure it's just the cluster name.
        
        Handles cases where full domain might be provided:
        - "greenfield" -> "greenfield"
        - "greenfield.cognitedata.com" -> "greenfield"
        - "https://greenfield.cognitedata.com" -> "greenfield"
        
        This ensures compatibility with both the SDK's expectations and
        cases where users might provide the full domain.
        """
        # Remove protocol if present
        if v.startswith("https://"):
            v = v[8:]
        elif v.startswith("http://"):
            v = v[7:]
        
        # Remove domain suffix if present
        if v.endswith(".cognitedata.com"):
            v = v[:-16]  # Remove ".cognitedata.com"
        
        return v.strip()
    
    @property
    def base_url(self) -> str:
        """Get the CDF base URL (aligned with CogniteClient.default).
        
        Returns:
            Base URL in format: https://{cluster}.cognitedata.com
        """
        return f"https://{self.cdf_cluster}.cognitedata.com"
    
    @property
    def scopes(self) -> list[str]:
        """Get OAuth2 scopes (aligned with OAuthClientCredentials.default_for_azure_ad).
        
        Returns:
            List containing: https://{cluster}.cognitedata.com/.default
        """
        return [f"{self.base_url}/.default"]
    
    @property
    def token_url(self) -> str:
        """Get the OAuth2 token URL (aligned with OAuthClientCredentials.default_for_azure_ad).
        
        Returns:
            Token URL: https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
        """
        return f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
    
    def create_credentials(self) -> "OAuthClientCredentials":
        """Create OAuth2 credentials using SDK's default_for_azure_ad (aligned with pygen-main).
        
        This uses the SDK's built-in method to ensure consistency with
        CogniteClient.default_oauth_client_credentials().
        
        Returns:
            OAuthClientCredentials instance
            
        Raises:
            ImportError: If cognite-sdk is not installed
        """
        from cognite.client.credentials import OAuthClientCredentials
        
        return OAuthClientCredentials.default_for_azure_ad(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            cdf_cluster=self.cdf_cluster,
        )
    
    def create_client(self, client_name: str = "databricks-udtf", **kwargs) -> "CogniteClient":
        """Create a CogniteClient using SDK's default_oauth_client_credentials (aligned with pygen-main).
        
        This method uses the same approach as load_cognite_client_from_toml() in pygen-main,
        ensuring consistency across the ecosystem.
        
        Args:
            client_name: Name for the client (default: "databricks-udtf")
            **kwargs: Additional arguments to pass to CogniteClient
            
        Returns:
            CogniteClient instance configured with this connection
            
        Raises:
            ImportError: If cognite-sdk is not installed
        """
        from cognite.client import CogniteClient
        
        # Use SDK's default method (same as load_cognite_client_from_toml)
        return CogniteClient.default_oauth_client_credentials(
            project=self.project,
            cdf_cluster=self.cdf_cluster,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            client_name=client_name,
        )
    
    @classmethod
    def from_toml(cls, toml_file: str | Path = "config.toml", section: str | None = "cognite") -> "CDFConnectionConfig":
        """Load configuration from a TOML file (aligned with load_cognite_client_from_toml).
        
        Args:
            toml_file: Path to TOML file
            section: Section name in TOML file (default: "cognite")
            
        Returns:
            CDFConnectionConfig instance
            
        Examples:
            >>> config = CDFConnectionConfig.from_toml("config.toml")
            >>> client = config.create_client()
        """
        import toml
        
        toml_content = toml.load(toml_file)
        if section is not None:
            toml_content = toml_content[section]
        
        return cls(**toml_content)


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
            parameters=["space", "external_id", "start", "end", "aggregates", "granularity"],
        ),
        "time_series_datapoints_long_udtf": TimeSeriesUDTFConfig(
            udtf_name="time_series_datapoints_long_udtf",
            view_name="time_series_datapoints_long",
            parameters=["space", "external_ids", "start", "end", "aggregates", "granularity", "include_aggregate_name"],
        ),
        "time_series_latest_datapoints_udtf": TimeSeriesUDTFConfig(
            udtf_name="time_series_latest_datapoints_udtf",
            view_name="time_series_latest_datapoints",
            parameters=["space", "external_ids", "before", "include_status"],
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

