"""Time Series Datapoints UDTFs for Cognite Data Fusion.

These UDTFs provide access to CDF Time Series datapoints from Databricks SQL,
supporting all three registration modes:
- Unity Catalog UDTF (direct call)
- Unity Catalog View (via view)
- Session-scoped UDTF (PySpark Connect)
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    DoubleType,
    IntegerType,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

# Wrap critical imports in try-except to handle missing dependencies (DBR < 18.1 limitation)
try:
    from cognite.client import CogniteClient
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.data_classes.data_modeling.ids import NodeId
    COGNITE_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as import_error:
    COGNITE_AVAILABLE = False
    IMPORT_ERROR = str(import_error)
    # Create dummy classes to prevent syntax errors if imports fail
    class CogniteClient:
        pass
    class OAuthClientCredentials:
        pass
    class NodeId:
        def __init__(self, space: str, external_id: str):
            self.space = space
            self.external_id = external_id


class TimeSeriesDatapointsUDTF:
    """UDTF for retrieving datapoints from a single Time Series using instance_id.
    
    Similar to client.time_series.data.retrieve() for a single time series.
    Returns rows in format: (timestamp, value)
    Note: All datapoints come from the same time series, so space/external_id not needed per row.
    """
    
    @staticmethod
    def outputSchema() -> StructType:
        """Return the output schema: (timestamp, value)."""
        return StructType([
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
        ])
    
    @staticmethod
    def analyze(
        space: str | None = None,
        external_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        aggregates: str | None = None,
        granularity: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ):
        """Analyze method required by PySpark Connect for session-scoped UDTFs.
        
        This method is used by PySpark Connect to validate arguments and determine output schema.
        For Unity Catalog registration, this method is optional but harmless if present.
        
        Args:
            space: CDF space name (required)
            external_id: CDF external_id (required)
            start: Start timestamp (ISO 8601 or "2w-ago", "1d-ago", etc.)
            end: End timestamp (ISO 8601 or "now", "1d-ahead", etc.)
            aggregates: Optional aggregate type (e.g., "average", "max", "min", "count")
            granularity: Optional granularity for aggregates (e.g., "1h", "1d", "30s")
            client_id: OAuth2 client ID (required)
            client_secret: OAuth2 client secret (required)
            tenant_id: Azure AD tenant ID (required)
            cdf_cluster: CDF cluster URL (required)
            project: CDF project name (required)
        
        Returns:
            AnalyzeResult containing the output schema
        """
        from pyspark.sql.udtf import AnalyzeResult
        return AnalyzeResult(TimeSeriesDatapointsUDTF.outputSchema())
    
    def __init__(self) -> None:
        """Initialize UDTF (no parameters allowed when using analyze method).
        
        This method works for all three registration modes:
        - Unity Catalog UDTF (direct call): Parameters go to eval()
        - Unity Catalog View (via view): Parameters go to eval()
        - Session-scoped: Parameters go to eval()
        
        Client initialization happens in eval() for all modes.
        """
        # Initialize instance variables
        self.client = None
        self._client_initialized = False
        self._init_error = None
        self._init_error_category = None
        self._init_success = True
    
    def _create_client(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        cdf_cluster: str,
        project: str,
    ) -> CogniteClient:
        """Create CogniteClient from OAuth2 credentials (aligned with pygen-main)."""
        from cognite.databricks.models import CDFConnectionConfig
        
        config = CDFConnectionConfig(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            cdf_cluster=cdf_cluster,
            project=project,
        )
        
        return config.create_client(client_name="databricks-udtf")
    
    def eval(
        self,
        space: str | None = None,
        external_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        aggregates: str | None = None,
        granularity: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ) -> Iterator[tuple[object, ...]]:
        """Execute UDTF using instance_id (space + external_id) for query.
        
        This method works for all three registration modes:
        - Unity Catalog: client is initialized here on first call, all parameters are for querying
        - Session-scoped: client is initialized here on first call, all parameters are required
        
        Args:
            space: CDF space name (required)
            external_id: CDF external_id (required)
            start: Start timestamp (ISO 8601 or "2w-ago", "1d-ago", etc.)
            end: End timestamp (ISO 8601 or "now", "1d-ahead", etc.)
            aggregates: Optional aggregate type (e.g., "average", "max", "min", "count")
            granularity: Optional granularity for aggregates (e.g., "1h", "1d", "30s")
            client_id: OAuth2 client ID (required)
            client_secret: OAuth2 client secret (required)
            tenant_id: Azure AD tenant ID (required)
            cdf_cluster: CDF cluster URL (required)
            project: CDF project name (required)
        
        Yields:
            (timestamp, value) tuples - all from the same time series instance
        """
        import sys
        import traceback
        
        try:
            # Initialize client if not already initialized
            if not self._client_initialized:
                if client_id is None or client_secret is None:
                    error_msg = "Missing credentials: client_id and client_secret are required."
                    yield (None, None)
                    return
                
                # Check if dependencies are available
                if not COGNITE_AVAILABLE:
                    error_msg = f"Missing dependencies: {IMPORT_ERROR}. Install cognite-sdk or use DBR 18.1+ with ENVIRONMENT clause."
                    yield (None, None)
                    return
                
                try:
                    self.client = self._create_client(client_id, client_secret, tenant_id, cdf_cluster, project)
                    self._client_initialized = True
                    self._init_error = None
                    self._init_error_category = None
                except Exception as e:
                    self._init_error = f"{type(e).__name__}: {str(e)}"
                    self._client_initialized = True
                    yield (None, None)
                    return
            
            # Check if initialization succeeded
            if not hasattr(self, '_init_success') or not self._init_success or self._init_error is not None:
                error_msg = getattr(self, '_init_error', 'Unknown initialization error')
                yield (None, None)
                return
            
            # Validate inputs
            if not space or not external_id:
                sys.stderr.write("ERROR: Both space and external_id are required\n")
                yield (None, None)
                return
            
            if not start:
                start = "2w-ago"  # Default to last 2 weeks
            if not end:
                end = "now"
            
            try:
                # Use instance_id (NodeId) for query
                datapoints = self.client.time_series.data.retrieve(
                    instance_id=NodeId(space, external_id),
                    start=start,
                    end=end,
                    aggregates=[aggregates] if aggregates else None,
                    granularity=granularity,
                )
                
                # Yield datapoints (no space/external_id needed - all from same instance)
                if aggregates:
                    # For aggregates, access by aggregate name (e.g., .average, .max)
                    aggregate_name = aggregates.lower()
                    if hasattr(datapoints, aggregate_name):
                        values = getattr(datapoints, aggregate_name)
                        timestamps = datapoints.timestamp
                        for ts_ms, val in zip(timestamps, values):
                            # Convert milliseconds timestamp to datetime for PySpark TimestampType
                            timestamp_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms is not None else None
                            yield (timestamp_dt, val)
                    else:
                        sys.stderr.write(f"ERROR: Aggregate '{aggregates}' not found in response\n")
                        yield (None, None)
                else:
                    # For raw datapoints, use .value
                    for dp in datapoints:
                        # Convert milliseconds timestamp to datetime for PySpark TimestampType
                        timestamp_dt = datetime.fromtimestamp(dp.timestamp / 1000.0, tz=timezone.utc) if dp.timestamp is not None else None
                        yield (timestamp_dt, dp.value)
                    
                    # If no rows were found, yield at least one row with None values
                    # This prevents "end-of-input" error when the time series is empty
                    if len(datapoints) == 0:
                        sys.stderr.write("[UDTF] ⚠ No datapoints found, yielding empty row to prevent 'end-of-input' error\n")
                        yield (None, None)
                        
            except Exception as e:
                # Log error for debugging
                error_info = f"[UDTF] ✗ Error during query: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                sys.stderr.write(error_info)
                # Yield a row with error information
                yield (None, None)
        except Exception as outer_error:
            # Last resort: if anything goes wrong, yield an error row
            error_info = f"ERROR: Unexpected error in eval(): {type(outer_error).__name__}: {str(outer_error)}"
            sys.stderr.write(f"{error_info}\n{traceback.format_exc()}\n")
            yield (None, None)


class TimeSeriesDatapointsLongUDTF:
    """UDTF for retrieving datapoints from multiple Time Series in long format using instance_id.
    
    Similar to client.time_series.data.retrieve_dataframe() but returns long format.
    Returns rows in format: (timestamp, time_series_external_id, value)
    Note: time_series_external_id identifies which time series, space is same for all.
    
    Use SQL PIVOT to convert to wide format like retrieve_dataframe.
    """
    
    @staticmethod
    def outputSchema() -> StructType:
        """Return the output schema: (timestamp, time_series_external_id, value)."""
        return StructType([
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("time_series_external_id", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
        ])
    
    @staticmethod
    def analyze(
        space: str | None = None,
        external_ids: str | None = None,  # Comma-separated list
        start: str | None = None,
        end: str | None = None,
        aggregates: str | None = None,
        granularity: str | None = None,
        include_aggregate_name: bool = False,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ):
        """Analyze method required by PySpark Connect for session-scoped UDTFs.
        
        Args:
            space: CDF space name (required, all time series must be in same space)
            external_ids: Comma-separated list of external_ids (e.g., "foo,bar,baz")
            start, end, aggregates, granularity: Query parameters
            include_aggregate_name: Whether to include aggregate name in time_series_external_id
            client_id, client_secret, tenant_id, cdf_cluster, project: OAuth credentials
        """
        from pyspark.sql.udtf import AnalyzeResult
        return AnalyzeResult(TimeSeriesDatapointsLongUDTF.outputSchema())
    
    def __init__(self) -> None:
        """Initialize UDTF (parameter-free for all registration modes)."""
        self.client = None
        self._client_initialized = False
        self._init_error = None
        self._init_success = True
    
    def _create_client(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        cdf_cluster: str,
        project: str,
    ) -> CogniteClient:
        """Create CogniteClient from OAuth2 credentials (aligned with pygen-main)."""
        from cognite.databricks.models import CDFConnectionConfig
        
        config = CDFConnectionConfig(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            cdf_cluster=cdf_cluster,
            project=project,
        )
        
        return config.create_client(client_name="databricks-udtf")
    
    def eval(
        self,
        space: str | None = None,
        external_ids: str | None = None,  # Comma-separated: "foo,bar,baz"
        start: str | None = None,
        end: str | None = None,
        aggregates: str | None = None,
        granularity: str | None = None,
        include_aggregate_name: bool = False,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ) -> Iterator[tuple[object, ...]]:
        """Retrieve Time Series datapoints in long format using instance_id.
        
        Args:
            space: CDF space name (required, all time series must be in same space)
            external_ids: Comma-separated list of external_ids (e.g., "foo,bar,baz")
            start: Start timestamp (ISO 8601 or "2w-ago")
            end: End timestamp (ISO 8601 or "now")
            aggregates: Optional aggregate type (e.g., "average", "max", "min")
            granularity: Optional granularity for aggregates (e.g., "1h", "30d")
            include_aggregate_name: Whether to include aggregate name in time_series_external_id (for compatibility with retrieve_dataframe)
            client_id: OAuth2 client ID (from Secret)
            client_secret: OAuth2 client secret (from Secret)
            tenant_id: Azure AD tenant ID (from Secret)
            cdf_cluster: CDF cluster URL (from Secret)
            project: CDF project name (from Secret)
        
        Yields:
            (timestamp, time_series_external_id, value) tuples
        """
        import sys
        import traceback
        
        try:
            # Initialize client
            if not self._client_initialized:
                if not client_id or not client_secret:
                    yield (None, None, None)
                    return
                
                if not COGNITE_AVAILABLE:
                    yield (None, None, None)
                    return
                
                try:
                    self.client = self._create_client(
                        client_id, client_secret, tenant_id, cdf_cluster, project
                    )
                    self._client_initialized = True
                    self._init_error = None
                except Exception as e:
                    self._init_error = f"{type(e).__name__}: {str(e)}"
                    self._client_initialized = True
                    yield (None, None, None)
                    return
            
            if not hasattr(self, '_init_success') or not self._init_success or self._init_error is not None:
                yield (None, None, None)
                return
            
            # Validate inputs
            if not space or not external_ids:
                sys.stderr.write("ERROR: Both space and external_ids are required\n")
                yield (None, None, None)
                return
            
            # Parse comma-separated external_ids
            external_id_list = [eid.strip() for eid in external_ids.split(",")]
            
            # Create NodeId list for instance_id queries
            instance_ids = [NodeId(space, eid) for eid in external_id_list]
            
            if not start:
                start = "2w-ago"
            if not end:
                end = "now"
            
            try:
                # Use instance_id for query
                datapoints_list = self.client.time_series.data.retrieve(
                    instance_id=instance_ids,
                    start=start,
                    end=end,
                    aggregates=[aggregates] if aggregates else None,
                    granularity=granularity,
                )
                
                # Yield datapoints with external_id identifier (space is same for all)
                row_count = 0
                for dps in datapoints_list:
                    # Extract external_id from instance_id for display
                    ts_external_id = dps.instance_id.external_id if dps.instance_id else None
                    
                    if not ts_external_id:
                        # Fallback: try to get from external_id attribute
                        ts_external_id = dps.external_id if hasattr(dps, 'external_id') and dps.external_id else None
                    
                    # Optionally include aggregate name in external_id (for compatibility with retrieve_dataframe)
                    if aggregates and include_aggregate_name:
                        ts_external_id = f"{ts_external_id}|{aggregates.lower()}"
                    
                    if aggregates:
                        # For aggregates, access by aggregate name
                        aggregate_name = aggregates.lower()
                        if hasattr(dps, aggregate_name):
                            values = getattr(dps, aggregate_name)
                            timestamps = dps.timestamp
                            for ts_ms, val in zip(timestamps, values):
                                # Convert milliseconds timestamp to datetime for PySpark TimestampType
                                timestamp_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms is not None else None
                                yield (timestamp_dt, ts_external_id, val)
                                row_count += 1
                        else:
                            sys.stderr.write(f"ERROR: Aggregate '{aggregates}' not found\n")
                    else:
                        # For raw datapoints
                        for dp in dps:
                            # Convert milliseconds timestamp to datetime for PySpark TimestampType
                            timestamp_dt = datetime.fromtimestamp(dp.timestamp / 1000.0, tz=timezone.utc) if dp.timestamp is not None else None
                            yield (timestamp_dt, ts_external_id, dp.value)
                            row_count += 1
                
                # If no rows were found, yield at least one row with None values
                if row_count == 0:
                    sys.stderr.write("[UDTF] ⚠ No datapoints found, yielding empty row to prevent 'end-of-input' error\n")
                    yield (None, None, None)
                        
            except Exception as e:
                error_info = f"[UDTF] ✗ Error during query: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                sys.stderr.write(error_info)
                yield (None, None, None)
        except Exception as outer_error:
            error_info = f"ERROR: Unexpected error in eval(): {type(outer_error).__name__}: {str(outer_error)}"
            sys.stderr.write(f"{error_info}\n{traceback.format_exc()}\n")
            yield (None, None, None)


class TimeSeriesLatestDatapointsUDTF:
    """UDTF for retrieving the latest datapoint(s) for one or more Time Series using instance_id.
    
    Similar to client.time_series.data.retrieve_latest()
    Returns rows in format: (time_series_external_id, timestamp, value, status_code)
    Note: time_series_external_id identifies which time series, space is same for all.
    """
    
    @staticmethod
    def outputSchema() -> StructType:
        """Return schema: (time_series_external_id, timestamp, value, status_code)."""
        return StructType([
            StructField("time_series_external_id", StringType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
            StructField("status_code", IntegerType(), nullable=True),  # Optional
        ])
    
    @staticmethod
    def analyze(
        space: str | None = None,
        external_ids: str | None = None,  # Comma-separated list
        before: str | None = None,
        include_status: bool = False,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ):
        """Analyze method required by PySpark Connect for session-scoped UDTFs.
        
        Args:
            space: CDF space name (required)
            external_ids: Comma-separated list of external_ids (e.g., "foo,bar,baz")
            before: Get latest before this time (ISO 8601 or "1h-ago", "now", etc.)
            include_status: Include status code in output
            client_id, client_secret, tenant_id, cdf_cluster, project: OAuth credentials
        """
        from pyspark.sql.udtf import AnalyzeResult
        return AnalyzeResult(TimeSeriesLatestDatapointsUDTF.outputSchema())
    
    def __init__(self) -> None:
        """Initialize UDTF (parameter-free for all registration modes)."""
        self.client = None
        self._client_initialized = False
        self._init_error = None
        self._init_success = True
    
    def _create_client(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        cdf_cluster: str,
        project: str,
    ) -> CogniteClient:
        """Create CogniteClient from OAuth2 credentials (aligned with pygen-main)."""
        from cognite.databricks.models import CDFConnectionConfig
        
        config = CDFConnectionConfig(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            cdf_cluster=cdf_cluster,
            project=project,
        )
        
        return config.create_client(client_name="databricks-udtf")
    
    def eval(
        self,
        space: str | None = None,
        external_ids: str | None = None,  # Comma-separated: "foo,bar,baz"
        before: str | None = None,  # Get latest before this time (e.g., "1h-ago")
        include_status: bool = False,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
    ) -> Iterator[tuple[object, ...]]:
        """Retrieve latest datapoint(s) for one or more time series using instance_id.
        
        Args:
            space: CDF space name (required)
            external_ids: Comma-separated list of external_ids (e.g., "foo,bar,baz")
            before: Get latest before this time (ISO 8601 or "1h-ago", "now", etc.)
            include_status: Include status code in output
            client_id: OAuth2 client ID (from Secret)
            client_secret: OAuth2 client secret (from Secret)
            tenant_id: Azure AD tenant ID (from Secret)
            cdf_cluster: CDF cluster URL (from Secret)
            project: CDF project name (from Secret)
        
        Yields:
            (time_series_external_id, timestamp, value, status_code) tuples
        """
        import sys
        import traceback
        
        try:
            # Initialize client
            if not self._client_initialized:
                if not client_id or not client_secret:
                    yield (None, None, None, None)
                    return
                
                if not COGNITE_AVAILABLE:
                    yield (None, None, None, None)
                    return
                
                try:
                    self.client = self._create_client(
                        client_id, client_secret, tenant_id, cdf_cluster, project
                    )
                    self._client_initialized = True
                    self._init_error = None
                except Exception as e:
                    self._init_error = f"{type(e).__name__}: {str(e)}"
                    self._client_initialized = True
                    yield (None, None, None, None)
                    return
            
            if not hasattr(self, '_init_success') or not self._init_success or self._init_error is not None:
                yield (None, None, None, None)
                return
            
            # Validate inputs
            if not space or not external_ids:
                sys.stderr.write("ERROR: Both space and external_ids are required\n")
                yield (None, None, None, None)
                return
            
            # Parse comma-separated external_ids
            external_id_list = [eid.strip() for eid in external_ids.split(",")]
            
            # Create NodeId list
            instance_ids = [NodeId(space, eid) for eid in external_id_list]
            
            if not before:
                before = "now"
            
            try:
                # Use instance_id for query
                datapoints_list = self.client.time_series.data.retrieve_latest(
                    instance_id=instance_ids,
                    before=before,
                    include_status=include_status,
                )
                
                # Yield latest datapoints (no space needed - same for all)
                row_count = 0
                for dps in datapoints_list:
                    if dps is None:  # Time series not found
                        continue
                    
                    # Extract external_id from instance_id
                    ts_external_id = dps.instance_id.external_id if dps.instance_id else None
                    
                    if not ts_external_id:
                        # Fallback: try to get from external_id attribute
                        ts_external_id = dps.external_id if hasattr(dps, 'external_id') and dps.external_id else None
                    
                    # Get the latest datapoint (first in the list)
                    if len(dps) > 0:
                        latest_dp = dps[0]
                        # Convert milliseconds timestamp to datetime for PySpark TimestampType
                        timestamp_dt = datetime.fromtimestamp(latest_dp.timestamp / 1000.0, tz=timezone.utc) if latest_dp.timestamp is not None else None
                        yield (
                            ts_external_id,
                            timestamp_dt,
                            latest_dp.value,
                            latest_dp.status_code if include_status else None,
                        )
                        row_count += 1
                
                # If no rows were found, yield at least one row with None values
                if row_count == 0:
                    sys.stderr.write("[UDTF] ⚠ No latest datapoints found, yielding empty row to prevent 'end-of-input' error\n")
                    yield (None, None, None, None)
                        
            except Exception as e:
                error_info = f"[UDTF] ✗ Error during query: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                sys.stderr.write(error_info)
                yield (None, None, None, None)
        except Exception as outer_error:
            error_info = f"ERROR: Unexpected error in eval(): {type(outer_error).__name__}: {str(outer_error)}"
            sys.stderr.write(f"{error_info}\n{traceback.format_exc()}\n")
            yield (None, None, None, None)

