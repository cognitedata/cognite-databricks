"""UDTFRegistry - Utility for registering Python UDTFs in Unity Catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    CreateFunction,
    CreateFunctionParameterStyle,
    CreateFunctionRoutineBody,
    CreateFunctionSecurityType,
    CreateFunctionSqlDataAccess,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

if TYPE_CHECKING:
    pass


class UDTFRegistry:
    """Utility for registering Python UDTFs in Unity Catalog."""

    def __init__(self, workspace_client: WorkspaceClient) -> None:
        """Initialize UDTF registry.

        Args:
            workspace_client: Databricks WorkspaceClient instance
        """
        self.workspace_client = workspace_client

    def _get_default_warehouse_id(self) -> str:
        """Finds and returns the ID of the first available SQL warehouse.

        Returns:
            The warehouse ID string

        Raises:
            ValueError: If no warehouses are found
        """
        warehouses = list(self.workspace_client.warehouses.list())
        if not warehouses:
            raise ValueError("No SQL warehouses found. Please provide warehouse_id parameter or create a warehouse.")
        warehouse_id = warehouses[0].id
        if warehouse_id is None:
            raise ValueError("Warehouse ID is None")
        return warehouse_id

    def register_udtf(
        self,
        catalog: str,
        schema: str,
        function_name: str,
        udtf_code: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        return_params: list[FunctionParameterInfo],  # NEW: Structured return columns
        dependencies: list[str] | None = None,  # For DBR 18.1+ custom dependencies
        comment: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
    ) -> FunctionInfo | None:
        """Register a Python UDTF in Unity Catalog.

        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            function_name: UDTF function name
            udtf_code: Python code for the UDTF class (must include eval() method with yield)
            input_params: Function parameter definitions (see type_json format notes below)
            return_type: Return type DDL string for the UDTF output schema.
                        Format: "TABLE(col1 TYPE, col2 TYPE, ...)"
                        Example: "TABLE(id INT, name STRING, score DOUBLE)"
            return_params: Structured metadata for the UDTF output columns (required for TABLE_TYPE).
            dependencies: Optional list of Python package dependencies (DBR 18.1+).
                         If None, uses fallback mode for pre-DBR 18.1 (requires pre-installed packages).
            comment: Function description
            if_exists: What to do if function already exists:
                      - "skip": Skip registration and return existing function (default)
                      - "replace": Delete and recreate the function
                      - "error": Raise ResourceAlreadyExists error
            debug: If True, prints detailed information about the UDTF registration.

        Returns:
            Registered FunctionInfo, or None if skipped

        Note:
            For Python UDTFs registered via Unity Catalog API:
            - data_type must be TABLE_TYPE
            - full_data_type must be the DDL string like "TABLE(col1 TYPE, col2 TYPE, ...)"
            - return_params MUST be provided as structured metadata even if also in full_data_type
            - routine_body must be EXTERNAL
            - external_language must be PYTHON
        """
        from databricks.sdk.errors import NotFound, ResourceAlreadyExists

        # Check if function already exists
        full_function_name = f"{catalog}.{schema}.{function_name}"

        if debug:
            print(f"\n[DEBUG] === Registering UDTF: {full_function_name} ===")
            print(f"[DEBUG] if_exists: {if_exists}")
            print(f"[DEBUG] Input parameters ({len(input_params)}):")
            for p in input_params:
                print(f"  - name={p.name}, position={p.position}")
                print(f"    type_text='{p.type_text}'")
                print(f"    type_name={p.type_name}")
                print(f"    type_json='{p.type_json}'")
                print(f"    parameter_mode={p.parameter_mode}")
                print(f"    parameter_type={p.parameter_type}")
            print(f"[DEBUG] Return columns ({len(return_params)}):")
            for p in return_params:
                print(f"  - name={p.name}, position={p.position}, type_text='{p.type_text}'")
            print(f"[DEBUG] Return type (DDL): {return_type}")
            print(f"[DEBUG] UDTF code length: {len(udtf_code)} chars")
        try:
            existing_function = self.workspace_client.functions.get(full_function_name)
            if if_exists == "skip":
                # Return existing function without error
                return existing_function
            elif if_exists == "replace":
                # Delete existing function before creating new one
                self.workspace_client.functions.delete(full_function_name)
                # Wait a brief moment to ensure deletion is complete
                # This prevents race conditions where the function might still exist during creation
                import time

                time.sleep(0.5)
            elif if_exists == "error":
                # Raise error immediately if function exists
                raise ResourceAlreadyExists(f"Routine or Model '{function_name}' already exists in {catalog}.{schema}")
            else:
                raise ValueError(f"Invalid if_exists value: {if_exists}. Must be 'skip', 'replace', or 'error'")
        except ResourceAlreadyExists:
            # Re-raise ResourceAlreadyExists if it was raised above
            raise
        except NotFound:
            # Function doesn't exist, proceed with creation
            pass

        # Build CreateFunction object according to Databricks SDK API
        # Based on OpenAPI spec: CreateFunction requires separate catalog_name, schema_name, name
        # and input_params must be wrapped in FunctionParameterInfos

        # Wrap input_params and return_params in FunctionParameterInfos structure
        input_params_wrapped = FunctionParameterInfos(parameters=input_params)
        return_params_wrapped = FunctionParameterInfos(parameters=return_params)

        # For EXTERNAL functions (Python UDTFs):
        # - return_params MUST be provided as structured metadata (since we use TABLE_TYPE)
        # - data_type must be "TABLE_TYPE"
        # - full_data_type must be the DDL string: "TABLE(col1 TYPE, col2 TYPE, ...)"
        # - routine_body must be "EXTERNAL"
        # - external_language must be "PYTHON"

        # Build CreateFunction with all required fields for Python UDTF
        create_function = CreateFunction(
            name=function_name,
            catalog_name=catalog,
            schema_name=schema,
            input_params=input_params_wrapped,
            return_params=return_params_wrapped,  # Structured metadata for output columns
            data_type=ColumnTypeName.TABLE_TYPE,
            full_data_type=return_type,  # DDL string: "TABLE(col1 TYPE, col2 TYPE, ...)"
            routine_body=CreateFunctionRoutineBody.EXTERNAL,
            routine_definition=udtf_code,  # The Python class with eval() method
            external_language="PYTHON",
            is_deterministic=False,
            comment=comment,
            # These 5 are required by some SDK version constructors:
            parameter_style=CreateFunctionParameterStyle.S,
            sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
            is_null_call=False,
            security_type=CreateFunctionSecurityType.DEFINER,
            specific_name=function_name,
        )

        # The API expects CreateFunctionRequest with function_info field
        if debug:
            print("[DEBUG] CreateFunction object built, calling workspace_client.functions.create()...")

        try:
            result = self.workspace_client.functions.create(function_info=create_function)
            if debug:
                print(f"[DEBUG] UDTF '{full_function_name}' registered successfully!")
            return result
        except ResourceAlreadyExists:
            if if_exists == "error":
                raise
            # If we get here and if_exists != "error", it means the function was created
            # between our check and create call (race condition)
            # Return the existing function
            if debug:
                print("[DEBUG] UDTF already exists (race condition), returning existing function")
            return self.workspace_client.functions.get(full_function_name)
        except (RuntimeError, ValueError, ResourceAlreadyExists) as e:
            print(f"\n[ERROR] Failed to create UDTF '{full_function_name}'")
            print("[ERROR] Input parameters sent:")
            for p in input_params:
                print(f"  - {p.name}: type_text='{p.type_text}', type_json='{p.type_json}', position={p.position}")
            print(f"[ERROR] Return type: {return_type}")
            print(f"[ERROR] Exception type: {type(e).__name__}")
            print(f"[ERROR] Exception message: {e}")
            raise

    def register_view(
        self,
        catalog: str,
        schema: str,
        view_name: str,
        view_sql: str,
        comment: str | None = None,
        warehouse_id: str | None = None,
        debug: bool = False,
    ) -> None:
        """Register a SQL View in Unity Catalog.

        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            view_name: View name
            view_sql: CREATE VIEW SQL statement
            comment: View description
            warehouse_id: Optional SQL warehouse ID. If None, will try to find a default warehouse.
                         You can get warehouse_id from Databricks UI or via:
                         workspace_client.warehouses.list() and find by name.
            debug: If True, prints detailed information about the view registration.
        """
        if debug:
            print(f"\n[DEBUG] === Registering View: {catalog}.{schema}.{view_name} ===")
            print(f"[DEBUG] Warehouse ID (input): {warehouse_id}")

        # Get warehouse_id - try to find default if not provided
        if warehouse_id is None:
            warehouse_id = self._get_default_warehouse_id()

        # Add comment to view SQL if provided
        # Note: SQL comments in CREATE VIEW are typically added as:
        # CREATE VIEW ... COMMENT 'comment text'
        # But this depends on your SQL dialect. For now, we'll just execute the provided SQL.
        # If comment is provided, it could be added to the SQL statement here if needed.

        if debug:
            print(f"[DEBUG] Warehouse ID (resolved): {warehouse_id}")
            print("[DEBUG] View SQL to execute:")
            print("-" * 40)
            print(view_sql)
            print("-" * 40)

        # Execute CREATE VIEW statement via Databricks SQL API
        try:
            if debug:
                print("[DEBUG] Calling statement_execution.execute_statement()...")

            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=view_sql,
                wait_timeout="30s",
            )

            if debug:
                print(f"[DEBUG] Response received: {type(response).__name__}")
                # Log response attributes for diagnosis
                print(f"[DEBUG] Response attributes: {[attr for attr in dir(response) if not attr.startswith('_')]}")

            # Check if execution was successful
            # Handle response status flexibly (may be string or enum)
            state = None
            try:
                if hasattr(response, "status") and response.status is not None and hasattr(response.status, "state"):
                    state = response.status.state
                    if debug:
                        print(f"[DEBUG] Got state from response.status.state: {state}")
                elif hasattr(response, "state"):
                    state = response.state
                    if debug:
                        print(f"[DEBUG] Got state from response.state: {state}")
            except (AttributeError, KeyError) as state_error:
                if debug:
                    print(f"[DEBUG] Error extracting state: {state_error}")
                    print(f"[DEBUG] State extraction error type: {type(state_error).__name__}")

            # Convert state to string for comparison
            state_str = str(state).upper() if state else None

            if debug:
                print(f"[DEBUG] Statement state: {state_str}")

            # Extract error message if available - with comprehensive error handling
            error_message = None
            error_details = {}
            statement_id = None
            try:
                # Try to get statement_id for further diagnosis
                if hasattr(response, "statement_id"):
                    statement_id = response.statement_id
                    if debug:
                        print(f"[DEBUG] Statement ID: {statement_id}")

                if hasattr(response, "status") and response.status is not None:
                    if debug and hasattr(response.status, "__dict__"):
                        attrs = [attr for attr in dir(response.status) if not attr.startswith("_")]
                        print(f"[DEBUG] response.status attributes: {attrs}")

                    if hasattr(response.status, "error") and response.status.error is not None:
                        error_obj = response.status.error
                        if debug:
                            print("[DEBUG] Response status has 'error' attribute")
                            if hasattr(error_obj, "__dict__"):
                                attrs = [attr for attr in dir(error_obj) if not attr.startswith("_")]
                                print(f"[DEBUG] response.status.error attributes: {attrs}")

                        if hasattr(error_obj, "message"):
                            error_message = error_obj.message
                            if debug:
                                print(f"[DEBUG] Extracted error message: {error_message}")
                        if hasattr(error_obj, "error_code"):
                            error_details["error_code"] = error_obj.error_code
                        if hasattr(error_obj, "error_type"):
                            error_details["error_type"] = error_obj.error_type
                        if hasattr(error_obj, "message_parameters"):
                            error_details["message_parameters"] = error_obj.message_parameters
                elif hasattr(response, "error"):
                    if debug:
                        print("[DEBUG] Response has 'error' attribute")
                    if hasattr(response.error, "message"):
                        error_message = response.error.message
            except (AttributeError, KeyError) as parse_error:
                # Error message extraction failed (might be empty response)
                if debug:
                    print(f"[DEBUG] Could not extract error message from response: {parse_error}")
                    print(f"[DEBUG] Error extraction exception type: {type(parse_error).__name__}")
                    import traceback

                    print(f"[DEBUG] Error extraction traceback:\n{traceback.format_exc()}")
                error_message = None

            if state_str and "SUCCEEDED" in state_str:
                if debug:
                    print(f"[DEBUG] View '{catalog}.{schema}.{view_name}' created successfully!")
                return
            elif state_str and "FAILED" in state_str:
                # Log comprehensive failure information
                if debug:
                    print("[DEBUG] === VIEW CREATION FAILED ===")
                    print(f"[DEBUG] Error message: {error_message}")
                    print(f"[DEBUG] Error details: {error_details}")
                    print(f"[DEBUG] Statement ID: {statement_id}")
                    if statement_id:
                        # Try to get more details using statement_id
                        try:
                            statement_info = self.workspace_client.statement_execution.get_statement(statement_id)
                            if debug:
                                print("[DEBUG] Statement info from get_statement:")
                                print(f"[DEBUG]   - Type: {type(statement_info).__name__}")
                                if hasattr(statement_info, "status") and statement_info.status is not None:
                                    print(f"[DEBUG]   - Status: {statement_info.status}")
                                    if (
                                        hasattr(statement_info.status, "error")
                                        and statement_info.status.error is not None
                                    ):
                                        print(f"[DEBUG]   - Status error: {statement_info.status.error}")
                                if hasattr(statement_info, "__dict__"):
                                    attrs = [attr for attr in dir(statement_info) if not attr.startswith("_")]
                                    print(f"[DEBUG]   - Attributes: {attrs}")
                        except (NotFound, RuntimeError, ValueError) as get_error:
                            if debug:
                                print(f"[DEBUG] Could not get statement details: {get_error}")
                                print(f"[DEBUG] get_statement error type: {type(get_error).__name__}")

                # Even if state is FAILED, verify if the view actually exists
                # This handles cases where SECRET() causes a security warning but view is still created
                try:
                    # Try to get the view to verify it exists
                    full_view_name = f"{catalog}.{schema}.{view_name}"
                    if debug:
                        print(f"[DEBUG] Checking if view exists: {full_view_name}")
                    existing_view = self.workspace_client.tables.get(full_view_name)
                    if existing_view and existing_view.table_type == "VIEW":
                        if debug:
                            print("[DEBUG] View state was FAILED but view exists - treating as success")
                            if error_message:
                                print(f"[DEBUG] Error message (may be security warning): {error_message}")
                        return
                except NotFound as view_check_error:
                    if debug:
                        print("[DEBUG] View does not exist (checked via tables.get)")
                        print(f"[DEBUG] View check error: {view_check_error}")
                        print(f"[DEBUG] View check error type: {type(view_check_error).__name__}")
                    pass

                # View doesn't exist, so raise the error
                error_msg = error_message or "Unknown error"
                if error_details:
                    error_msg += f" (Details: {error_details})"
                if statement_id:
                    error_msg += f" (Statement ID: {statement_id})"
                raise RuntimeError(f"Failed to create view {catalog}.{schema}.{view_name}: {error_msg}")
            elif state_str:
                # State might be PENDING, RUNNING, etc.
                raise RuntimeError(f"View creation did not complete. State: {state_str}")
            else:
                # If we can't determine state but no exception was raised, verify view exists
                from databricks.sdk.errors import NotFound


                try:
                    full_view_name = f"{catalog}.{schema}.{view_name}"
                    existing_view = self.workspace_client.tables.get(full_view_name)
                    if existing_view and existing_view.table_type == "VIEW":
                        if debug:
                            print("[DEBUG] No state returned but view exists - assuming success")
                        return
                except NotFound:
                    pass

                if debug:
                    print("[DEBUG] No state returned, assuming success")
                return

        except (RuntimeError, ValueError, AttributeError) as e:
            # Special handling for "end-of-input" which often means empty response from SQL Warehouse
            # This is common for DDL statements like CREATE VIEW when they succeed immediately.
            from databricks.sdk.errors import NotFound

            error_str = str(e)
            if "end-of-input" in error_str or "No content to map" in error_str:
                # Even with end-of-input, verify view exists
                # DDL statements often return empty responses even on success
                try:
                    full_view_name = f"{catalog}.{schema}.{view_name}"
                    existing_view = self.workspace_client.tables.get(full_view_name)
                    if existing_view and existing_view.table_type == "VIEW":
                        if debug:
                            print("[DEBUG] Received empty response but view exists - treating as success")
                        return
                except NotFound:
                    pass

                # View doesn't exist - this is a real error
                # Wait briefly in case view creation is still in progress
                import time

                time.sleep(0.5)

                # Check one more time
                try:
                    full_view_name = f"{catalog}.{schema}.{view_name}"
                    existing_view = self.workspace_client.tables.get(full_view_name)
                    if existing_view and existing_view.table_type == "VIEW":
                        if debug:
                            print("[DEBUG] View exists after brief wait - treating as success")
                        return
                except NotFound:
                    pass

                # View still doesn't exist - this is a real failure
                print(f"\n[ERROR] Received 'end-of-input' error when creating view '{catalog}.{schema}.{view_name}'")
                print("[ERROR] View does not exist after error, indicating CREATE VIEW statement failed")
                print(f"[ERROR] Warehouse ID: {warehouse_id}")
                print("[ERROR] View SQL:")
                print("-" * 40)
                print(view_sql)
                print("-" * 40)
                raise RuntimeError(
                    f"Failed to create view {catalog}.{schema}.{view_name}. "
                    f"Received 'end-of-input' error and view does not exist. "
                    f"Original error: {e}"
                ) from e

            # Enhanced error message with full context
            print(f"\n[ERROR] Failed to register view '{catalog}.{schema}.{view_name}'")
            print(f"[ERROR] Warehouse ID: {warehouse_id}")
            print("[ERROR] View SQL:")
            print("-" * 40)
            print(view_sql)
            print("-" * 40)
            print(f"[ERROR] Exception type: {type(e).__name__}")
            print(f"[ERROR] Exception message: {e}")
            raise RuntimeError(
                f"Failed to register view {catalog}.{schema}.{view_name} using warehouse {warehouse_id}. "
                f"Original error: {e}"
            ) from e

    def register_foreign_key_constraint(
        self,
        catalog: str,
        schema: str,
        view_name: str,
        column_name: str,
        referenced_catalog: str,
        referenced_schema: str,
        referenced_view: str,
        referenced_column: str = "external_id",  # Default to external_id for CDF nodes
        constraint_name: str | None = None,
        warehouse_id: str | None = None,
        debug: bool = False,
    ) -> None:
        """Register an informational foreign key constraint on a view.

        Informational constraints (NOT ENFORCED) document relationships between views
        where a STRING column (external_id) references another view's external_id column.

        This is useful for documenting DirectRelation and MultiReverseDirectRelation
        properties that reference other nodes/edges in the data model.

        Args:
            catalog: Catalog containing the view
            schema: Schema containing the view
            view_name: Name of the view to add constraint to
            column_name: Column name in the view (the foreign key)
            referenced_catalog: Catalog containing the referenced view
            referenced_schema: Schema containing the referenced view
            referenced_view: Name of the referenced view (the primary key table)
            referenced_column: Column name in referenced view (default: "external_id")
            constraint_name: Optional constraint name. If None, auto-generates:
                           f"fk_{view_name}_{column_name}"
            warehouse_id: Optional SQL warehouse ID. If None, auto-detects.
            debug: If True, prints SQL being executed

        Example:
            # Document that user_id in MyView references external_id in Users view
            registry.register_foreign_key_constraint(
                catalog="main",
                schema="my_schema",
                view_name="MyView",
                column_name="user_id",
                referenced_catalog="main",
                referenced_schema="my_schema",
                referenced_view="Users",
            )
        """
        if warehouse_id is None:
            warehouse_id = self._get_default_warehouse_id()

        if constraint_name is None:
            constraint_name = f"fk_{view_name}_{column_name}"

        # Build ALTER VIEW statement with foreign key constraint
        # Format: ALTER VIEW catalog.schema.view_name
        #         ADD CONSTRAINT constraint_name
        #         FOREIGN KEY (column_name) REFERENCES catalog.schema.referenced_view(referenced_column) NOT ENFORCED
        constraint_sql = (
            f"ALTER VIEW {catalog}.{schema}.{view_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({column_name}) "
            f"REFERENCES {referenced_catalog}.{referenced_schema}.{referenced_view}({referenced_column}) "
            f"NOT ENFORCED"
        )

        if debug:
            print("[DEBUG] Adding foreign key constraint:")
            print(f"[DEBUG] {constraint_sql}")

        try:
            self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=constraint_sql,
                wait_timeout="30s",
            )

            if debug:
                print(f"[DEBUG] Foreign key constraint '{constraint_name}' added successfully")

        except (RuntimeError, ValueError) as e:
            # Foreign key constraints are informational - log but don't fail registration
            if debug:
                print(f"[WARNING] Failed to add foreign key constraint '{constraint_name}': {e}")
            # Don't raise - constraints are optional metadata
