"""UDTFRegistry - Utility for registering Python UDTFs in Unity Catalog."""

from __future__ import annotations

import time
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

    def _repair_udtf_metadata(
        self,
        full_function_name: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        debug: bool = False,
    ) -> bool:
        """Repair corrupted type_json metadata for a UDTF that was registered via SQL.

        This function fixes the "Zombie Metadata" issue where SQL registration
        generates empty/corrupt type_json, causing "end-of-input" errors.

        Args:
            full_function_name: Full function name (catalog.schema.function)
            input_params: Original input parameters (to get type_text)
            return_type: Return type DDL string (e.g., "TABLE(col1 STRING, ...)")
            debug: If True, prints detailed information

        Returns:
            True if repair succeeded, False otherwise
        """
        import json

        from databricks.sdk.service.catalog import (
            ColumnTypeName,
            FunctionParameterInfo,
            FunctionParameterInfos,
            UpdateFunction,
        )

        try:
            # Get the function (metadata is retrievable via SDK even if Spark fails)
            func = self.workspace_client.functions.get(full_function_name)

            # Helper to map SQL types to valid DataType JSON strings
            def get_clean_json(param_type_text: str) -> str:
                """Convert SQL type text to DataType JSON format."""
                pt = param_type_text.upper()
                if "ARRAY" in pt:
                    # Generic Array<String> JSON
                    return json.dumps({"type": "array", "elementType": "string", "containsNull": True})
                elif "STRING" in pt:
                    return '"string"'
                elif "INT" in pt or "INTEGER" in pt:
                    return '"long"'
                elif "DOUBLE" in pt:
                    return '"double"'
                elif "BOOLEAN" in pt:
                    return '"boolean"'
                elif "TIMESTAMP" in pt:
                    return '"timestamp"'
                elif "LONG" in pt:
                    return '"long"'
                else:
                    # Fallback: Default to string if unknown
                    return '"string"'

            # Rebuild Inputs - use original input_params to preserve parameter_default values
            fixed_inputs: list[FunctionParameterInfo] = []

            # Create a lookup map from the function's current parameters (for reference)
            func_param_map = {}
            if func.input_params and func.input_params.parameters:
                for p in func.input_params.parameters:
                    func_param_map[p.name] = p

            # Use original input_params to preserve parameter_default and other metadata
            for orig_param in input_params:
                # Use original parameter but fix type_json if needed
                fixed_inputs.append(
                    FunctionParameterInfo(
                        name=orig_param.name,
                        type_name=orig_param.type_name,
                        type_text=orig_param.type_text,
                        type_json=get_clean_json(orig_param.type_text),  # Fix type_json
                        position=orig_param.position,
                        parameter_mode=orig_param.parameter_mode,
                        parameter_type=orig_param.parameter_type,
                        parameter_default=orig_param.parameter_default,  # CRITICAL: Preserve default value
                    )
                )

            # Rebuild Returns
            fixed_returns: list[FunctionParameterInfo] = []
            if func.return_params and func.return_params.parameters:
                for p in func.return_params.parameters:
                    fixed_returns.append(
                        FunctionParameterInfo(
                            name=p.name,
                            type_name=p.type_name or ColumnTypeName.STRING,
                            type_text=p.type_text or "STRING",
                            type_json=get_clean_json(p.type_text or "STRING"),
                            position=p.position,
                            parameter_mode=p.parameter_mode,
                            parameter_type=p.parameter_type,
                        )
                    )

            # Push Update
            function_name_only = full_function_name.split(".")[-1]  # Just function name, not full name
            update_function = UpdateFunction(
                name=function_name_only,
                input_params=FunctionParameterInfos(parameters=fixed_inputs) if fixed_inputs else None,
                return_params=FunctionParameterInfos(parameters=fixed_returns) if fixed_returns else None,
            )
            self.workspace_client.functions.update(
                name=full_function_name,
                function_info=update_function,
            )

            if debug:
                print(f"[DEBUG] ✓ Metadata repaired for {full_function_name}")
            return True

        except Exception as e:
            if debug:
                print(f"[DEBUG] ⚠ Failed to repair metadata for {full_function_name}: {e}")
            return False

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

    def register_udtf_via_sql(
        self,
        catalog: str,
        schema: str,
        function_name: str,
        udtf_code: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        handler_name: str,
        warehouse_id: str | None = None,
        comment: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
    ) -> None:
        """Register a Python UDTF in Unity Catalog using SQL CREATE FUNCTION.

        This method is serverless-compatible and registers a persistent UDTF
        via the SQL Statement Execution API.
        """

        full_function_name = f"{catalog}.{schema}.{function_name}"
        warehouse_id = warehouse_id or self._get_default_warehouse_id()

        def _format_param(param: FunctionParameterInfo) -> str:
            return f"{param.name} {param.type_text}"

        param_sql = ", ".join(_format_param(p) for p in input_params)
        create_keyword = "CREATE OR REPLACE" if if_exists == "replace" else "CREATE"
        if if_exists == "skip":
            create_keyword = "CREATE IF NOT EXISTS"

        sql = (
            f"{create_keyword} FUNCTION {full_function_name}({param_sql})\n"
            f"RETURNS {return_type}\n"
            "LANGUAGE PYTHON\n"
            f"COMMENT '{comment or ''}'\n"
            f"HANDLER '{handler_name}'\n"
            "AS $$\n"
            f"{udtf_code}\n"
            "$$"
        )

        if debug:
            print(f"[DEBUG] SQL registration statement for {full_function_name}:\n{sql}")

        statement = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        statement_id = statement.statement_id

        # Wait for completion with timeout
        start_time = time.time()
        timeout_seconds = 300
        while True:
            result = self.workspace_client.statement_execution.get_statement(statement_id)
            state = result.status.state
            state_str = str(state).upper() if state is not None else ""
            if debug:
                print(f"[DEBUG] Statement {statement_id} state: {state}")
            if any(keyword in state_str for keyword in ("SUCCEEDED", "FAILED", "CANCELED")):
                if debug:
                    print(f"[INFO] SQL registration state: {state}")
                if "SUCCEEDED" not in state_str:
                    error_message = None
                    error_code = None
                    if hasattr(result, "status") and result.status is not None:
                        error_obj = getattr(result.status, "error", None)
                        if error_obj is not None:
                            error_message = getattr(error_obj, "message", None)
                            error_code = getattr(error_obj, "error_code", None)
                    raise RuntimeError(
                        f"Failed to register UDTF via SQL: {full_function_name} "
                        f"(state={state_str}, error_code={error_code}, message={error_message})"
                    )
                break
            if time.time() - start_time > timeout_seconds:
                raise TimeoutError(
                    f"Timed out waiting for SQL registration: {full_function_name} (last state: {state})"
                )
            time.sleep(1)

    def _create_sql_wrapper(
        self,
        catalog: str,
        schema: str,
        function_name: str,
        internal_function_name: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        warehouse_id: str | None = None,
        comment: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
    ) -> None:
        """Create a SQL wrapper function for a UDTF (serverless-compatible)."""
        warehouse_id = warehouse_id or self._get_default_warehouse_id()
        full_function_name = f"{catalog}.{schema}.{function_name}"
        internal_function_name = f"{catalog}.{schema}.{internal_function_name}"

        create_keyword = "CREATE OR REPLACE" if if_exists == "replace" else "CREATE"
        if if_exists == "skip":
            create_keyword = "CREATE IF NOT EXISTS"

        def _format_param_with_default(param: FunctionParameterInfo) -> str:
            default_clause = " DEFAULT NULL" if param.parameter_default == "NULL" else ""
            return f"{param.name} {param.type_text}{default_clause}"

        param_sql = ", ".join(_format_param_with_default(p) for p in input_params)
        arg_sql = ", ".join(f"{p.name} => {p.name}" for p in input_params)

        sql = (
            f"{create_keyword} FUNCTION {full_function_name}({param_sql})\n"
            f"RETURNS {return_type}\n"
            "LANGUAGE SQL\n"
            f"COMMENT '{comment or ''}'\n"
            f"RETURN SELECT * FROM {internal_function_name}({arg_sql})"
        )

        if debug:
            print(f"[DEBUG] SQL wrapper statement for {full_function_name}:\n{sql}")

        statement = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        statement_id = statement.statement_id
        start_time = time.time()
        timeout_seconds = 300
        while True:
            result = self.workspace_client.statement_execution.get_statement(statement_id)
            state = result.status.state
            state_str = str(state).upper() if state is not None else ""
            if debug:
                print(f"[DEBUG] Statement {statement_id} state: {state}")
            if any(keyword in state_str for keyword in ("SUCCEEDED", "FAILED", "CANCELED")):
                if debug:
                    print(f"[INFO] SQL wrapper registration state: {state}")
                if "SUCCEEDED" not in state_str:
                    error_message = None
                    error_code = None
                    if hasattr(result, "status") and result.status is not None:
                        error_obj = getattr(result.status, "error", None)
                        if error_obj is not None:
                            error_message = getattr(error_obj, "message", None)
                            error_code = getattr(error_obj, "error_code", None)
                    raise RuntimeError(
                        f"Failed to create SQL wrapper: {full_function_name} "
                        f"(state={state_str}, error_code={error_code}, message={error_message})"
                    )
                break
            if time.time() - start_time > timeout_seconds:
                raise TimeoutError(
                    f"Timed out waiting for SQL wrapper registration: {full_function_name} (last state: {state})"
                )
            time.sleep(1)

    def register_udtf(
        self,
        catalog: str,
        schema: str,
        function_name: str,
        udtf_code: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        return_params: list[FunctionParameterInfo] | None = None,  # Required: Structured return columns for UDTFs
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
            return_params: Required structured metadata for the UDTF output columns.
                        Unity Catalog requires return_params to be populated for UDTFs.
                        If None or empty, a ValueError will be raised.
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
            - return_params is required and must be populated (Unity Catalog requirement)
            - routine_body must be EXTERNAL
            - external_language must be PYTHON
            - Generated UDTFs use direct REST API calls (no external dependencies required)
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
            if return_params:
                print(f"[DEBUG] Return columns ({len(return_params)}):")
                for p in return_params:
                    print(f"  - name={p.name}, position={p.position}, type_text='{p.type_text}'")
            else:
                print("[DEBUG] Return columns: None (using full_data_type only)")
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

        # Wrap input_params in FunctionParameterInfos structure
        input_params_wrapped = FunctionParameterInfos(parameters=input_params)

        # Validate and wrap return_params
        # Unity Catalog requires return_params to be populated for UDTFs (cannot be None or empty)
        if not return_params:
            raise ValueError(
                "return_params is required for UDTF registration but was None or empty. "
                "This indicates a bug in the generator - return_params should always be "
                "parsed from the UDTF class or view."
            )
        return_params_wrapped = FunctionParameterInfos(parameters=return_params)

        # For EXTERNAL functions (Python UDTFs):
        # - return_params is required by Unity Catalog API (even if empty)
        # - data_type must be "TABLE_TYPE"
        # - full_data_type must be the DDL string: "TABLE(col1 TYPE, col2 TYPE, ...)"
        # - routine_body must be "EXTERNAL"
        # - external_language must be "PYTHON"

        # Build CreateFunction with all required fields for Python UDTF
        create_function_kwargs = {
            "name": function_name,
            "catalog_name": catalog,
            "schema_name": schema,
            "input_params": input_params_wrapped,
            "return_params": return_params_wrapped,  # Always include (required by Unity Catalog API)
            "data_type": ColumnTypeName.TABLE_TYPE,
            "full_data_type": return_type,  # DDL string: "TABLE(col1 TYPE, col2 TYPE, ...)"
            "routine_body": CreateFunctionRoutineBody.EXTERNAL,
            "routine_definition": udtf_code,  # The Python class with eval() method
            "external_language": "PYTHON",
            "is_deterministic": False,
            "comment": comment,
            # These 5 are required by some SDK version constructors:
            "parameter_style": CreateFunctionParameterStyle.S,
            "sql_data_access": CreateFunctionSqlDataAccess.NO_SQL,
            "is_null_call": False,
            "security_type": CreateFunctionSecurityType.DEFINER,
            "specific_name": function_name,
        }

        # Patch as_dict() to ensure "parameters" field is always included in serialization
        # (SDK returns {} when parameters=[], but Unity Catalog requires {"parameters": [...]})
        original_as_dict = return_params_wrapped.as_dict

        def patched_as_dict() -> dict:
            """Patched as_dict() that always includes 'parameters' field."""
            result = original_as_dict()
            # Ensure "parameters" field is always present (Unity Catalog requirement)
            if "parameters" not in result:
                if return_params_wrapped.parameters is not None:
                    result["parameters"] = [
                        p.as_dict() if hasattr(p, "as_dict") else p for p in return_params_wrapped.parameters
                    ]
                else:
                    result["parameters"] = []
            return result

        return_params_wrapped.as_dict = patched_as_dict  # type: ignore[method-assign]

        # Debug output
        if debug:
            print(f"[DEBUG] Including {len(return_params)} return_params in CreateFunction")
            # Debug: Check what will be serialized
            return_params_dict = return_params_wrapped.as_dict()
            print(f"[DEBUG] return_params_wrapped.as_dict() = {return_params_dict}")
            print(f"[DEBUG] return_params_wrapped.parameters = {return_params_wrapped.parameters}")

        create_function = CreateFunction(**create_function_kwargs)  # type: ignore[call-overload]

        # Debug: Check what CreateFunction will serialize
        if debug:
            create_function_dict = create_function.as_dict()
            print(f"[DEBUG] CreateFunction.as_dict() includes return_params: {'return_params' in create_function_dict}")
            if "return_params" in create_function_dict:
                print(f"[DEBUG] CreateFunction.as_dict()['return_params'] = {create_function_dict['return_params']}")
            else:
                print("[DEBUG] WARNING: return_params is missing from CreateFunction.as_dict()!")
                print(f"[DEBUG] Full CreateFunction.as_dict() keys: {list(create_function_dict.keys())}")
                print(f"[DEBUG] create_function.return_params object: {create_function.return_params}")
                print(f"[DEBUG] create_function.return_params truthy? {bool(create_function.return_params)}")

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
        except (RuntimeError, ValueError) as e:
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

                # Check if error message indicates empty response issue
                # Even if state is FAILED, verify if the view actually exists
                # This handles cases where SECRET() causes a security warning but view is still created
                # OR where the API returns empty response but view is created
                error_str = (error_message or "").lower()
                is_empty_response_error = (
                    "end-of-input" in error_str
                    or "no content to map" in error_str
                    or "bad_request" in str(error_details.get("error_code", "")).lower()
                )

                if is_empty_response_error:
                    if debug:
                        print("[DEBUG] Detected empty response error")
                        print("[DEBUG] Verifying if view was created despite error...")

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

                # Add helpful message for empty response errors
                if is_empty_response_error:
                    error_msg += (
                        "\nThis may be a SQL statement execution API issue. "
                        "Please verify the SQL statement manually or check Unity Catalog API status."
                    )

                raise RuntimeError(f"Failed to create view {catalog}.{schema}.{view_name}: {error_msg}")
            elif state_str:
                # State might be PENDING, RUNNING, etc.
                raise RuntimeError(f"View creation did not complete. State: {state_str}")
            else:
                # If we can't determine state but no exception was raised, verify view exists
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
            # NOTE: NotFound is already imported at module level (line 8)

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
