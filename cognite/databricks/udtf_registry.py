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
            UpdateFunction,
            FunctionParameterInfos,
            FunctionParameterInfo,
            ColumnTypeName,
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
                    return json.dumps({
                        "type": "array",
                        "elementType": "string",
                        "containsNull": True
                    })
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
                fixed_inputs.append(FunctionParameterInfo(
                    name=orig_param.name,
                    type_name=orig_param.type_name,
                    type_text=orig_param.type_text,
                    type_json=get_clean_json(orig_param.type_text),  # Fix type_json
                    position=orig_param.position,
                    parameter_mode=orig_param.parameter_mode,
                    parameter_type=orig_param.parameter_type,
                    parameter_default=orig_param.parameter_default,  # CRITICAL: Preserve default value
                ))

            # Rebuild Returns
            fixed_returns: list[FunctionParameterInfo] = []
            if func.return_params and func.return_params.parameters:
                for p in func.return_params.parameters:
                    fixed_returns.append(FunctionParameterInfo(
                        name=p.name,
                        type_name=p.type_name or ColumnTypeName.STRING,
                        type_text=p.type_text or "STRING",
                        type_json=get_clean_json(p.type_text or "STRING"),
                        position=p.position,
                        parameter_mode=p.parameter_mode,
                        parameter_type=p.parameter_type,
                    ))

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
        warehouse_id: str | None = None,
        comment: str | None = None,
        if_exists: str = "skip",
        debug: bool = False,
    ) -> None:
        """Register a Python UDTF in Unity Catalog using SQL CREATE FUNCTION.
        
        This approach uses explicit RETURNS TABLE to bypass Unity Catalog introspection,
        avoiding the "end-of-input" JSON parsing errors that occur with SDK API registration.
        
        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            function_name: UDTF function name
            udtf_code: Python code for the UDTF class (must include eval() method)
            input_params: Function parameter definitions
            return_type: Return type DDL string (e.g., "TABLE(col1 STRING, col2 INT)")
            warehouse_id: SQL warehouse ID (required for SQL execution). If None, uses default.
            comment: Function description
            if_exists: What to do if function already exists: "skip", "replace", "error"
            debug: If True, prints detailed information
            
        Raises:
            RuntimeError: If SQL execution fails
            ValueError: If if_exists value is invalid
        """
        from databricks.sdk.errors import NotFound, ResourceAlreadyExists
        import ast
        
        full_function_name = f"{catalog}.{schema}.{function_name}"
        
        # Extract UDTF class name from code using AST parsing
        udtf_class_name: str | None = None
        try:
            tree = ast.parse(udtf_code)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # Check if this class has eval method (required for UDTF)
                    has_eval = any(
                        isinstance(item, ast.FunctionDef) and item.name == "eval"
                        for item in node.body
                    )
                    if has_eval:
                        udtf_class_name = node.name
                        break
        except SyntaxError as e:
            if debug:
                print(f"[DEBUG] Warning: Could not parse UDTF code to extract class name: {e}")
        
        if not udtf_class_name:
            raise ValueError(
                f"Could not extract UDTF class name from code. "
                "Expected a class with 'eval' method."
            )
        
        # Get warehouse_id - try to find default if not provided
        if warehouse_id is None:
            warehouse_id = self._get_default_warehouse_id()
        
        # Register Python UDTF with _internal suffix
        # The public-facing function will be a SQL wrapper with DEFAULT NULL
        internal_function_name = f"{function_name}_internal"
        full_internal_function_name = f"{catalog}.{schema}.{internal_function_name}"
        
        # Handle if_exists for SQL approach
        # Drop both internal and wrapper functions if replacing
        if if_exists == "replace":
            # Drop existing functions first using SDK API (more reliable than SQL)
            for func_name_to_drop in [full_internal_function_name, full_function_name]:
                try:
                    if debug:
                        print(f"[DEBUG] Dropping existing function: {func_name_to_drop}")
                    self.workspace_client.functions.delete(func_name_to_drop)
                    # Wait for deletion to complete
                    import time
                    time.sleep(0.5)
                    
                    # Verify function was actually deleted
                    max_retries = 3
                    for retry in range(max_retries):
                        try:
                            self.workspace_client.functions.get(func_name_to_drop)
                            # Function still exists, wait a bit longer
                            if retry < max_retries - 1:
                                time.sleep(0.5)
                            else:
                                if debug:
                                    print(f"[DEBUG] Warning: Function still exists after drop attempt")
                        except NotFound:
                            # Function successfully deleted
                            if debug:
                                print(f"[DEBUG] Function successfully deleted")
                            break
                except NotFound:
                    # Function doesn't exist, which is fine
                    if debug:
                        print(f"[DEBUG] Function does not exist, skipping drop")
                except Exception as e:
                    if debug:
                        print(f"[DEBUG] Error dropping function (may not exist): {e}")
                    # Continue anyway - CREATE OR REPLACE will handle it
        elif if_exists == "skip":
            # Check if wrapper function exists (if it exists, internal probably does too)
            try:
                self.workspace_client.functions.get(full_function_name)
                if debug:
                    print(f"[DEBUG] Function {full_function_name} already exists, skipping")
                return
            except NotFound:
                pass  # Function doesn't exist, proceed
        elif if_exists == "error":
            # Check if wrapper function exists and raise error if it does
            try:
                self.workspace_client.functions.get(full_function_name)
                raise ResourceAlreadyExists(
                    f"Routine or Model '{function_name}' already exists in {catalog}.{schema}"
                )
            except NotFound:
                pass  # Function doesn't exist, proceed
        else:
            raise ValueError(f"Invalid if_exists value: {if_exists}. Must be 'skip', 'replace', or 'error'")
        
        # Build input parameters SQL string
        # NOTE: Spark SQL does NOT support DEFAULT values in CREATE FUNCTION for Python UDTFs
        # We must set parameter_default via SDK API after registration instead
        input_params_sql = []
        for param in input_params:
            # Format: param_name TYPE (no DEFAULT clause - not supported in SQL for Python functions)
            param_sql = f"{param.name} {param.type_text}"
            input_params_sql.append(param_sql)
        
        input_params_str = ", ".join(input_params_sql)
        
        # Use standard dollar-quoting ($$) for Spark SQL
        # Spark SQL doesn't support named dollar-quoting ($tag$)
        # Check if code contains $$ delimiter (unlikely but possible)
        if "$$" in udtf_code:
            # Fall back to single quotes with SQL escaping if $$ is found in code
            escaped_udtf_code = udtf_code.replace("'", "''")
            create_function_sql = f"""CREATE OR REPLACE FUNCTION {full_internal_function_name}({input_params_str})
RETURNS {return_type}
LANGUAGE PYTHON
HANDLER '{udtf_class_name}'
AS '{escaped_udtf_code}'"""
        else:
            # Use standard $$ delimiter (Spark SQL compatible)
            create_function_sql = f"""CREATE OR REPLACE FUNCTION {full_internal_function_name}({input_params_str})
RETURNS {return_type}
LANGUAGE PYTHON
HANDLER '{udtf_class_name}'
AS $$
{udtf_code}
$$"""
        
        if debug:
            print(f"\n[DEBUG] === Registering internal Python UDTF via SQL: {full_internal_function_name} ===")
            print(f"[DEBUG] Warehouse ID: {warehouse_id}")
            print(f"[DEBUG] UDTF Class Name: {udtf_class_name}")
            print(f"[DEBUG] Input parameters ({len(input_params)}): {input_params_str}")
            print(f"[DEBUG] Return type: {return_type}")
            print(f"[DEBUG] SQL Statement:")
            print("-" * 40)
            print(create_function_sql)
            print("-" * 40)
        
        # Execute SQL statement to register internal Python UDTF
        try:
            if debug:
                print("[DEBUG] Calling statement_execution.execute_statement()...")
            
            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=create_function_sql,
                wait_timeout="30s",
            )
            
            # Check response status (similar to register_view)
            state = None
            if hasattr(response, "status") and response.status is not None and hasattr(response.status, "state"):
                state = response.status.state
            elif hasattr(response, "state"):
                state = response.state
            
            state_str = str(state).upper() if state else None
            
            # ALWAYS log the state (not just in debug mode) to diagnose issues
            print(f"[INFO] SQL registration state: {state_str}")
            
            if debug:
                print(f"[DEBUG] Statement state: {state_str}")
            
            # Extract error message if available
            error_message = None
            if hasattr(response, "status") and response.status is not None:
                if hasattr(response.status, "error") and response.status.error is not None:
                    error_obj = response.status.error
                    if hasattr(error_obj, "message"):
                        error_message = error_obj.message
            
            if state_str and "SUCCEEDED" in state_str:
                if debug:
                    print(f"[DEBUG] Internal Python UDTF '{full_internal_function_name}' registered successfully via SQL!")
                
                # ALWAYS log that we're attempting to patch (not just in debug mode)
                print(f"[INFO] Patching function metadata for '{full_internal_function_name}'...")
                
                # CRITICAL: Patch metadata via SDK API to prevent introspection failures
                # Unity Catalog may have failed to populate return_params during SQL registration,
                # leaving corrupted metadata that causes "end-of-input" errors at execution time.
                # We manually populate return_params to bypass this bug.
                try:
                    if debug:
                        print(f"[DEBUG] Patching function metadata via SDK API to prevent introspection failures...")
                    
                    # Get the function we just created
                    # This may fail with "end-of-input" if metadata is corrupted (Zombie Metadata bug)
                    try:
                        function_info = self.workspace_client.functions.get(full_internal_function_name)
                    except Exception as get_error:
                        error_str = str(get_error)
                        # Check if this is the specific "Zombie Metadata" error
                        if "end-of-input" in error_str or "JsonMappingException" in error_str or "No content to map" in error_str:
                            print(f"[WARN] {full_internal_function_name} hit metadata bug during read-back. Attempting auto-repair...")
                            
                            # CALL THE REPAIR FUNCTION
                            if self._repair_udtf_metadata(full_internal_function_name, input_params, return_type, debug=debug):
                                print(f"[INFO] ✓ {full_internal_function_name} registered and repaired.")
                                # Retry getting the function info after repair
                                import time
                                time.sleep(0.3)  # Brief wait for update to propagate
                                function_info = self.workspace_client.functions.get(full_internal_function_name)
                            else:
                                print(f"[ERROR] {full_function_name} registered but repair failed.")
                                # Continue anyway - function is registered, just metadata is broken
                                # The rest of the patch logic will try to fix it
                                raise get_error  # Re-raise to trigger the outer exception handler
                        else:
                            # It was some other real error
                            raise
                    
                    # Parse return_type to build return_params with structured metadata
                    from databricks.sdk.service.catalog import (
                        UpdateFunction,
                        FunctionParameterInfos,
                        FunctionParameterInfo,
                        FunctionParameterMode,
                        FunctionParameterType,
                        ColumnTypeName,
                    )
                    from cognite.databricks.type_converter import TypeConverter
                    from pyspark.sql.types import StringType, LongType, DoubleType, BooleanType, TimestampType, ArrayType
                    import re
                    
                    # CRITICAL: Patch input_params first
                    # Unity Catalog introspects Python code and generates StructField format
                    # We need to convert them to DataType format to prevent "end-of-input" errors
                    input_params_list = []
                    for input_param in input_params:
                        try:
                            # Get the Spark type from type_text
                            type_text_upper = input_param.type_text.upper()
                            if type_text_upper.startswith('ARRAY<'):
                                inner_match = re.match(r'ARRAY<(\w+)>', type_text_upper)
                                if inner_match:
                                    inner_type = inner_match.group(1)
                                    if inner_type == 'STRING':
                                        spark_type = ArrayType(StringType())
                                    else:
                                        spark_type = ArrayType(StringType())
                                else:
                                    spark_type = ArrayType(StringType())
                            elif type_text_upper == 'STRING':
                                spark_type = StringType()
                            elif type_text_upper == 'INT':
                                spark_type = LongType()
                            elif type_text_upper == 'DOUBLE':
                                spark_type = DoubleType()
                            elif type_text_upper == 'BOOLEAN':
                                spark_type = BooleanType()
                            else:
                                spark_type = StringType()
                            
                            # Generate DataType JSON (quoted string like '"string"', not StructField object)
                            type_json_value = TypeConverter.spark_to_datatype_json(spark_type)
                            
                            # CRITICAL: Ensure it's DataType JSON format (quoted string), not StructField JSON (object)
                            if not type_json_value or not type_json_value.strip().startswith('"'):
                                # Fallback to simple string type if generation failed
                                type_json_value = '"string"'
                            
                            if debug:
                                print(f"[DEBUG] Input param {input_param.name}: type_json={type_json_value}")
                            
                            input_params_list.append(
                                FunctionParameterInfo(
                                    name=input_param.name,
                                    type_text=input_param.type_text,
                                    type_name=input_param.type_name,
                                    type_json=type_json_value,  # DataType JSON format (quoted string)
                                    position=input_param.position,
                                    parameter_mode=input_param.parameter_mode,
                                    parameter_type=input_param.parameter_type,
                                    parameter_default=input_param.parameter_default,  # CRITICAL: Preserve default value
                                )
                            )
                        except Exception as e:
                            if debug:
                                print(f"[DEBUG] ⚠ Could not convert input param {input_param.name}: {e}")
                            # Fallback: use hardcoded DataType JSON
                            input_params_list.append(
                                FunctionParameterInfo(
                                    name=input_param.name,
                                    type_text=input_param.type_text,
                                    type_name=input_param.type_name,
                                    type_json='"string"',  # Hardcoded correct DataType JSON
                                    position=input_param.position,
                                    parameter_mode=input_param.parameter_mode,
                                    parameter_type=input_param.parameter_type,
                                    parameter_default=input_param.parameter_default,  # CRITICAL: Preserve default value
                                )
                            )
                    
                    # Parse return_type: "TABLE(col1 TYPE, col2 TYPE, ...)"
                    table_match = re.match(r'TABLE\((.*)\)', return_type)
                    return_params_list = []
                    
                    if table_match:
                        columns_str = table_match.group(1)
                        
                        # Parse columns, handling nested types like ARRAY<STRING>
                        columns = []
                        current_col = ""
                        paren_depth = 0
                        angle_depth = 0
                        
                        for char in columns_str:
                            if char == '(':
                                paren_depth += 1
                            elif char == ')':
                                paren_depth -= 1
                            elif char == '<':
                                angle_depth += 1
                            elif char == '>':
                                angle_depth -= 1
                            elif char == ',' and paren_depth == 0 and angle_depth == 0:
                                columns.append(current_col.strip())
                                current_col = ""
                                continue
                            current_col += char
                        
                        if current_col.strip():
                            columns.append(current_col.strip())
                        
                        # Parse each column definition
                        for position, col_def in enumerate(columns):
                            col_def = col_def.strip()
                            
                            # Check for NOT NULL
                            nullable = "NOT NULL" not in col_def.upper()
                            col_def = re.sub(r'\s+NOT\s+NULL', '', col_def, flags=re.IGNORECASE)
                            
                            # Split into name and type (last space before type)
                            # Handle types like "ARRAY<STRING>" by finding the last space before the type
                            # Simple approach: split on last space
                            parts = col_def.rsplit(' ', 1)
                            if len(parts) == 2:
                                col_name, col_type = parts
                                
                                # Map SQL type to Spark type
                                col_type_upper = col_type.upper()
                                if col_type_upper.startswith('ARRAY<'):
                                    # Extract inner type
                                    inner_match = re.match(r'ARRAY<(\w+)>', col_type_upper)
                                    if inner_match:
                                        inner_type = inner_match.group(1)
                                        if inner_type == 'STRING':
                                            spark_type = ArrayType(StringType())
                                        else:
                                            spark_type = ArrayType(StringType())  # Default to string array
                                    else:
                                        spark_type = ArrayType(StringType())
                                elif col_type_upper == 'STRING':
                                    spark_type = StringType()
                                elif col_type_upper == 'INT':
                                    spark_type = LongType()
                                elif col_type_upper == 'DOUBLE':
                                    spark_type = DoubleType()
                                elif col_type_upper == 'BOOLEAN':
                                    spark_type = BooleanType()
                                elif col_type_upper == 'TIMESTAMP':
                                    spark_type = TimestampType()
                                else:
                                    spark_type = StringType()  # Default fallback
                                
                                # Generate type_json
                                # Unity Catalog expects DataType JSON (quoted string), not StructField JSON (object)
                                # For simple types: '"string"', for arrays: '{"type":"array",...}'
                                type_json_value = TypeConverter.spark_to_datatype_json(spark_type)
                                
                                # CRITICAL: Ensure it's DataType JSON format (quoted string for simple types)
                                if not type_json_value or (not isinstance(spark_type, ArrayType) and not type_json_value.strip().startswith('"')):
                                    # Fallback to simple string type if generation failed
                                    type_json_value = '"string"'
                                
                                if debug:
                                    print(f"[DEBUG] Generated type_json for {col_name} ({col_type}): {type_json_value}")
                                
                                # Get ColumnTypeName
                                _, type_name = TypeConverter.spark_to_sql_type_info(spark_type)
                                
                                return_params_list.append(
                                    FunctionParameterInfo(
                                        name=col_name,
                                        type_text=col_type,
                                        type_name=type_name,
                                        type_json=type_json_value,  # DataType JSON format (quoted string or array object)
                                        position=position,
                                        parameter_mode=FunctionParameterMode.IN,
                                        parameter_type=FunctionParameterType.COLUMN,  # Return parameters are COLUMN type
                                    )
                                )
                    
                    # Update function with structured metadata for both input and return params
                    # This prevents Unity Catalog from trying to introspect Python code
                    if debug:
                        print(f"[DEBUG] Attempting to patch function metadata via SDK API...")
                        print(f"[DEBUG]   Function: {full_function_name}")
                        print(f"[DEBUG]   Input params count: {len(input_params_list)}")
                        print(f"[DEBUG]   Return params count: {len(return_params_list)}")
                    
                    update_function = UpdateFunction(
                        name=function_name,
                        input_params=FunctionParameterInfos(parameters=input_params_list),  # Patch input_params
                        return_params=FunctionParameterInfos(parameters=return_params_list),
                        # Keep all other fields from existing function
                    )
                    
                    if debug:
                        print(f"[DEBUG] Calling functions.update() with UpdateFunction...")
                        # Debug: Show parameter_default values being set
                        for param in input_params_list:
                            if param.parameter_default:
                                print(f"[DEBUG]   Setting parameter_default for {param.name}: '{param.parameter_default}'")
                    
                    self.workspace_client.functions.update(
                        name=full_function_name,
                        function_info=update_function,
                    )
                    
                    # IMMEDIATE VERIFICATION: Check if patch stuck (like diagnostic script)
                    import time
                    time.sleep(0.3)  # Brief wait for update to propagate
                    
                    # CRITICAL: Always verify the patch was applied correctly
                    # This ensures type_json is in DataType JSON format (quoted string), not StructField JSON (object)
                    verification_passed = False
                    max_retries = 3
                    retry_delay = 0.5  # seconds
                    
                    for retry in range(max_retries):
                        try:
                            # Fetch fresh from API to verify patch stuck
                            func_verify = self.workspace_client.functions.get(full_internal_function_name)
                            
                            # Check if type_json is DataType JSON (quoted string) or StructField JSON (object)
                            input_issues: list[str] = []
                            if func_verify.input_params and func_verify.input_params.parameters:
                                # Create a map of expected parameter_default values from original input_params
                                expected_defaults = {p.name: p.parameter_default for p in input_params if hasattr(p, 'parameter_default') and p.parameter_default}
                                
                                for param in func_verify.input_params.parameters:
                                    if not param.type_json or param.type_json.strip() == "":
                                        input_issues.append(f"{param.name}: type_json is empty")
                                    elif param.type_json.strip().startswith("{"):
                                        # It's StructField JSON (object), not DataType JSON (quoted string)
                                        input_issues.append(f"{param.name}: type_json is StructField JSON (object), expected DataType JSON (quoted string)")
                                        if debug:
                                            print(f"[DEBUG]   Found StructField JSON: {param.type_json[:80]}...")
                                    elif not param.type_json.strip().startswith('"'):
                                        input_issues.append(f"{param.name}: type_json format invalid: {param.type_json[:50]}")
                                    
                                    # CRITICAL: Verify parameter_default is preserved
                                    if param.name in expected_defaults:
                                        expected_default = expected_defaults[param.name]
                                        if param.parameter_default != expected_default:
                                            input_issues.append(f"{param.name}: parameter_default not preserved (expected '{expected_default}', got '{param.parameter_default}')")
                                            if debug:
                                                print(f"[DEBUG]   ⚠ {param.name}: parameter_default mismatch - expected '{expected_default}', got '{param.parameter_default}'")
                            else:
                                input_issues.append("No input_params found")
                            
                            # Verify return_params - check for DataType JSON format
                            return_issues: list[str] = []
                            if func_verify.return_params and func_verify.return_params.parameters:
                                for param in func_verify.return_params.parameters:
                                    if not param.type_json or param.type_json.strip() == "":
                                        return_issues.append(f"{param.name}: type_json is empty")
                                    elif param.type_json.strip().startswith("{"):
                                        # Check if it's an array (which should be an object) or StructField JSON
                                        try:
                                            import json
                                            parsed = json.loads(param.type_json)
                                            # If it has "name" key, it's StructField JSON (wrong format)
                                            if "name" in parsed:
                                                return_issues.append(f"{param.name}: type_json is StructField JSON (object), expected DataType JSON (quoted string)")
                                                if debug:
                                                    print(f"[DEBUG]   Found StructField JSON: {param.type_json[:80]}...")
                                        except json.JSONDecodeError:
                                            return_issues.append(f"{param.name}: type_json is invalid JSON")
                                    elif not param.type_json.strip().startswith('"'):
                                        return_issues.append(f"{param.name}: type_json format invalid: {param.type_json[:50]}")
                            else:
                                return_issues.append("No return_params found")
                            
                            # Check if verification passed
                            if not input_issues and not return_issues:
                                verification_passed = True
                                if debug:
                                    print(f"[DEBUG] ✓ Function metadata patched and verified successfully")
                                    print(f"[DEBUG]   Input parameters: {len(func_verify.input_params.parameters) if func_verify.input_params else 0}")
                                    print(f"[DEBUG]   Return parameters: {len(func_verify.return_params.parameters) if func_verify.return_params else 0}")
                                    # Show sample to confirm DataType JSON format
                                    if func_verify.input_params and func_verify.input_params.parameters:
                                        sample_param = func_verify.input_params.parameters[0]
                                        print(f"[DEBUG]   Sample type_json (should be quoted string): {sample_param.name}={sample_param.type_json}")
                                        # Show parameter_default values for view properties
                                        view_property_params = [p for p in func_verify.input_params.parameters if p.parameter_default]
                                        if view_property_params:
                                            print(f"[DEBUG]   View property parameters with defaults: {len(view_property_params)}")
                                            for p in view_property_params[:3]:  # Show first 3
                                                print(f"[DEBUG]     {p.name}: parameter_default='{p.parameter_default}'")
                                break  # Verification passed, exit retry loop
                            else:
                                # Unity Catalog overwrote with StructField JSON - need to re-patch
                                all_issues = input_issues + return_issues
                                if debug or retry == max_retries - 1:
                                    print(f"[DEBUG] ⚠ Verification failed (attempt {retry + 1}/{max_retries}): Unity Catalog overwrote with StructField JSON")
                                    for issue in all_issues[:3]:  # Show first 3 issues
                                        print(f"[DEBUG]   - {issue}")
                                
                                # Re-patch with hardcoded DataType JSON (like diagnostic script)
                                if retry == max_retries - 1:
                                    if debug:
                                        print(f"[DEBUG] Re-patching with hardcoded DataType JSON format...")
                                    
                                    # Re-patch: use hardcoded DataType JSON based on type_text
                                    fixed_inputs: list[FunctionParameterInfo] = []
                                    if func_verify.input_params and func_verify.input_params.parameters:
                                        # Create lookup map for parameter_default from original input_params
                                        orig_defaults = {p.name: p.parameter_default for p in input_params if hasattr(p, 'parameter_default') and p.parameter_default}
                                        
                                        for param in func_verify.input_params.parameters:
                                            type_text_upper = (param.type_text or "STRING").upper()
                                            if type_text_upper == "STRING":
                                                correct_json = '"string"'
                                            elif type_text_upper == "INT":
                                                correct_json = '"long"'
                                            elif type_text_upper == "DOUBLE":
                                                correct_json = '"double"'
                                            elif type_text_upper == "BOOLEAN":
                                                correct_json = '"boolean"'
                                            elif type_text_upper.startswith("ARRAY<"):
                                                import json
                                                inner_type = type_text_upper.replace("ARRAY<", "").replace(">", "")
                                                element_type = "string" if inner_type == "STRING" else "string"
                                                correct_json = json.dumps({"type": "array", "elementType": element_type, "containsNull": True})
                                            else:
                                                correct_json = '"string"'
                                            
                                            # CRITICAL: Preserve parameter_default from original input_params
                                            param_default = orig_defaults.get(param.name, param.parameter_default)
                                            
                                            fixed_inputs.append(
                                                FunctionParameterInfo(
                                                    name=param.name,
                                                    type_name=param.type_name or ColumnTypeName.STRING,
                                                    type_text=param.type_text or "STRING",
                                                    type_json=correct_json,  # Hardcoded DataType JSON
                                                    position=param.position,
                                                    parameter_mode=param.parameter_mode,
                                                    parameter_type=param.parameter_type,
                                                    parameter_default=param_default,  # CRITICAL: Preserve default value
                                                )
                                            )
                                    
                                    fixed_returns: list[FunctionParameterInfo] = []
                                    if func_verify.return_params and func_verify.return_params.parameters:
                                        for param in func_verify.return_params.parameters:
                                            type_text_upper = (param.type_text or "STRING").upper()
                                            if type_text_upper == "STRING":
                                                correct_json = '"string"'
                                            elif type_text_upper == "INT":
                                                correct_json = '"long"'
                                            elif type_text_upper == "DOUBLE":
                                                correct_json = '"double"'
                                            elif type_text_upper == "BOOLEAN":
                                                correct_json = '"boolean"'
                                            elif type_text_upper.startswith("ARRAY<"):
                                                import json
                                                inner_type = type_text_upper.replace("ARRAY<", "").replace(">", "")
                                                element_type = "string" if inner_type == "STRING" else "string"
                                                correct_json = json.dumps({"type": "array", "elementType": element_type, "containsNull": True})
                                            else:
                                                correct_json = '"string"'
                                            
                                            fixed_returns.append(
                                                FunctionParameterInfo(
                                                    name=param.name,
                                                    type_name=param.type_name or ColumnTypeName.STRING,
                                                    type_text=param.type_text or "STRING",
                                                    type_json=correct_json,  # Hardcoded DataType JSON
                                                    position=param.position,
                                                    parameter_mode=param.parameter_mode,
                                                    parameter_type=param.parameter_type,
                                                )
                                            )
                                    
                                    # Re-apply patch with hardcoded DataType JSON
                                    try:
                                        fix_update = UpdateFunction(
                                            name=internal_function_name,
                                            input_params=FunctionParameterInfos(parameters=fixed_inputs) if fixed_inputs else None,
                                            return_params=FunctionParameterInfos(parameters=fixed_returns) if fixed_returns else None,
                                        )
                                        self.workspace_client.functions.update(
                                            name=full_internal_function_name,
                                            function_info=fix_update,
                                        )
                                        if debug:
                                            print(f"[DEBUG] ✓ Re-patched with hardcoded DataType JSON")
                                        
                                        time.sleep(0.5)  # Wait for update to propagate
                                        verification_passed = True  # Will be re-verified in next iteration
                                        
                                    except Exception as fix_error:
                                        if debug:
                                            print(f"[DEBUG] ⚠ Could not re-patch: {fix_error}")
                                
                                # Wait before retry (except on last attempt)
                                if retry < max_retries - 1:
                                    time.sleep(retry_delay)
                                    
                        except Exception as verify_error:
                            if debug or retry == max_retries - 1:
                                print(f"[DEBUG] ⚠ Verification error (attempt {retry + 1}/{max_retries}): {verify_error}")
                            
                            # Wait before retry (except on last attempt)
                            if retry < max_retries - 1:
                                time.sleep(retry_delay)
                    
                    # Final check: if verification still failed after all retries, warn but don't fail
                    if not verification_passed:
                        print(f"[WARNING] ⚠ Function '{full_internal_function_name}' was registered, but metadata verification failed.")
                        print(f"[WARNING]   Unity Catalog may have overwritten DataType JSON with StructField JSON.")
                        print(f"[WARNING]   This may cause 'end-of-input' errors when executing the function.")
                        print(f"[WARNING]   You may need to manually patch the metadata using the SDK API.")
                        if debug:
                            print(f"[WARNING]   See the debug output above for details.")
                    else:
                        # Verification passed - print success message even if not in debug mode
                        print(f"[INFO] ✓ Function '{full_internal_function_name}' registered and metadata verified (DataType JSON format)")
                        
                except Exception as metadata_error:
                    if debug:
                        print(f"[DEBUG] ⚠ Warning: Could not patch function metadata: {metadata_error}")
                        print(f"[DEBUG]   Exception type: {type(metadata_error).__name__}")
                        import traceback
                        print(f"[DEBUG]   Traceback: {traceback.format_exc()}")
                        print(f"[DEBUG]   Function registered via SQL, but metadata patch failed")
                        print(f"[DEBUG]   This may cause introspection failures at execution time")
                    # Don't fail the registration - SQL registration succeeded
                    # But log the warning so user knows metadata might be corrupted
                
                # Create SQL wrapper function with DEFAULT NULL for optional parameters
                # This allows users to omit optional parameters in SQL queries
                # The wrapper calls the internal Python UDTF
                self._create_sql_wrapper(
                    catalog=catalog,
                    schema=schema,
                    function_name=function_name,
                    internal_function_name=internal_function_name,
                    input_params=input_params,
                    return_type=return_type,
                    warehouse_id=warehouse_id,
                    comment=comment,
                    if_exists=if_exists,
                    debug=debug,
                )
                
                # Add comment if provided (using SDK API - Databricks doesn't support COMMENT ON FUNCTION SQL)
                if comment:
                    try:
                        if debug:
                            print(f"[DEBUG] Adding comment via SDK API...")
                        
                        # Get current function to preserve all fields
                        current_function = self.workspace_client.functions.get(full_internal_function_name)
                        
                        # Import UpdateFunction if not already imported
                        from databricks.sdk.service.catalog import UpdateFunction
                        
                        # Create UpdateFunction with comment (preserve all other fields)
                        update_function = UpdateFunction(
                            name=internal_function_name,  # Just function name, not full name
                            comment=comment,
                            # Preserve all other fields from current function
                            input_params=current_function.input_params,
                            return_params=current_function.return_params,
                            data_type=current_function.data_type,
                            full_data_type=current_function.full_data_type,
                            routine_body=current_function.routine_body,
                            routine_definition=current_function.routine_definition,
                            parameter_style=current_function.parameter_style,
                            is_deterministic=current_function.is_deterministic,
                            sql_data_access=current_function.sql_data_access,
                            is_null_call=current_function.is_null_call,
                            security_type=current_function.security_type,
                            specific_name=current_function.specific_name,
                            external_language=current_function.external_language,
                        )
                        
                        self.workspace_client.functions.update(
                            name=full_internal_function_name,
                            function_info=update_function,
                        )
                        
                        # IMMEDIATE VERIFICATION: Check if comment was persisted
                        import time
                        time.sleep(0.3)  # Brief wait for update to propagate
                        
                        comment_verification_passed = False
                        max_retries = 3
                        retry_delay = 0.5
                        
                        for retry in range(max_retries):
                            try:
                                # Fetch fresh from API to verify comment stuck
                                func_verify = self.workspace_client.functions.get(full_internal_function_name)
                                
                                # Check if comment matches
                                if func_verify.comment == comment:
                                    comment_verification_passed = True
                                    if debug:
                                        print(f"[DEBUG] ✓ Comment verified successfully: '{comment}'")
                                    break
                                else:
                                    if debug or retry == max_retries - 1:
                                        print(f"[DEBUG] ⚠ Comment verification failed (attempt {retry + 1}/{max_retries})")
                                        print(f"[DEBUG]   Expected: '{comment}'")
                                        print(f"[DEBUG]   Got: '{func_verify.comment}'")
                                    
                                    if retry < max_retries - 1:
                                        # Re-apply comment update
                                        update_function_retry = UpdateFunction(
                                            name=internal_function_name,
                                            comment=comment,
                                            input_params=func_verify.input_params,
                                            return_params=func_verify.return_params,
                                            data_type=func_verify.data_type,
                                            full_data_type=func_verify.full_data_type,
                                            routine_body=func_verify.routine_body,
                                            routine_definition=func_verify.routine_definition,
                                            parameter_style=func_verify.parameter_style,
                                            is_deterministic=func_verify.is_deterministic,
                                            sql_data_access=func_verify.sql_data_access,
                                            is_null_call=func_verify.is_null_call,
                                            security_type=func_verify.security_type,
                                            specific_name=func_verify.specific_name,
                                            external_language=func_verify.external_language,
                                        )
                                        self.workspace_client.functions.update(
                                            name=full_internal_function_name,
                                            function_info=update_function_retry,
                                        )
                                        time.sleep(retry_delay)
                                    
                            except Exception as verify_error:
                                if debug or retry == max_retries - 1:
                                    print(f"[DEBUG] ⚠ Comment verification error (attempt {retry + 1}/{max_retries}): {verify_error}")
                                if retry < max_retries - 1:
                                    time.sleep(retry_delay)
                        
                        if not comment_verification_passed:
                            print(f"[WARNING] ⚠ Function '{full_internal_function_name}' comment was set, but verification failed.")
                            print(f"[WARNING]   Comment may not have been persisted correctly.")
                        else:
                            if debug:
                                print(f"[DEBUG] ✓ Comment '{comment}' persisted and verified")
                                
                    except Exception as comment_error:
                        if debug:
                            print(f"[DEBUG] Could not add comment (non-critical): {comment_error}")
                        # Don't fail registration if comment fails - it's non-critical
                return
            elif state_str and "FAILED" in state_str:
                error_msg = error_message or "Unknown error"
                
                # Check if this is the "Zombie Metadata" error - SQL may have succeeded
                # but Unity Catalog failed to parse the metadata it generated
                if "end-of-input" in error_msg or "JsonMappingException" in error_msg or "No content to map" in error_msg:
                    print(f"[WARN] SQL reported FAILED with metadata error for {full_function_name}")
                    print(f"[WARN] Checking if function was actually created...")
                    
                    # Check if function exists despite the error
                    try:
                        existing_function = self.workspace_client.functions.get(full_internal_function_name)
                        if existing_function:
                            print(f"[INFO] Function exists despite SQL error. Attempting auto-repair...")
                            
                            # CALL THE REPAIR FUNCTION
                            if self._repair_udtf_metadata(full_internal_function_name, input_params, return_type, debug=debug):
                                print(f"[INFO] ✓ {full_internal_function_name} registered and repaired.")
                                # Still create the wrapper even if internal was repaired
                                self._create_sql_wrapper(
                                    catalog=catalog,
                                    schema=schema,
                                    function_name=function_name,
                                    internal_function_name=internal_function_name,
                                    input_params=input_params,
                                    return_type=return_type,
                                    warehouse_id=warehouse_id,
                                    comment=comment,
                                    if_exists=if_exists,
                                    debug=debug,
                                )
                                return  # Success - function exists and is repaired
                            else:
                                print(f"[ERROR] {full_internal_function_name} exists but repair failed.")
                                # Continue anyway - function is registered, just metadata is broken
                                # User can manually repair later
                                return
                    except Exception as check_error:
                        # Function doesn't exist - this is a real failure
                        print(f"[ERROR] Function does not exist. This is a real SQL failure.")
                        raise RuntimeError(f"UDTF registration failed: {error_msg}") from check_error
                else:
                    # It's a different error - real failure
                    raise RuntimeError(f"UDTF registration failed: {error_msg}")
            elif state_str:
                # State might be PENDING, RUNNING, etc.
                raise RuntimeError(f"UDTF registration did not complete. State: {state_str}")
            else:
                # If we can't determine state, verify function exists
                try:
                    existing_function = self.workspace_client.functions.get(full_function_name)
                    if existing_function:
                        if debug:
                            print("[DEBUG] No state returned but function exists - assuming success")
                        return
                except NotFound:
                    pass
                
                # If function doesn't exist, this might be a real failure
                # But DDL statements sometimes return empty responses on success
                if debug:
                    print("[DEBUG] No state returned, verifying function exists...")
                try:
                    existing_function = self.workspace_client.functions.get(full_function_name)
                    if existing_function:
                        if debug:
                            print("[DEBUG] Function exists - treating as success")
                        return
                except NotFound:
                    raise RuntimeError(
                        f"UDTF registration may have failed: No state returned and function does not exist. "
                        f"Please verify the SQL statement manually."
                    )
                
        except (RuntimeError, ValueError) as e:
            print(f"\n[ERROR] Failed to register internal UDTF '{full_internal_function_name}' via SQL")
            print(f"[ERROR] Exception: {type(e).__name__}: {e}")
            if debug:
                print(f"[ERROR] SQL Statement:")
                print("-" * 40)
                print(create_function_sql)
                print("-" * 40)
            raise
        except Exception as e:
            # Check if function exists despite error (DDL statements sometimes return errors but succeed)
            error_str = str(e)
            
            # Check if this is the "Zombie Metadata" error
            if "end-of-input" in error_str or "JsonMappingException" in error_str or "No content to map" in error_str:
                print(f"[WARN] Exception caught with metadata error for {full_internal_function_name}")
                print(f"[WARN] Checking if function was actually created...")
                
                try:
                    existing_function = self.workspace_client.functions.get(full_internal_function_name)
                    if existing_function:
                        print(f"[INFO] Function exists despite error. Attempting auto-repair...")
                        
                        # CALL THE REPAIR FUNCTION
                        if self._repair_udtf_metadata(full_internal_function_name, input_params, return_type, debug=debug):
                            print(f"[INFO] ✓ {full_internal_function_name} registered and repaired.")
                            # Still create the wrapper
                            try:
                                self._create_sql_wrapper(
                                    catalog=catalog,
                                    schema=schema,
                                    function_name=function_name,
                                    internal_function_name=internal_function_name,
                                    input_params=input_params,
                                    return_type=return_type,
                                    warehouse_id=warehouse_id,
                                    comment=comment,
                                    if_exists=if_exists,
                                    debug=debug,
                                )
                            except Exception:
                                pass  # Wrapper creation failed, but internal function exists
                            return  # Success - function exists and is repaired
                        else:
                            print(f"[ERROR] {full_internal_function_name} exists but repair failed.")
                            # Continue anyway - function is registered, just metadata is broken
                            return
                except Exception as check_error:
                    # Function doesn't exist or can't be checked - this might be a real failure
                    pass  # Fall through to error reporting below
            
            # Original logic for non-metadata errors
            try:
                existing_function = self.workspace_client.functions.get(full_internal_function_name)
                if existing_function:
                    if debug:
                        print(f"[DEBUG] Received error but internal function exists - treating as success: {e}")
                    # Still try to create wrapper
                    try:
                        self._create_sql_wrapper(
                            catalog=catalog,
                            schema=schema,
                            function_name=function_name,
                            internal_function_name=internal_function_name,
                            input_params=input_params,
                            return_type=return_type,
                            warehouse_id=warehouse_id,
                            comment=comment,
                            if_exists=if_exists,
                            debug=debug,
                        )
                    except Exception:
                        pass  # Wrapper creation failed, but internal function exists
                    return
            except NotFound:
                pass
            
            print(f"\n[ERROR] Failed to register internal UDTF '{full_internal_function_name}' via SQL")
            print(f"[ERROR] Exception: {type(e).__name__}: {e}")
            if debug:
                print(f"[ERROR] SQL Statement:")
                print("-" * 40)
                print(create_function_sql)
                print("-" * 40)
            raise RuntimeError(f"Failed to register internal UDTF {full_internal_function_name}: {e}") from e

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
        """Create a SQL wrapper function with DEFAULT NULL for optional parameters.
        
        This wrapper calls the internal Python UDTF and allows users to omit optional
        parameters (view properties) in SQL queries.
        
        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            function_name: Public function name (the wrapper)
            internal_function_name: Internal Python UDTF name (with _internal suffix)
            input_params: Function parameter definitions
            return_type: Return type DDL string (e.g., "TABLE(col1 STRING, col2 INT)")
            warehouse_id: SQL warehouse ID (required for SQL execution). If None, uses default.
            comment: Function description
            if_exists: What to do if function already exists: "skip", "replace", "error"
            debug: If True, prints detailed information
        """
        from databricks.sdk.errors import NotFound
        
        full_function_name = f"{catalog}.{schema}.{function_name}"
        full_internal_function_name = f"{catalog}.{schema}.{internal_function_name}"
        
        # Get warehouse_id - try to find default if not provided
        if warehouse_id is None:
            warehouse_id = self._get_default_warehouse_id()
        
        # Handle if_exists
        if if_exists == "replace":
            try:
                if debug:
                    print(f"[DEBUG] Dropping existing wrapper function: {full_function_name}")
                self.workspace_client.functions.delete(full_function_name)
                import time
                time.sleep(0.5)
            except NotFound:
                if debug:
                    print(f"[DEBUG] Wrapper function does not exist, skipping drop")
            except Exception as e:
                if debug:
                    print(f"[DEBUG] Error dropping wrapper function (may not exist): {e}")
        elif if_exists == "skip":
            try:
                self.workspace_client.functions.get(full_function_name)
                if debug:
                    print(f"[DEBUG] Wrapper function {full_function_name} already exists, skipping")
                return
            except NotFound:
                pass  # Function doesn't exist, proceed
        elif if_exists == "error":
            try:
                self.workspace_client.functions.get(full_function_name)
                from databricks.sdk.errors import ResourceAlreadyExists
                raise ResourceAlreadyExists(
                    f"Routine or Model '{function_name}' already exists in {catalog}.{schema}"
                )
            except NotFound:
                pass  # Function doesn't exist, proceed
        else:
            raise ValueError(f"Invalid if_exists value: {if_exists}. Must be 'skip', 'replace', or 'error'")
        
        # Build input parameters SQL string with DEFAULT NULL for optional parameters
        # Secret parameters (first 5) are required, view properties are optional
        input_params_sql = []
        secret_param_names = {"client_id", "client_secret", "tenant_id", "cdf_cluster", "project"}
        
        for param in input_params:
            param_sql = f"{param.name} {param.type_text}"
            # Add DEFAULT NULL for view property parameters (not secret parameters)
            if param.name not in secret_param_names:
                param_sql += " DEFAULT NULL"
            input_params_sql.append(param_sql)
        
        input_params_str = ", ".join(input_params_sql)
        
        # Build parameter list for calling the internal function (all parameters, no defaults)
        internal_params_str = ", ".join([p.name for p in input_params])
        
        # Create SQL wrapper function
        create_wrapper_sql = f"""CREATE OR REPLACE FUNCTION {full_function_name}({input_params_str})
RETURNS {return_type}
RETURN SELECT * FROM {full_internal_function_name}({internal_params_str})"""
        
        if debug:
            print(f"\n[DEBUG] === Creating SQL wrapper function: {full_function_name} ===")
            print(f"[DEBUG] Internal function: {full_internal_function_name}")
            print(f"[DEBUG] Input parameters with defaults: {input_params_str}")
            print(f"[DEBUG] SQL Statement:")
            print("-" * 40)
            print(create_wrapper_sql)
            print("-" * 40)
        
        # Execute SQL statement to create wrapper
        try:
            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=create_wrapper_sql,
                wait_timeout="30s",
            )
            
            # Check response status
            state = None
            if hasattr(response, "status") and response.status is not None and hasattr(response.status, "state"):
                state = response.status.state
            elif hasattr(response, "state"):
                state = response.state
            
            state_str = str(state).upper() if state else None
            print(f"[INFO] SQL wrapper registration state: {state_str}")
            
            if state_str and "SUCCEEDED" in state_str:
                print(f"[INFO] ✓ SQL wrapper '{full_function_name}' created successfully")
                if debug:
                    print(f"[DEBUG] Wrapper function allows omitting optional parameters (view properties)")
            elif state_str and "FAILED" in state_str:
                error_message = None
                if hasattr(response, "status") and response.status is not None:
                    if hasattr(response.status, "error") and response.status.error is not None:
                        error_obj = response.status.error
                        if hasattr(error_obj, "message"):
                            error_message = error_obj.message
                raise RuntimeError(f"Failed to create SQL wrapper: {error_message or 'Unknown error'}")
        except Exception as e:
            print(f"[ERROR] Failed to create SQL wrapper '{full_function_name}': {e}")
            if debug:
                import traceback
                traceback.print_exc()
            raise RuntimeError(f"Failed to create SQL wrapper {full_function_name}: {e}") from e

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

        # Wrap input_params in FunctionParameterInfos structure
        input_params_wrapped = FunctionParameterInfos(parameters=input_params)
        
        # Validate and wrap return_params
        # Unity Catalog requires return_params to be populated for UDTFs (cannot be None or empty)
        if not return_params:
            raise ValueError(
                f"return_params is required for UDTF registration but was None or empty. "
                f"This indicates a bug in the generator - return_params should always be parsed from the UDTF class or view."
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
                result["parameters"] = [p.as_dict() if hasattr(p, 'as_dict') else p for p in return_params_wrapped.parameters]
            return result
        
        return_params_wrapped.as_dict = patched_as_dict  # type: ignore[method-assign]
        
        # Debug output
        if debug:
            print(f"[DEBUG] Including {len(return_params)} return_params in CreateFunction")
            # Debug: Check what will be serialized
            return_params_dict = return_params_wrapped.as_dict()
            print(f"[DEBUG] return_params_wrapped.as_dict() = {return_params_dict}")
            print(f"[DEBUG] return_params_wrapped.parameters = {return_params_wrapped.parameters}")
        
        create_function = CreateFunction(**create_function_kwargs)
        
        # Debug: Check what CreateFunction will serialize
        if debug:
            create_function_dict = create_function.as_dict()
            print(f"[DEBUG] CreateFunction.as_dict() includes return_params: {'return_params' in create_function_dict}")
            if 'return_params' in create_function_dict:
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
