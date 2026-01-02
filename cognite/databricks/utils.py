"""Utility functions for cognite-databricks."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

from cognite.pygen_spark.utils import to_udtf_function_name

# Re-export for backward compatibility
__all__ = ["to_udtf_function_name"]


def inspect_function_parameters(
    workspace_client: WorkspaceClient,
    function_name: str,
) -> None:
    """Inspect an existing function's parameter definitions to understand type_json format.
    
    This is useful for debugging - see what format Databricks uses for type_json
    in existing functions.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        function_name: Full function name (catalog.schema.function_name)
        
    Example:
        >>> from databricks.sdk import WorkspaceClient
        >>> from cognite.databricks.utils import inspect_function_parameters
        >>> ws = WorkspaceClient()
        >>> inspect_function_parameters(ws, "system.builtin.abs")
    """
    try:
        func_info = workspace_client.functions.get(function_name)
        
        print(f"\n{'='*60}")
        print(f"Function: {function_name}")
        print(f"{'='*60}")
        print(f"Full name: {func_info.full_name}")
        print(f"Routine body: {func_info.routine_body}")
        print(f"External language: {func_info.external_language}")
        print(f"Data type: {func_info.data_type}")
        print(f"Full data type: {func_info.full_data_type}")
        
        if func_info.input_params and func_info.input_params.parameters:
            print(f"\nInput Parameters ({len(func_info.input_params.parameters)}):")
            print("-" * 60)
            for param in func_info.input_params.parameters:
                print(f"  [{param.position}] {param.name}:")
                print(f"      type_text: {repr(param.type_text)}")
                print(f"      type_name: {param.type_name}")
                print(f"      type_json: {repr(param.type_json)}")
                print(f"      parameter_mode: {param.parameter_mode}")
                print(f"      parameter_type: {param.parameter_type}")
        else:
            print("\nNo input parameters")
        
        if func_info.return_params and func_info.return_params.parameters:
            print(f"\nReturn Parameters ({len(func_info.return_params.parameters)}):")
            print("-" * 60)
            for param in func_info.return_params.parameters:
                print(f"  [{param.position}] {param.name}:")
                print(f"      type_text: {repr(param.type_text)}")
                print(f"      type_name: {param.type_name}")
                print(f"      type_json: {repr(param.type_json)}")
        
        print(f"\n{'='*60}\n")
        
    except Exception as e:
        print(f"Error getting function {function_name}: {e}")


def list_functions_in_schema(
    workspace_client: WorkspaceClient,
    catalog: str,
    schema: str,
    limit: int = 10,
) -> list[str]:
    """List functions in a schema.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        catalog: Catalog name
        schema: Schema name
        limit: Maximum number of functions to return
        
    Returns:
        List of function names
    """
    functions = []
    try:
        for func in workspace_client.functions.list(catalog_name=catalog, schema_name=schema):
            functions.append(func.full_name)
            if len(functions) >= limit:
                break
    except Exception as e:
        print(f"Error listing functions: {e}")
    
    return functions


def inspect_recently_created_udtf(
    workspace_client: WorkspaceClient,
    catalog: str,
    schema: str,
    function_name: str,
) -> None:
    """Inspect a recently created UDTF to see what was actually stored.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        catalog: Catalog name
        schema: Schema name
        function_name: Function name (without catalog.schema prefix)
    """
    full_name = f"{catalog}.{schema}.{function_name}"
    inspect_function_parameters(workspace_client, full_name)

