"""Unit tests for utils module."""

from __future__ import annotations

from unittest.mock import MagicMock

from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionInfoRoutineBody,
    FunctionParameterInfo,
    FunctionParameterType,
)

from cognite.databricks.utils import (
    inspect_function_parameters,
    inspect_recently_created_udtf,
    list_functions_in_schema,
)


class TestUtils:
    """Tests for utility functions."""

    def test_list_functions_in_schema(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test listing functions in a schema."""
        # Mock function list
        mock_func1 = MagicMock(full_name="catalog.schema.func1")
        mock_func2 = MagicMock(full_name="catalog.schema.func2")
        mock_workspace_client.functions.list.return_value = [mock_func1, mock_func2]

        result = list_functions_in_schema(mock_workspace_client, "catalog", "schema", limit=10)

        assert len(result) == 2
        assert "catalog.schema.func1" in result
        assert "catalog.schema.func2" in result
        mock_workspace_client.functions.list.assert_called_once_with(catalog_name="catalog", schema_name="schema")

    def test_list_functions_in_schema_with_limit(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test listing functions with limit."""
        mock_funcs = [MagicMock(full_name=f"catalog.schema.func{i}") for i in range(5)]
        mock_workspace_client.functions.list.return_value = mock_funcs

        result = list_functions_in_schema(mock_workspace_client, "catalog", "schema", limit=3)

        assert len(result) == 3
        assert all(f"catalog.schema.func{i}" in result for i in range(3))

    def test_list_functions_in_schema_with_none_full_name(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test listing functions when full_name is None."""
        mock_func1 = MagicMock(full_name="catalog.schema.func1")
        mock_func2 = MagicMock(full_name=None)  # Some functions might not have full_name
        mock_workspace_client.functions.list.return_value = [mock_func1, mock_func2]

        result = list_functions_in_schema(mock_workspace_client, "catalog", "schema", limit=10)

        assert len(result) == 1
        assert "catalog.schema.func1" in result

    def test_list_functions_in_schema_error_handling(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test error handling in list_functions_in_schema."""
        mock_workspace_client.functions.list.side_effect = RuntimeError("Connection error")

        result = list_functions_in_schema(mock_workspace_client, "catalog", "schema", limit=10)

        assert result == []

    def test_inspect_function_parameters(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test inspecting function parameters."""
        # Mock function info with input and return parameters
        # Note: type_name should be ColumnTypeName, not FunctionParameterType
        mock_input_param = FunctionParameterInfo(
            name="param1",
            type_text="STRING",
            type_name=ColumnTypeName.STRING,  # Fixed: use ColumnTypeName
            type_json='{"type": "string"}',
            position=0,
            parameter_type=FunctionParameterType.PARAM,
        )
        mock_return_param = FunctionParameterInfo(
            name="result",
            type_text="TABLE(id INT)",
            type_name=ColumnTypeName.TABLE_TYPE,  # Fixed: use ColumnTypeName.TABLE_TYPE
            type_json='{"type": "table", "fields": [{"name": "id", "type": "int"}]}',
            position=0,
            parameter_type=FunctionParameterType.PARAM,
        )

        mock_func_info = FunctionInfo(
            full_name="catalog.schema.func",
            routine_body=FunctionInfoRoutineBody.EXTERNAL,  # Fixed: use enum
            external_language="PYTHON",
            data_type=ColumnTypeName.TABLE_TYPE,  # Fixed: use ColumnTypeName enum
            full_data_type="TABLE(id INT)",
            input_params=MagicMock(parameters=[mock_input_param]),
            return_params=MagicMock(parameters=[mock_return_param]),
        )
        mock_workspace_client.functions.get.return_value = mock_func_info

        # Should not raise - just prints to stdout
        inspect_function_parameters(mock_workspace_client, "catalog.schema.func")

        mock_workspace_client.functions.get.assert_called_once_with("catalog.schema.func")

    def test_inspect_function_parameters_no_params(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test inspecting function with no parameters."""
        mock_func_info = FunctionInfo(
            full_name="catalog.schema.func",
            routine_body=FunctionInfoRoutineBody.EXTERNAL,  # Fixed: use enum
            external_language="PYTHON",
            data_type=ColumnTypeName.TABLE_TYPE,  # Fixed: use ColumnTypeName enum
            full_data_type="TABLE()",
            input_params=None,
            return_params=None,
        )
        mock_workspace_client.functions.get.return_value = mock_func_info

        # Should not raise
        inspect_function_parameters(mock_workspace_client, "catalog.schema.func")

    def test_inspect_function_parameters_error_handling(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test error handling in inspect_function_parameters."""
        mock_workspace_client.functions.get.side_effect = Exception("Function not found")

        # Should not raise - just prints error
        inspect_function_parameters(mock_workspace_client, "catalog.schema.func")

    def test_inspect_recently_created_udtf(
        self,
        mock_workspace_client: MagicMock,
    ) -> None:
        """Test inspecting a recently created UDTF."""
        mock_func_info = FunctionInfo(
            full_name="catalog.schema.test_udtf",
            routine_body=FunctionInfoRoutineBody.EXTERNAL,  # Fixed: use enum
            external_language="PYTHON",
            data_type=ColumnTypeName.TABLE_TYPE,  # Fixed: use ColumnTypeName enum
            full_data_type="TABLE(id INT)",
            input_params=None,
            return_params=None,
        )
        mock_workspace_client.functions.get.return_value = mock_func_info

        # Should call inspect_function_parameters with full name
        inspect_recently_created_udtf(mock_workspace_client, "catalog", "schema", "test_udtf")

        mock_workspace_client.functions.get.assert_called_once_with("catalog.schema.test_udtf")
