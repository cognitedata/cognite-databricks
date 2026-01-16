"""TypeConverter - Databricks-specific extensions to pygen-spark TypeConverter.

This module extends the generic TypeConverter from pygen-spark with
Databricks-specific functionality (Unity Catalog ColumnTypeName).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.types import (  # type: ignore[import-not-found]
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from cognite.pygen_spark.type_converter import TypeConverter as BaseTypeConverter

if TYPE_CHECKING:
    from databricks.sdk.service.catalog import ColumnTypeName


class TypeConverter(BaseTypeConverter):
    """TypeConverter with Databricks-specific extensions.

    Extends the generic TypeConverter from pygen-spark with Databricks SDK
    integration for Unity Catalog registration.
    """

    @staticmethod
    def spark_to_sql_type_info(spark_type: DataType) -> tuple[str, ColumnTypeName]:
        """Convert PySpark DataType to SQL type info (Databricks-specific).

        Args:
            spark_type: PySpark DataType

        Returns:
            Tuple of (sql_type_string, ColumnTypeName)
        """
        from databricks.sdk.service.catalog import ColumnTypeName

        if isinstance(spark_type, StringType):
            return ("STRING", ColumnTypeName.STRING)
        elif isinstance(spark_type, (LongType, IntegerType)):
            return ("INT", ColumnTypeName.INT)
        elif isinstance(spark_type, DoubleType):
            return ("DOUBLE", ColumnTypeName.DOUBLE)
        elif isinstance(spark_type, BooleanType):
            return ("BOOLEAN", ColumnTypeName.BOOLEAN)
        elif isinstance(spark_type, DateType):
            return ("DATE", ColumnTypeName.DATE)
        elif isinstance(spark_type, TimestampType):
            return ("TIMESTAMP", ColumnTypeName.TIMESTAMP)
        elif isinstance(spark_type, ArrayType):
            # For arrays, extract base type for ColumnTypeName
            base_sql_type, _base_type_name = TypeConverter.spark_to_sql_type_info(spark_type.elementType)
            sql_type = f"ARRAY<{base_sql_type}>"
            # ColumnTypeName doesn't have ARRAY, use base type or STRING as fallback
            return (sql_type, ColumnTypeName.STRING)
        else:
            return ("STRING", ColumnTypeName.STRING)  # Default fallback

    @staticmethod
    def spark_to_datatype_json(spark_type: DataType) -> str:
        """Convert PySpark DataType to DataType JSON string (for Unity Catalog type_json).

        Unity Catalog's type_json field expects DataType JSON, not StructField JSON.

        For simple types: Returns quoted string (e.g., '"string"', '"long"')
        For complex types: Returns JSON object (e.g., '{"type":"array","elementType":"string","containsNull":true}')

        Args:
            spark_type: PySpark DataType

        Returns:
            JSON string representing the DataType (not StructField)
        """
        import json

        if isinstance(spark_type, StringType):
            return '"string"'
        elif isinstance(spark_type, (LongType, IntegerType)):
            return '"long"'
        elif isinstance(spark_type, DoubleType):
            return '"double"'
        elif isinstance(spark_type, BooleanType):
            return '"boolean"'
        elif isinstance(spark_type, DateType):
            return '"date"'
        elif isinstance(spark_type, TimestampType):
            return '"timestamp"'
        elif isinstance(spark_type, ArrayType):
            # For arrays, return the DataType JSON object
            element_json = TypeConverter.spark_to_datatype_json(spark_type.elementType)
            # element_json is a quoted string like '"string"', we need to unquote it
            element_type_str = json.loads(element_json) if element_json.startswith('"') else element_json

            array_json = {
                "type": "array",
                "elementType": element_type_str,
                "containsNull": spark_type.containsNull,
            }
            return json.dumps(array_json)
        else:
            return '"string"'  # Default fallback
