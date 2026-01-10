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
        elif isinstance(spark_type, LongType):
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
            base_sql_type, base_type_name = TypeConverter.spark_to_sql_type_info(spark_type.elementType)
            sql_type = f"ARRAY<{base_sql_type}>"
            # ColumnTypeName doesn't have ARRAY, use base type or STRING as fallback
            return (sql_type, ColumnTypeName.STRING)
        else:
            return ("STRING", ColumnTypeName.STRING)  # Default fallback
