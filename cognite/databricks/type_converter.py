"""TypeConverter - Unified type conversion using PySpark as source of truth.

This module provides a centralized way to convert between CDF property types,
PySpark DataTypes, SQL DDL strings, and Databricks SDK types.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DataType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        TimestampType,
    )
except ImportError as e:
    # PySpark is provided by Databricks runtime
    raise ImportError(
        "PySpark is required but not available. "
        "In Databricks, PySpark is provided by the runtime. "
        "For local development, install with: pip install cognite-databricks[local]"
    ) from e

if TYPE_CHECKING:
    from cognite.client import data_modeling as dm
    from databricks.sdk.service.catalog import ColumnTypeName


class TypeConverter:
    """Unified type conversion using PySpark as source of truth.
    
    All type conversions go through PySpark DataTypes to ensure consistency
    between generated code, registration, and validation.
    """
    
    @staticmethod
    def cdf_to_spark(property_type: object, is_array: bool = False) -> DataType:
        """Convert CDF property type to PySpark DataType.
        
        Args:
            property_type: CDF property type (e.g., dm.Text, dm.Int32, etc.)
            is_array: If True, wrap the base type in ArrayType
            
        Returns:
            PySpark DataType object
        """
        import cognite.client.data_modeling as dm
        
        # Map CDF property types to PySpark types
        if isinstance(property_type, (dm.Int32, dm.Int64)):
            base_type = LongType()
        elif isinstance(property_type, dm.Boolean):
            base_type = BooleanType()
        elif isinstance(property_type, (dm.Float32, dm.Float64)):
            base_type = DoubleType()
        elif isinstance(property_type, dm.Date):
            base_type = DateType()
        elif isinstance(property_type, dm.Timestamp):
            base_type = TimestampType()
        elif isinstance(property_type, dm.Text):
            base_type = StringType()
        elif isinstance(property_type, dm.DirectRelation):
            # Direct relations are typically represented as strings (external_id references)
            base_type = StringType()
        else:
            # Default to STRING for unknown types
            base_type = StringType()
        
        # Wrap in ArrayType if needed
        if is_array:
            return ArrayType(base_type, containsNull=True)
        else:
            return base_type
    
    @staticmethod
    def spark_to_sql_ddl(spark_type: DataType) -> str:
        """Convert PySpark DataType to SQL DDL type string.
        
        Args:
            spark_type: PySpark DataType
            
        Returns:
            SQL DDL type string (e.g., "STRING", "INT", "ARRAY<STRING>")
        """
        if isinstance(spark_type, StringType):
            return "STRING"
        elif isinstance(spark_type, LongType):
            return "INT"
        elif isinstance(spark_type, DoubleType):
            return "DOUBLE"
        elif isinstance(spark_type, BooleanType):
            return "BOOLEAN"
        elif isinstance(spark_type, DateType):
            return "DATE"
        elif isinstance(spark_type, TimestampType):
            return "TIMESTAMP"
        elif isinstance(spark_type, ArrayType):
            element_ddl = TypeConverter.spark_to_sql_ddl(spark_type.elementType)
            return f"ARRAY<{element_ddl}>"
        else:
            return "STRING"  # Default fallback
    
    @staticmethod
    def spark_to_sql_type_info(spark_type: DataType) -> tuple[str, ColumnTypeName]:
        """Convert PySpark DataType to SQL type info.
        
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
    
    @staticmethod
    def spark_to_type_json(spark_type: DataType, name: str, nullable: bool = True) -> str:
        """Convert PySpark DataType to StructField JSON string.
        
        Args:
            spark_type: PySpark DataType
            name: Field name
            nullable: Whether the field is nullable
            
        Returns:
            JSON string in Spark StructField format
        """
        from pyspark.sql.types import StructField
        
        field = StructField(name, spark_type, nullable=nullable)
        return field.json()
    
    @staticmethod
    def struct_type_to_ddl(struct_type: object) -> str:
        """Convert PySpark StructType to SQL DDL string.
        
        Args:
            struct_type: PySpark StructType
            
        Returns:
            SQL DDL string like "TABLE(col1 STRING, col2 INT, col3 ARRAY<STRING>)"
        """
        from pyspark.sql.types import StructType
        
        if not isinstance(struct_type, StructType):
            raise TypeError(f"Expected StructType, got {type(struct_type)}")
        
        columns = []
        for field in struct_type.fields:
            sql_type = TypeConverter.spark_to_sql_ddl(field.dataType)
            columns.append(f"{field.name} {sql_type}")
        return f"TABLE({', '.join(columns)})"
    
    @staticmethod
    def python_type_to_spark(python_type: type) -> DataType:
        """Convert Python type annotation to PySpark DataType.
        
        This is used when parsing UDTF parameters from class annotations
        (e.g., for time series UDTFs that don't have CDF property types).
        
        Args:
            python_type: Python type (e.g., str, int, float, bool)
            
        Returns:
            PySpark DataType object
        """
        if python_type == str:
            return StringType()
        elif python_type == int:
            return LongType()
        elif python_type == float:
            return DoubleType()
        elif python_type == bool:
            return BooleanType()
        else:
            # Default to STRING for unknown types
            return StringType()

