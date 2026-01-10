"""Unit tests for TypeConverter Databricks-specific methods."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from cognite.databricks.type_converter import TypeConverter


class TestTypeConverterDatabricks:
    """Tests for TypeConverter Databricks-specific methods."""

    def test_spark_to_sql_type_info_string(self) -> None:
        """Test StringType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(StringType())

        assert sql_type == "STRING"
        assert type_name.value == "STRING"

    def test_spark_to_sql_type_info_long(self) -> None:
        """Test LongType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(LongType())

        assert sql_type == "INT"
        assert type_name.value == "INT"

    def test_spark_to_sql_type_info_double(self) -> None:
        """Test DoubleType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(DoubleType())

        assert sql_type == "DOUBLE"
        assert type_name.value == "DOUBLE"

    def test_spark_to_sql_type_info_boolean(self) -> None:
        """Test BooleanType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(BooleanType())

        assert sql_type == "BOOLEAN"
        assert type_name.value == "BOOLEAN"

    def test_spark_to_sql_type_info_date(self) -> None:
        """Test DateType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(DateType())

        assert sql_type == "DATE"
        assert type_name.value == "DATE"

    def test_spark_to_sql_type_info_timestamp(self) -> None:
        """Test TimestampType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(TimestampType())

        assert sql_type == "TIMESTAMP"
        assert type_name.value == "TIMESTAMP"

    def test_spark_to_sql_type_info_array(self) -> None:
        """Test ArrayType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(ArrayType(StringType()))

        assert sql_type == "ARRAY<STRING>"
        # ArrayType uses STRING as fallback for ColumnTypeName
        assert type_name.value == "STRING"

    def test_spark_to_sql_type_info_nested_array(self) -> None:
        """Test nested ArrayType conversion."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(ArrayType(ArrayType(LongType())))

        assert sql_type == "ARRAY<ARRAY<INT>>"
        assert type_name.value == "STRING"  # Fallback for arrays

    def test_spark_to_sql_type_info_unknown_type(self) -> None:
        """Test unknown type falls back to STRING."""

        # Create a mock type that's not handled
        class UnknownType:
            pass

        sql_type, type_name = TypeConverter.spark_to_sql_type_info(UnknownType())  # type: ignore[arg-type]

        assert sql_type == "STRING"
        assert type_name.value == "STRING"
