"""Unit tests for TypeConverter in cognite-databricks."""

from __future__ import annotations

import pytest

from cognite.databricks.type_converter import TypeConverter


class TestTypeConverter:
    """Tests for TypeConverter class."""

    def test_type_converter_exists(self) -> None:
        """Test that TypeConverter can be imported."""
        assert TypeConverter is not None

    # Note: TypeConverter in cognite-databricks is a re-export from pygen-spark
    # More detailed tests are in pygen-spark/tests/test_unit/test_type_converter.py

