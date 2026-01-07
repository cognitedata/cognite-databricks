"""Test utilities for cognite-databricks tests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View


def assert_udtf_file_valid(file_path: Path) -> None:
    """Assert that a UDTF file is valid.
    
    Args:
        file_path: Path to the UDTF file
        
    Raises:
        AssertionError: If file is invalid
    """
    assert file_path.exists(), f"UDTF file does not exist: {file_path}"
    assert file_path.suffix == ".py", f"UDTF file should be .py: {file_path}"
    
    code = file_path.read_text()
    assert len(code) > 0, f"UDTF file is empty: {file_path}"
    assert "class" in code, f"UDTF file should contain class definition: {file_path}"


def create_mock_view(
    space: str = "test_space",
    external_id: str = "TestView",
    version: str = "v1",
    properties: dict | None = None,
) -> "View":
    """Create a mock view for testing.
    
    Args:
        space: View space
        external_id: View external ID
        version: View version
        properties: View properties (defaults to simple text property)
        
    Returns:
        Mock View object
    """
    from cognite.client import data_modeling as dm

    if properties is None:
        properties = {"name": dm.Text()}

    return dm.View(
        space=space,
        external_id=external_id,
        version=version,
        properties=properties,
    )

