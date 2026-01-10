"""Test utilities for cognite-databricks."""

from __future__ import annotations

from datetime import datetime, timezone

from cognite.client import data_modeling as dm


def create_mock_view(
    space: str = "test_space",
    external_id: str = "TestView",
    version: str = "v1",
    properties: dict[str, dm.ViewProperty] | None = None,
) -> dm.View:
    """Create a mock view for testing.

    Args:
        space: View space
        external_id: View external_id
        version: View version
        properties: View properties (defaults to simple text property)

    Returns:
        Mock View object
    """
    if properties is None:
        properties = {
            "name": dm.MappedProperty(
                type=dm.Text(),
                nullable=True,
                container=dm.ContainerId(space, "TestContainer", "1"),
                container_property_identifier="name",
                immutable=False,
                auto_increment=False,
            )
        }

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    return dm.View(
        space=space,
        external_id=external_id,
        version=version,
        created_time=now,
        last_updated_time=now,
        name=external_id,
        description="",
        filter=None,
        implements=[],
        writable=False,
        used_for="node",
        is_global=False,
        properties=properties,
    )
