"""DataModelQueryRewriter with view metadata: only push what CDF and the UDTF can serve.

Without a schema the rewriter pushed MIN/MAX on text columns (CDF: "Expected property to be of a numerical
type"), bound columns the view does not have, and sent SQL column names where the UDTF needs CDF property names.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.pygen_spark.udtf_parameters import base_url_parameter, data_model_pushdown_parameters
from pydantic import BaseModel, Field

from cognite.databricks.data_model_query_rewriter import DataModelQueryRewriter, DataModelViewMetadata
from cognite.databricks.generator import UDTFGenerator

CERT = "f0connectortest.sailboat_sailboat_v1.ORCCertificate"
SECRET_SCOPE = "cdf_sailboat_sailboat"


def _mapped(container: str, identifier: str, prop_type: dm.PropertyType) -> dm.MappedProperty:
    return dm.MappedProperty(
        container=dm.ContainerId("sailboat", container),
        container_property_identifier=identifier,
        type=prop_type,
        nullable=True,
        immutable=False,
        auto_increment=False,
    )


@pytest.fixture
def certificate_view() -> dm.View:
    return dm.View(
        space="sailboat",
        external_id="ORCCertificate",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "name": _mapped("ORCCertificate", "name", dm.Text()),
            "aph_tod": _mapped("ORCCertificate", "aph_tod", dm.Float32()),
            "class": _mapped("ORCCertificate", "class", dm.Text()),  # reserved word: SQL column is class_
        },
        filter=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
    )


@pytest.fixture
def metadata(certificate_view: dm.View) -> DataModelViewMetadata:
    return DataModelViewMetadata.from_view(certificate_view)


def _named_args(sql: str) -> dict[str, str]:
    return dict(re.findall(r"(\w+)\s*=>\s*('(?:''|[^'])*'|[^,\n]+)", sql))


def test_metadata_maps_sql_columns_to_cdf_properties_and_kinds(metadata: DataModelViewMetadata) -> None:
    assert metadata.view_name == "ORCCertificate"
    assert metadata.by_column["aph_tod"].value_kind == "double"
    assert metadata.by_column["class_"].property == "class"
    assert metadata.is_numeric("aph_tod") and not metadata.is_numeric("name")


def test_min_on_text_column_is_left_to_spark(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT min(name) AS min_name FROM {CERT} WHERE space = 'inst_sailboat_fleet_a'"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert any("numeric" in reason for reason in hints.skip_reasons)
    assert DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata) is None


def test_min_on_numeric_column_is_pushed(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT min(aph_tod) AS min_aph_tod FROM {CERT} WHERE space = 'inst_sailboat_fleet_a'"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    assert json.loads(_named_args(rewritten)["_aggregates"].strip("'")) == [{"fn": "min", "property": "aph_tod"}]


def test_unknown_column_is_not_pushed(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT * FROM {CERT} WHERE boat_name = 'XBOX' LIMIT 5"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert any("boat_name" in reason for reason in hints.skip_reasons)


def test_json_pushdown_args_use_cdf_property_names(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT * FROM {CERT} WHERE class_ IS NOT NULL AND class_ >= 'A' AND class_ = 'ORC'"

    args = _named_args(
        DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata) or ""
    )

    assert json.loads(args["_exists"].strip("'")) == ["class"]
    assert json.loads(args["_gte"].strip("'")) == {"class": "A"}
    assert args["class_"] == "'ORC'"  # view-property equality binds the UDTF parameter (SQL column name)


def test_rewritten_call_supplies_every_udtf_parameter(metadata: DataModelViewMetadata) -> None:
    # Unity Catalog Python UDTFs cannot declare defaults, so every parameter must be passed (NULL when unbound)
    sql = f"SELECT max(aph_tod) AS max_aph_tod FROM {CERT} WHERE space = 'inst_sailboat_fleet_a'"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    names = re.findall(r"(\w+)\s*=>", rewritten)
    assert names == [
        "client_id",
        "client_secret",
        "tenant_id",
        "cdf_cluster",
        "project",
        "name",
        "aph_tod",
        "class_",
        *data_model_pushdown_parameters.names,
        base_url_parameter.name,
    ]
    args = _named_args(rewritten)
    assert args["name"].strip() == "NULL"
    assert args["external_id"].strip() == "NULL"
    assert args["instance_space"] == "'inst_sailboat_fleet_a'"


def test_metadata_for_another_view_is_rejected(metadata: DataModelViewMetadata) -> None:
    with pytest.raises(ValueError, match="ORCCertificate"):
        DataModelQueryRewriter.analyze(
            "SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat LIMIT 1", view_metadata=metadata
        )


def test_generator_rewrite_query_resolves_metadata_from_the_data_model(
    mock_workspace_client: MagicMock,
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    certificate_view: dm.View,
) -> None:
    from cognite.pygen_spark import SparkUDTFGenerator

    model = dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[certificate_view],
    )
    generator = UDTFGenerator(
        workspace_client=mock_workspace_client,
        cognite_client=mock_cognite_client,
        catalog="f0connectortest",
        schema="sailboat_sailboat_v1",
        code_generator=SparkUDTFGenerator(client=mock_cognite_client, output_dir=temp_output_dir, data_model=model),
    )

    text_min = generator.rewrite_query(f"SELECT min(name) AS m FROM {CERT}", secret_scope=SECRET_SCOPE)
    numeric_max = generator.rewrite_query(f"SELECT max(aph_tod) AS m FROM {CERT}", secret_scope=SECRET_SCOPE)

    assert text_min is None
    assert numeric_max is not None and "orc_certificate_udtf(" in numeric_max


class GroupByWhereCase(BaseModel):
    """A WHERE clause that must still push when the query also groups."""

    label: str
    where_sql: str
    instance_space: str | list[str] | None = None
    external_id: str | list[str] | None = None
    name_equals: str | None = None
    name_in: list[str] | None = None
    exists_properties: list[str] = Field(default_factory=list)
    not_exists_properties: list[str] = Field(default_factory=list)
    gt: dict[str, object] = Field(default_factory=dict)
    gte: dict[str, object] = Field(default_factory=dict)
    lt: dict[str, object] = Field(default_factory=dict)
    lte: dict[str, object] = Field(default_factory=dict)


_GROUP_BY_WHERE_CASES = (
    GroupByWhereCase(
        label="space-equals",
        where_sql="space = 'inst_sailboat_fleet_a'",
        instance_space="inst_sailboat_fleet_a",
    ),
    GroupByWhereCase(
        label="space-in",
        where_sql="space IN ('inst_sailboat_fleet_a', 'inst_sailboat_fleet_b')",
        instance_space=["inst_sailboat_fleet_a", "inst_sailboat_fleet_b"],
    ),
    GroupByWhereCase(
        label="external-id-equals",
        where_sql="external_id = 'seed_orc_certificate_fleet_a_01'",
        external_id="seed_orc_certificate_fleet_a_01",
    ),
    GroupByWhereCase(
        label="external-id-in",
        where_sql="external_id IN ('seed_orc_certificate_fleet_a_01', 'seed_orc_certificate_fleet_a_02')",
        external_id=["seed_orc_certificate_fleet_a_01", "seed_orc_certificate_fleet_a_02"],
    ),
    GroupByWhereCase(
        label="property-equals",
        where_sql="name = 'Seed ORC A 01'",
        name_equals="Seed ORC A 01",
    ),
    GroupByWhereCase(
        label="property-in",
        where_sql="name IN ('Seed ORC A 01', 'Seed ORC A 02')",
        name_in=["Seed ORC A 01", "Seed ORC A 02"],
    ),
    GroupByWhereCase(
        label="exists",
        where_sql="class_ IS NOT NULL",
        exists_properties=["class"],
    ),
    GroupByWhereCase(
        label="not-exists",
        where_sql="class_ IS NULL",
        not_exists_properties=["class"],
    ),
    GroupByWhereCase(
        label="numeric-range",
        where_sql="aph_tod >= 480 AND aph_tod < 600",
        gte={"aph_tod": 480},
        lt={"aph_tod": 600},
    ),
    GroupByWhereCase(
        label="space-equals-range-and-exists",
        where_sql=(
            "space = 'inst_sailboat_fleet_a' AND name = 'Seed ORC A 01' AND aph_tod > 100 AND class_ IS NOT NULL"
        ),
        instance_space="inst_sailboat_fleet_a",
        name_equals="Seed ORC A 01",
        gt={"aph_tod": 100},
        exists_properties=["class"],
    ),
)


def _json_arg(args: dict[str, str], name: str) -> object:
    raw = args[name].strip()
    if raw.startswith("'") and raw.endswith("'"):
        raw = raw[1:-1]
    return json.loads(raw)


@pytest.mark.parametrize("case", _GROUP_BY_WHERE_CASES, ids=[case.label for case in _GROUP_BY_WHERE_CASES])
def test_group_by_keeps_where_pushdown(case: GroupByWhereCase, metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT name, count(*) AS n FROM {CERT} WHERE {case.where_sql} GROUP BY name"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    assert rewritten.startswith("SELECT name, external_id AS count_externalId FROM")
    args = _named_args(rewritten)
    assert _json_arg(args, "_group_by") == ["name"]
    assert _json_arg(args, "_aggregates") == [{"fn": "count", "property": "externalId"}]
    assert args["_query_mode"] == "'aggregate'"
    if case.instance_space is not None:
        _assert_bound(args, "instance_space", case.instance_space)
    if case.external_id is not None:
        _assert_bound(args, "external_id", case.external_id)
    if case.name_equals is not None:
        assert args["name"] == f"'{case.name_equals}'"
    if case.name_in is not None:
        assert _json_arg(args, "name") == case.name_in
    if case.exists_properties:
        assert _json_arg(args, "_exists") == case.exists_properties
    if case.not_exists_properties:
        assert _json_arg(args, "_not_exists") == case.not_exists_properties
    if case.gt:
        assert _json_arg(args, "_gt") == case.gt
    if case.gte:
        assert _json_arg(args, "_gte") == case.gte
    if case.lt:
        assert _json_arg(args, "_lt") == case.lt
    if case.lte:
        assert _json_arg(args, "_lte") == case.lte


def _assert_bound(args: dict[str, str], name: str, expected: str | list[str]) -> None:
    if isinstance(expected, str):
        assert args[name] == f"'{expected}'"
        return
    assert _json_arg(args, name) == expected


def test_group_by_two_columns_with_range_and_exists(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT space, name, count(*) FROM {CERT} WHERE aph_tod >= 1 AND class_ IS NOT NULL GROUP BY space, name"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    assert rewritten.startswith("SELECT space, name, external_id AS count_externalId FROM")
    args = _named_args(rewritten)
    assert _json_arg(args, "_group_by") == ["space", "name"]
    assert _json_arg(args, "_gte") == {"aph_tod": 1}
    assert _json_arg(args, "_exists") == ["class"]


def test_group_by_reserved_column_uses_cdf_property_name(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT class_, max(aph_tod) FROM {CERT} WHERE name = 'Seed ORC A 01' GROUP BY class_"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    assert rewritten.startswith("SELECT class_, aph_tod AS max_aph_tod FROM")
    args = _named_args(rewritten)
    assert _json_arg(args, "_group_by") == ["class"]
    assert args["name"] == "'Seed ORC A 01'"


def test_group_by_preserves_select_order(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT count(*) AS n, name AS component FROM {CERT} WHERE space = 'inst_sailboat_fleet_a' GROUP BY name"

    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata)

    assert rewritten is not None
    assert rewritten.startswith("SELECT external_id AS count_externalId, name FROM")


def test_group_by_that_does_not_match_the_select_list_is_not_rewritten(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT name, count(*) FROM {CERT} WHERE space = 'inst_sailboat_fleet_a' GROUP BY aph_tod"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert hints.skip_reasons
    assert DataModelQueryRewriter.rewrite_to_udtf_sql(sql, secret_scope=SECRET_SCOPE, view_metadata=metadata) is None


def test_group_by_same_column_as_the_metric_is_not_rewritten(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT aph_tod, max(aph_tod) FROM {CERT} WHERE space = 'inst_sailboat_fleet_a' GROUP BY aph_tod"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert any("collid" in reason for reason in hints.skip_reasons)


def test_group_by_external_id_collides_with_count(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT external_id, count(*) FROM {CERT} WHERE name = 'Seed ORC A 01' GROUP BY external_id"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert any("external_id" in reason for reason in hints.skip_reasons)


def test_group_by_expression_is_not_rewritten(metadata: DataModelViewMetadata) -> None:
    sql = f"SELECT count(*) FROM {CERT} WHERE space = 'inst_sailboat_fleet_a' GROUP BY lower(name)"

    hints = DataModelQueryRewriter.analyze(sql, view_metadata=metadata)

    assert hints.pushdown_supported is False
    assert any("GROUP BY" in reason for reason in hints.skip_reasons)
