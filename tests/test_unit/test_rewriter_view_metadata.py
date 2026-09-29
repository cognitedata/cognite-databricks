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

from cognite.databricks.data_model_query_rewriter import DataModelQueryRewriter, DataModelViewMetadata
from cognite.databricks.generator import UDTFGenerator
from cognite.pygen_spark.udtf_parameters import base_url_parameter, data_model_pushdown_parameters

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
