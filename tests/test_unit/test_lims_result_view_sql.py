"""A 40-property LIMS result view must pass every UDTF argument, including base_url.

Column order matches the synthetic ``LimsResult`` view seeded for cognite-cogsail.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.databricks.data_model_query_rewriter import DataModelQueryRewriter, DataModelViewMetadata
from cognite.databricks.generator import UDTFGenerator
from cognite.pygen_spark.udtf_parameters import base_url_parameter, data_model_pushdown_parameters

CATALOG = "f0connectortest"
SCHEMA = "dm_dom_lims_result_limsresult_dom_v1"
SECRET_SCOPE = "cdf_dm_dom_lims_result_limsresult_dom"
VIEW_SQL_NAME = f"{CATALOG}.{SCHEMA}.LimsResult"
CREDENTIALS = ["client_id", "client_secret", "tenant_id", "cdf_cluster", "project"]

# Same order as the cogsail LimsResult container.
LIM_COLUMNS: list[tuple[str, dm.PropertyType]] = [
    ("resultId", dm.Int64()),
    ("testId", dm.Int64()),
    ("sampleId", dm.Int64()),
    ("aliquotId", dm.Text()),
    ("batchOrWorklistId", dm.Text()),
    ("componentName", dm.Text()),
    ("resultType", dm.Text()),
    ("rawValue", dm.Text()),
    ("formattedEntry", dm.Text()),
    ("numericValue", dm.Float64()),
    ("roundedValue", dm.Float64()),
    ("reportingUnits", dm.Text()),
    ("significantFigures", dm.Int32()),
    ("specRuleId", dm.Text()),
    ("minSpecLimit", dm.Float64()),
    ("maxSpecLimit", dm.Float64()),
    ("minActionLimit", dm.Float64()),
    ("maxActionLimit", dm.Float64()),
    ("limitOfDetection", dm.Float64()),
    ("limitOfQuant", dm.Float64()),
    ("qualificationFlag", dm.Text()),
    ("instrumentId", dm.Text()),
    ("analyticalMethodId", dm.Text()),
    ("reagentLotNumber", dm.Text()),
    ("calculationFormulaId", dm.Text()),
    ("dilutionFactor", dm.Float64()),
    ("runNumber", dm.Int32()),
    ("rawDataFilePath", dm.Text()),
    ("statusCode", dm.Text()),
    ("isReportable", dm.Boolean()),
    ("isOutOfSpec", dm.Boolean()),
    ("isOutOfTrend", dm.Boolean()),
    ("retestFlag", dm.Boolean()),
    ("enteredByUserId", dm.Text()),
    ("enteredTimestamp", dm.Timestamp()),
    ("reviewedByUserId", dm.Text()),
    ("reviewedTimestamp", dm.Timestamp()),
    ("changeReasonCode", dm.Text()),
    ("auditComment", dm.Text()),
    ("rowVersionChecksum", dm.Text()),
]


def _named_args(sql: str) -> dict[str, str]:
    return dict(re.findall(r"(\w+)\s*=>\s*('(?:''|[^'])*'|[^,\n]+)", sql))


def _mapped(name: str, prop_type: dm.PropertyType) -> dm.MappedProperty:
    return dm.MappedProperty(
        container=dm.ContainerId("dm_dom_lims_result", "LimsResult"),
        container_property_identifier=name,
        type=prop_type,
        nullable=True,
        immutable=False,
        auto_increment=False,
    )


@pytest.fixture
def lims_view() -> dm.View:
    return dm.View(
        space="dm_dom_lims_result",
        external_id="LimsResult",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="LIMS result",
        description="",
        properties={name: _mapped(name, prop_type) for name, prop_type in LIM_COLUMNS},
        filter=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
    )


@pytest.fixture
def lims_model(lims_view: dm.View) -> dm.DataModel[dm.View]:
    return dm.DataModel(
        space="dm_dom_lims_result",
        external_id="LimsResult_DOM",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="LIMS result DOM",
        description=None,
        is_global=False,
        views=[lims_view],
    )


@pytest.fixture
def generator(
    mock_workspace_client: MagicMock,
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    lims_model: dm.DataModel[dm.View],
) -> UDTFGenerator:
    from cognite.pygen_spark import SparkUDTFGenerator

    return UDTFGenerator(
        workspace_client=mock_workspace_client,
        cognite_client=mock_cognite_client,
        catalog=CATALOG,
        schema=SCHEMA,
        code_generator=SparkUDTFGenerator(
            client=mock_cognite_client, output_dir=temp_output_dir, data_model=lims_model
        ),
    )


def test_view_sql_passes_every_property_pushdown_param_and_base_url(generator: UDTFGenerator) -> None:
    view_sql = generator.code_generator.generate_views(
        secret_scope=SECRET_SCOPE, catalog=CATALOG, schema=SCHEMA
    ).view_sqls["LimsResult"]
    names = re.findall(r"(\w+)\s*=>", view_sql)

    assert names == [
        *CREDENTIALS,
        *[name for name, _prop in LIM_COLUMNS],
        *data_model_pushdown_parameters.names,
        base_url_parameter.name,
    ]
    args = _named_args(view_sql)
    assert args["componentName"].strip() == "NULL"
    assert args["numericValue"].strip() == "NULL"
    assert args["instance_space"].strip() == "NULL"
    assert args["_row_limit"].strip() == "NULL"
    compact_sql = re.sub(r"\s+", "", view_sql)
    assert f"base_url=>SECRET('{SECRET_SCOPE}','base_url')" in compact_sql
    assert names[-1] == "base_url"


def test_text_min_stays_in_spark_and_numeric_max_is_pushed(generator: UDTFGenerator, lims_view: dm.View) -> None:
    metadata = DataModelViewMetadata.from_view(lims_view)
    text_min = f"SELECT min(componentName) AS m FROM {VIEW_SQL_NAME}"
    numeric_max = f"SELECT max(numericValue) AS m FROM {VIEW_SQL_NAME} WHERE space = 'inst_lims_result_lab_a'"

    assert generator.rewrite_query(text_min, secret_scope=SECRET_SCOPE) is None
    rewritten = generator.rewrite_query(numeric_max, secret_scope=SECRET_SCOPE)

    assert rewritten is not None
    names = re.findall(r"(\w+)\s*=>", rewritten)
    assert names == [
        *CREDENTIALS,
        *[name for name, _prop in LIM_COLUMNS],
        *data_model_pushdown_parameters.names,
        base_url_parameter.name,
    ]
    args = _named_args(rewritten)
    assert args["componentName"].strip() == "NULL"
    assert args["instance_space"] == "'inst_lims_result_lab_a'"
    assert "numericValue" in args["_aggregates"]
    assert metadata.is_numeric("numericValue")
    assert not metadata.is_numeric("componentName")
    assert (
        DataModelQueryRewriter.rewrite_to_udtf_sql(text_min, secret_scope=SECRET_SCOPE, view_metadata=metadata) is None
    )
