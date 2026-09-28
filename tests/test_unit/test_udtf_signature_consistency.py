"""Unity Catalog UDTF signature must accept every named argument its callers pass.

Regression for 0.4.0: ``register_udtfs()`` registered secrets + view properties only, while the generated
UDTF and ``register_views()`` view SQL pass pushdown params (``instance_space``, ``_row_limit``, ...), so
``CREATE VIEW`` failed with ``UNRECOGNIZED_PARAMETER_NAME: instance_space``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

pytest.importorskip("pyspark")

from pyspark.sql.types import LongType

from cognite.databricks.data_model_query_rewriter import DataModelQueryRewriter
from cognite.databricks.generator import UDTFGenerator
from cognite.databricks.type_converter import TypeConverter

PUSHDOWN_NAMES = [
    "instance_space",
    "external_id",
    "_exists",
    "_not_exists",
    "_gt",
    "_gte",
    "_lt",
    "_lte",
    "_row_limit",
    "_query_mode",
    "_aggregates",
    "_group_by",
]


@pytest.fixture
def small_boat_model() -> dm.DataModel[dm.View]:
    view = dm.View(
        space="sailboat",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "name": dm.Text(),  # type: ignore[dict-item]
            "description": dm.Text(),  # type: ignore[dict-item]
            "boat_guid": dm.Text(),  # type: ignore[dict-item]
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="node",
        is_global=False,
    )
    return dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[view],
    )


@pytest.fixture
def generator(
    mock_workspace_client: MagicMock,
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    small_boat_model: dm.DataModel[dm.View],
) -> UDTFGenerator:
    from cognite.pygen_spark import SparkUDTFGenerator

    code_generator = SparkUDTFGenerator(
        client=mock_cognite_client,
        output_dir=temp_output_dir,
        data_model=small_boat_model,
    )
    return UDTFGenerator(
        workspace_client=mock_workspace_client,
        cognite_client=mock_cognite_client,
        catalog="f0connectortest",
        schema="sailboat_sailboat_v1",
        code_generator=code_generator,
    )


def _named_args(sql: str) -> list[str]:
    return re.findall(r"(\w+)\s*=>", sql)


def _eval_params(code: str, class_name: str) -> list[str]:
    for node in ast.walk(ast.parse(code)):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "eval":
                    return [arg.arg for arg in item.args.args if arg.arg != "self"]
    raise AssertionError(f"{class_name}.eval not found")


def test_signature_accepts_every_named_arg_in_generated_view_sql(generator: UDTFGenerator) -> None:
    signature = {p.name for p in generator._parse_udtf_params("SmallBoat")}
    view_sql = generator.code_generator.generate_views(
        secret_scope="cdf_sailboat_sailboat", catalog="f0connectortest", schema="sailboat_sailboat_v1"
    ).view_sqls["SmallBoat"]

    missing = [name for name in _named_args(view_sql) if name not in signature]
    assert missing == [], f"view SQL passes args the UC signature does not declare: {missing}"


def test_signature_matches_generated_udtf_eval_parameters(generator: UDTFGenerator) -> None:
    view = generator._get_view_by_id("SmallBoat")
    assert view is not None
    code = generator.code_generator.udtf_generator.generate_udtf(view, include_analyze=True, use_udtf_decorator=False)

    assert [p.name for p in generator._parse_udtf_params("SmallBoat")] == _eval_params(code, "SmallBoatUDTF")


def test_pushdown_params_are_optional_and_typed(generator: UDTFGenerator) -> None:
    by_name = {p.name: p for p in generator._parse_udtf_params("SmallBoat")}
    long_sql_type, _ = TypeConverter.spark_to_sql_type_info(LongType())

    for name in PUSHDOWN_NAMES:
        assert name in by_name, name
        assert by_name[name].parameter_default == "NULL", name
    assert by_name["_row_limit"].type_text == long_sql_type
    for name in PUSHDOWN_NAMES:
        if name != "_row_limit":
            assert by_name[name].type_text == "STRING", name


def test_class_fallback_types_pushdown_params(generator: UDTFGenerator) -> None:
    """When the view is missing from the model, the signature is parsed from the generated class."""

    class SmallBoatUDTF:
        def eval(
            self,
            client_id: str | None = None,
            client_secret: str | None = None,
            tenant_id: str | None = None,
            cdf_cluster: str | None = None,
            project: str | None = None,
            name: object | None = None,
            instance_space: object | None = None,
            _row_limit: object | None = None,
            _aggregates: object | None = None,
        ) -> None:
            return None

    by_name = {p.name: p for p in generator._parse_udtf_params_from_class(SmallBoatUDTF)}
    long_sql_type, _ = TypeConverter.spark_to_sql_type_info(LongType())

    assert by_name["instance_space"].type_text == "STRING"
    assert by_name["_aggregates"].type_text == "STRING"
    assert by_name["_row_limit"].type_text == long_sql_type


def test_rewriter_output_uses_only_declared_parameters(generator: UDTFGenerator) -> None:
    signature = {p.name for p in generator._parse_udtf_params("SmallBoat")}
    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(
        """
        SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat
        WHERE name = 'XBOX'
          AND description IS NOT NULL
          AND space = 'inst_sailboat_fleet_a'
        LIMIT 5
        """,
        secret_scope="cdf_sailboat_sailboat",
    )
    assert rewritten is not None

    undeclared = [name for name in _named_args(rewritten) if name not in signature]
    assert undeclared == [], f"rewriter emits args the UC signature does not declare: {undeclared}"
