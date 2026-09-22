"""pytest-bdd steps for data-model SQL rewrite."""

from __future__ import annotations

from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from cognite.databricks.data_model_query_rewriter import DataModelPushdown, DataModelQueryRewriter

scenarios("../features/data_model_sql_rewrite.feature")


@pytest.fixture
def rewrite_ctx() -> dict[str, Any]:
    return {"sql": "", "hints": None, "rewritten": None}


@given("the SQL query")
def given_sql(rewrite_ctx: dict[str, Any], docstring: str) -> None:
    rewrite_ctx["sql"] = docstring.strip()


@when("I analyze the data-model pushdown")
def when_analyze(rewrite_ctx: dict[str, Any]) -> None:
    rewrite_ctx["hints"] = DataModelQueryRewriter.analyze(rewrite_ctx["sql"])


@when("I rewrite the SQL to a UDTF call")
def when_rewrite(rewrite_ctx: dict[str, Any]) -> None:
    rewrite_ctx["rewritten"] = DataModelQueryRewriter.rewrite_to_udtf_sql(rewrite_ctx["sql"])


@then(parsers.parse('property equals should include {prop} as "{value}"'))
def then_equals(rewrite_ctx: dict[str, Any], prop: str, value: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert hints.property_equals.get(prop) == value


@then(parsers.parse('exists properties should include "{prop}"'))
def then_exists(rewrite_ctx: dict[str, Any], prop: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert prop in hints.exists_properties


@then(parsers.parse("row_limit should be {value}"))
def then_row_limit(rewrite_ctx: dict[str, Any], value: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    if value == "null":
        assert hints.row_limit is None
    else:
        assert hints.row_limit == int(value)


@then(parsers.parse('query_mode should be "{mode}"'))
def then_mode(rewrite_ctx: dict[str, Any], mode: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert hints.query_mode == mode


@then(parsers.parse('instance_space should be "{space}"'))
def then_space(rewrite_ctx: dict[str, Any], space: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert hints.instance_space == space


@then(parsers.parse('skip_reasons should mention "{text}"'))
def then_skip(rewrite_ctx: dict[str, Any], text: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert any(text in reason for reason in hints.skip_reasons)


@then(parsers.parse('the rewritten SQL should contain "{fragment}"'))
def then_contains(rewrite_ctx: dict[str, Any], fragment: str) -> None:
    assert rewrite_ctx["rewritten"] is not None
    assert fragment in rewrite_ctx["rewritten"]


@then("aggregates should include count on externalId")
def then_count_agg(rewrite_ctx: dict[str, Any]) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert {"fn": "count", "property": "externalId"} in hints.aggregates


@then(parsers.parse("aggregates should include {fn} on {prop}"))
def then_metric(rewrite_ctx: dict[str, Any], fn: str, prop: str) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert {"fn": fn, "property": prop} in hints.aggregates


@then("pushdown_supported should be false")
def then_unsupported(rewrite_ctx: dict[str, Any]) -> None:
    hints: DataModelPushdown = rewrite_ctx["hints"]
    assert hints.pushdown_supported is False
