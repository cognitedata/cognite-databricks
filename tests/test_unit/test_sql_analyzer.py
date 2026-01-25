"""Unit tests for SQLQueryAnalyzer."""

from __future__ import annotations

from cognite.databricks.sql_analyzer import SQLQueryAnalyzer


def test_extract_pushdown_hints_basic() -> None:
    sql = """
    SELECT AVG(value) AS avg_value
    FROM time_series
    WHERE timestamp >= '2025-01-01' AND timestamp <= '2025-12-31'
    GROUP BY date_add('2025-01-01', CAST(FLOOR(datediff(timestamp, '2025-01-01') / 14) * 14 AS INT))
    """

    hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

    assert hints.start_hint == "2025-01-01"
    assert hints.end_hint == "2025-12-31"
    assert hints.aggregate_hint == "average"
    assert hints.granularity_hint == "14d"


def test_extract_pushdown_hints_between() -> None:
    sql = "SELECT SUM(value) FROM ts WHERE timestamp BETWEEN '2025-01-01' AND '2025-01-31'"

    hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

    assert hints.start_hint == "2025-01-01"
    assert hints.end_hint == "2025-01-31"
    assert hints.aggregate_hint == "sum"
    assert hints.granularity_hint is None


def test_sql_aggregate_to_cdf_mapping() -> None:
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("AVG") == "average"
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("sum") == "sum"
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("min") == "min"
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("max") == "max"
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("count") == "count"
    assert SQLQueryAnalyzer.sql_aggregate_to_cdf("median") is None


def test_extract_pushdown_hints_no_hints() -> None:
    sql = "SELECT value FROM time_series"

    hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

    assert hints.start_hint is None
    assert hints.end_hint is None
    assert hints.aggregate_hint is None
    assert hints.granularity_hint is None


def test_extract_pushdown_hints_partial_start_only() -> None:
    sql = "SELECT AVG(value) FROM ts WHERE timestamp >= '2025-01-01'"

    hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

    assert hints.start_hint == "2025-01-01"
    assert hints.end_hint is None
    assert hints.aggregate_hint == "average"
    assert hints.granularity_hint is None


def test_extract_pushdown_hints_unsupported_date_trunc_unit() -> None:
    sql = "SELECT AVG(value) FROM ts GROUP BY date_trunc('week', timestamp)"

    hints = SQLQueryAnalyzer.extract_pushdown_hints(sql)

    assert hints.granularity_hint is None
