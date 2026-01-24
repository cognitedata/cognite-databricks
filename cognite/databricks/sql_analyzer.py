"""SQL query analyzer for extracting pushdown hints for time series UDTFs."""

from __future__ import annotations

import re

from pydantic import BaseModel, Field


class PushdownHints(BaseModel):
    """Hints extracted from SQL query for CDF API pushdown."""

    start_hint: str | None = Field(default=None, description="Start timestamp hint derived from WHERE clause")
    end_hint: str | None = Field(default=None, description="End timestamp hint derived from WHERE clause")
    aggregate_hint: str | None = Field(default=None, description="Aggregate hint derived from SELECT clause")
    granularity_hint: str | None = Field(default=None, description="Granularity hint derived from GROUP BY clause")


class SQLQueryAnalyzer(BaseModel):
    """Analyze SQL queries to extract pushdown hints for time series UDTFs."""

    @staticmethod
    def extract_pushdown_hints(sql_query: str) -> PushdownHints:
        """Extract pushdown hints from SQL query patterns.

        Args:
            sql_query: SQL query string

        Returns:
            PushdownHints with start/end/aggregate/granularity values when detected
        """
        normalized = " ".join(sql_query.strip().split())
        start_hint = SQLQueryAnalyzer._extract_start_hint(normalized)
        end_hint = SQLQueryAnalyzer._extract_end_hint(normalized)
        aggregate_hint = SQLQueryAnalyzer._extract_aggregate_hint(normalized)
        granularity_hint = SQLQueryAnalyzer._extract_granularity_hint(normalized)

        return PushdownHints(
            start_hint=start_hint,
            end_hint=end_hint,
            aggregate_hint=aggregate_hint,
            granularity_hint=granularity_hint,
        )

    @staticmethod
    def sql_aggregate_to_cdf(sql_agg: str) -> str | None:
        """Map SQL aggregate to CDF aggregate name.

        Args:
            sql_agg: SQL aggregate name (e.g., AVG, SUM)

        Returns:
            CDF aggregate name or None if unsupported
        """
        mapping = {
            "avg": "average",
            "average": "average",
            "sum": "sum",
            "min": "min",
            "max": "max",
            "count": "count",
        }
        return mapping.get(sql_agg.strip().lower())

    @staticmethod
    def _extract_start_hint(sql_query: str) -> str | None:
        """Extract start timestamp hint from WHERE clause."""
        between_match = re.search(
            r"timestamp\s+between\s+['\"]([^'\"]+)['\"]\s+and\s+['\"]([^'\"]+)['\"]",
            sql_query,
            flags=re.IGNORECASE,
        )
        if between_match:
            return between_match.group(1)

        gte_match = re.search(r"timestamp\s*>=\s*['\"]([^'\"]+)['\"]", sql_query, flags=re.IGNORECASE)
        if gte_match:
            return gte_match.group(1)

        gt_match = re.search(r"timestamp\s*>\s*['\"]([^'\"]+)['\"]", sql_query, flags=re.IGNORECASE)
        if gt_match:
            return gt_match.group(1)

        return None

    @staticmethod
    def _extract_end_hint(sql_query: str) -> str | None:
        """Extract end timestamp hint from WHERE clause."""
        between_match = re.search(
            r"timestamp\s+between\s+['\"]([^'\"]+)['\"]\s+and\s+['\"]([^'\"]+)['\"]",
            sql_query,
            flags=re.IGNORECASE,
        )
        if between_match:
            return between_match.group(2)

        lte_match = re.search(r"timestamp\s*<=\s*['\"]([^'\"]+)['\"]", sql_query, flags=re.IGNORECASE)
        if lte_match:
            return lte_match.group(1)

        lt_match = re.search(r"timestamp\s*<\s*['\"]([^'\"]+)['\"]", sql_query, flags=re.IGNORECASE)
        if lt_match:
            return lt_match.group(1)

        return None

    @staticmethod
    def _extract_aggregate_hint(sql_query: str) -> str | None:
        """Extract aggregate hint from SELECT clause."""
        match = re.search(r"\b(avg|sum|min|max|count)\s*\(\s*value\s*\)", sql_query, flags=re.IGNORECASE)
        if not match:
            return None
        return SQLQueryAnalyzer.sql_aggregate_to_cdf(match.group(1))

    @staticmethod
    def _extract_granularity_hint(sql_query: str) -> str | None:
        """Extract granularity from GROUP BY date expression.

        Looks for patterns like:
        date_add('2025-01-01', CAST(FLOOR(datediff(timestamp, '2025-01-01') / 14) * 14 AS INT))
        """
        match = re.search(
            r"datediff\s*\(\s*timestamp\s*,\s*['\"][^'\"]+['\"]\s*\)\s*/\s*(\d+)\s*\)\s*\*\s*\1",
            sql_query,
            flags=re.IGNORECASE,
        )
        if match:
            return f"{int(match.group(1))}d"

        return None
