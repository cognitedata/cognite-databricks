"""SQL rewriter for data-model catalog view pushdown to CDF UDTF parameters.

Extends the time-series-oriented ``SQLQueryAnalyzer`` with WHERE/LIMIT/aggregate
rewrites that bind into generated data-model UDTF parameters
(``_exists``, ``_row_limit``, ``_query_mode``, instance identity, ranges).
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import BaseModel, Field


class DataModelPushdown(BaseModel):
    """Hints extracted from a catalog SQL query for data-model UDTF pushdown."""

    catalog: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    view_name: str | None = None
    property_equals: dict[str, object] = Field(default_factory=dict)
    exists_properties: list[str] = Field(default_factory=list)
    not_exists_properties: list[str] = Field(default_factory=list)
    instance_space: str | list[str] | None = None
    external_id: str | list[str] | None = None
    gt: dict[str, object] = Field(default_factory=dict)
    gte: dict[str, object] = Field(default_factory=dict)
    lt: dict[str, object] = Field(default_factory=dict)
    lte: dict[str, object] = Field(default_factory=dict)
    row_limit: int | None = None
    query_mode: Literal["list", "aggregate"] = "list"
    aggregates: list[dict[str, str]] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    pushdown_supported: bool = True
    skip_reasons: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class DataModelQueryRewriter(BaseModel):
    """Analyze and rewrite catalog SQL into UDTF calls with pushdown params."""

    @staticmethod
    def analyze(sql_query: str) -> DataModelPushdown:
        """Extract data-model pushdown hints from a SQL query.

        Unsupported patterns (ORDER BY + LIMIT, OFFSET, joins, COUNT DISTINCT,
        HAVING) set ``pushdown_supported=False`` and leave list-scan defaults.
        """
        normalized = " ".join(sql_query.strip().split())
        result = DataModelPushdown()

        if re.search(r"\bjoin\b", normalized, flags=re.IGNORECASE):
            result.pushdown_supported = False
            result.skip_reasons.append("joins are not rewritten")
            return result

        if re.search(r"\bhaving\b", normalized, flags=re.IGNORECASE):
            result.pushdown_supported = False
            result.skip_reasons.append("HAVING is not rewritten")
            return result

        if re.search(r"\boffset\b", normalized, flags=re.IGNORECASE):
            result.pushdown_supported = False
            result.skip_reasons.append("OFFSET is not rewritten")
            return result

        if re.search(r"\bcount\s*\(\s*distinct\b", normalized, flags=re.IGNORECASE):
            result.pushdown_supported = False
            result.skip_reasons.append("COUNT(DISTINCT) is not rewritten")
            return result

        from_match = re.search(
            r"\bfrom\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
            normalized,
            flags=re.IGNORECASE,
        )
        if from_match:
            result.catalog = from_match.group(1)
            result.schema_name = from_match.group(2)
            result.view_name = from_match.group(3)

        where_match = re.search(
            r"\bwhere\b(.+?)(?:\border\s+by\b|\bgroup\s+by\b|\blimit\b|$)",
            normalized,
            flags=re.IGNORECASE,
        )
        where_clause = where_match.group(1).strip() if where_match else ""

        DataModelQueryRewriter._extract_null_checks(where_clause, result)
        DataModelQueryRewriter._extract_identity_filters(where_clause, result)
        DataModelQueryRewriter._extract_ranges(where_clause, result)
        DataModelQueryRewriter._extract_equals(where_clause, result)

        has_order_by = bool(re.search(r"\border\s+by\b", normalized, flags=re.IGNORECASE))
        limit_match = re.search(r"\blimit\s+(\d+)\b", normalized, flags=re.IGNORECASE)
        if limit_match:
            if has_order_by:
                result.skip_reasons.append("ORDER BY + LIMIT is not pushed (Spark sort may differ)")
            else:
                result.row_limit = int(limit_match.group(1))

        DataModelQueryRewriter._extract_aggregates(normalized, result)
        return result

    @staticmethod
    def rewrite_to_udtf_sql(
        sql_query: str,
        *,
        udtf_fqn: str | None = None,
        secret_scope: str = "cdf_credentials",
        credential_args: dict[str, str] | None = None,
    ) -> str | None:
        """Rewrite a simple catalog view query into a UDTF call with pushdown args.

        Returns:
            Rewritten SQL, or None when pushdown is not supported / nothing to push.
        """
        hints = DataModelQueryRewriter.analyze(sql_query)
        if not hints.pushdown_supported or hints.view_name is None:
            return None

        has_pushdown = bool(
            hints.property_equals
            or hints.exists_properties
            or hints.not_exists_properties
            or hints.instance_space is not None
            or hints.external_id is not None
            or hints.gt
            or hints.gte
            or hints.lt
            or hints.lte
            or hints.row_limit is not None
            or hints.query_mode == "aggregate"
        )
        if not has_pushdown:
            return None

        if udtf_fqn is None:
            if not (hints.catalog and hints.schema_name and hints.view_name):
                return None
            udtf_fqn = f"{hints.catalog}.{hints.schema_name}.{hints.view_name}_udtf"

        creds = credential_args or {
            "client_id": f"SECRET('{secret_scope}', 'client_id')",
            "client_secret": f"SECRET('{secret_scope}', 'client_secret')",
            "tenant_id": f"SECRET('{secret_scope}', 'tenant_id')",
            "cdf_cluster": f"SECRET('{secret_scope}', 'cdf_cluster')",
            "project": f"SECRET('{secret_scope}', 'project')",
        }

        args: list[str] = [
            f"client_id => {creds['client_id']}",
            f"client_secret => {creds['client_secret']}",
            f"tenant_id => {creds['tenant_id']}",
            f"cdf_cluster => {creds['cdf_cluster']}",
            f"project => {creds['project']}",
        ]

        for prop, value in hints.property_equals.items():
            args.append(f"{prop} => {_sql_literal(value)}")

        if hints.instance_space is not None:
            args.append(f"instance_space => {_sql_literal(hints.instance_space)}")
        if hints.external_id is not None:
            args.append(f"external_id => {_sql_literal(hints.external_id)}")
        if hints.exists_properties:
            args.append(f"_exists => {_sql_literal(json.dumps(hints.exists_properties))}")
        if hints.not_exists_properties:
            args.append(f"_not_exists => {_sql_literal(json.dumps(hints.not_exists_properties))}")
        if hints.gt:
            args.append(f"_gt => {_sql_literal(json.dumps(hints.gt))}")
        if hints.gte:
            args.append(f"_gte => {_sql_literal(json.dumps(hints.gte))}")
        if hints.lt:
            args.append(f"_lt => {_sql_literal(json.dumps(hints.lt))}")
        if hints.lte:
            args.append(f"_lte => {_sql_literal(json.dumps(hints.lte))}")
        if hints.row_limit is not None and hints.query_mode == "list":
            args.append(f"_row_limit => {hints.row_limit}")
        if hints.query_mode == "aggregate":
            args.append("_query_mode => 'aggregate'")
            args.append(f"_aggregates => {_sql_literal(json.dumps(hints.aggregates))}")
            if hints.group_by:
                args.append(f"_group_by => {_sql_literal(json.dumps(hints.group_by))}")

        select_list = "*"
        if hints.query_mode == "aggregate" and hints.aggregates:
            aliases: list[str] = []
            for i, metric in enumerate(hints.aggregates):
                aliases.append(f"col{i} AS {metric['fn']}_{metric['property']}")
            if aliases:
                select_list = ", ".join(aliases)

        return f"SELECT {select_list} FROM {udtf_fqn}(\n    " + ",\n    ".join(args) + "\n)"

    @staticmethod
    def _extract_null_checks(where_clause: str, result: DataModelPushdown) -> None:
        for match in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+is\s+not\s+null\b", where_clause, flags=re.IGNORECASE):
            prop = match.group(1)
            if prop.lower() not in {"space", "external_id"}:
                result.exists_properties.append(prop)
        for match in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+is\s+null\b", where_clause, flags=re.IGNORECASE):
            prop = match.group(1)
            # Avoid matching the "not null" already handled: require no "not" before is null
            start = match.start()
            prefix = where_clause[max(0, start - 4) : start].lower()
            if "not" in prefix:
                continue
            if prop.lower() not in {"space", "external_id"}:
                result.not_exists_properties.append(prop)

    @staticmethod
    def _extract_identity_filters(where_clause: str, result: DataModelPushdown) -> None:
        space_eq = re.search(r"\bspace\s*=\s*'([^']+)'", where_clause, flags=re.IGNORECASE)
        if space_eq:
            result.instance_space = space_eq.group(1)
        else:
            space_in = re.search(r"\bspace\s+in\s*\(([^)]+)\)", where_clause, flags=re.IGNORECASE)
            if space_in:
                result.instance_space = _parse_sql_string_list(space_in.group(1))

        ext_eq = re.search(r"\bexternal_id\s*=\s*'([^']+)'", where_clause, flags=re.IGNORECASE)
        if ext_eq:
            result.external_id = ext_eq.group(1)
        else:
            ext_in = re.search(r"\bexternal_id\s+in\s*\(([^)]+)\)", where_clause, flags=re.IGNORECASE)
            if ext_in:
                result.external_id = _parse_sql_string_list(ext_in.group(1))

    @staticmethod
    def _extract_ranges(where_clause: str, result: DataModelPushdown) -> None:
        for op, target in (
            (">=", "gte"),
            ("<=", "lte"),
            (">", "gt"),
            ("<", "lt"),
        ):
            pattern = rf"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*{re.escape(op)}\s*('([^']+)'|(-?\d+(?:\.\d+)?))"
            for match in re.finditer(pattern, where_clause, flags=re.IGNORECASE):
                prop = match.group(1)
                if prop.lower() in {"space", "external_id"}:
                    continue
                if match.group(3) is not None:
                    value: object = match.group(3)
                else:
                    num = match.group(4)
                    value = float(num) if "." in num else int(num)
                getattr(result, target)[prop] = value

    @staticmethod
    def _extract_equals(where_clause: str, result: DataModelPushdown) -> None:
        # Skip props already claimed as identity or null-check only
        for match in re.finditer(
            r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']+)'",
            where_clause,
            flags=re.IGNORECASE,
        ):
            prop = match.group(1)
            if prop.lower() in {"space", "external_id"}:
                continue
            result.property_equals[prop] = match.group(2)

        for match in re.finditer(
            r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+in\s*\(([^)]+)\)",
            where_clause,
            flags=re.IGNORECASE,
        ):
            prop = match.group(1)
            if prop.lower() in {"space", "external_id"}:
                continue
            result.property_equals[prop] = _parse_sql_string_list(match.group(2))

    @staticmethod
    def _extract_aggregates(sql: str, result: DataModelPushdown) -> None:
        # Only rewrite when SELECT is aggregate-only (no bare columns besides aggregates)
        select_match = re.search(r"\bselect\b(.+?)\bfrom\b", sql, flags=re.IGNORECASE)
        if not select_match:
            return
        select_clause = select_match.group(1).strip()
        if select_clause == "*":
            return

        metrics: list[dict[str, str]] = []
        for match in re.finditer(
            r"\b(count|min|max)\s*\(\s*(\*|([a-zA-Z_][a-zA-Z0-9_]*))\s*\)",
            select_clause,
            flags=re.IGNORECASE,
        ):
            fn = match.group(1).lower()
            if match.group(2) == "*":
                if fn != "count":
                    continue
                metrics.append({"fn": "count", "property": "externalId"})
            else:
                prop = match.group(3)
                metrics.append({"fn": fn, "property": prop})

        # Reject if select has non-aggregate identifiers beyond aliases
        stripped = re.sub(
            r"\b(count|min|max)\s*\(\s*(?:\*|[a-zA-Z_][a-zA-Z0-9_]*)\s*\)(?:\s+as\s+[a-zA-Z_][a-zA-Z0-9_]*)?",
            "",
            select_clause,
            flags=re.IGNORECASE,
        )
        leftover = re.sub(r"[, ]+", "", stripped)
        if leftover:
            return

        if metrics:
            result.query_mode = "aggregate"
            result.aggregates = metrics
            # LIMIT on aggregates is groupBy bucket cap, not list row limit
            result.row_limit = None

        group_match = re.search(
            r"\bgroup\s+by\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*)",
            sql,
            flags=re.IGNORECASE,
        )
        if group_match and metrics:
            result.group_by = [p.strip() for p in group_match.group(1).split(",")]


def _sql_literal(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, list):
        return _sql_literal(json.dumps(value))
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _parse_sql_string_list(raw: str) -> list[Any]:
    items: list[Any] = []
    for part in raw.split(","):
        part = part.strip()
        if part.startswith("'") and part.endswith("'"):
            items.append(part[1:-1])
        else:
            try:
                items.append(int(part) if "." not in part else float(part))
            except ValueError:
                items.append(part)
    return items
