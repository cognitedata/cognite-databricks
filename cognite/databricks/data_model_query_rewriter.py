"""SQL rewriter for data-model catalog view pushdown to CDF UDTF parameters.

Extends the time-series-oriented ``SQLQueryAnalyzer`` with WHERE/LIMIT/aggregate
rewrites that bind into generated data-model UDTF parameters
(``_exists``, ``_row_limit``, ``_query_mode``, instance identity, ranges).
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

from cognite.databricks.utils import to_udtf_function_name
from cognite.pygen_spark.udtf_parameters import base_url_parameter, data_model_pushdown_parameters

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View

# CDF instances/aggregate only supports MIN / MAX on numeric properties
NUMERIC_VALUE_KINDS = frozenset({"long", "double"})
COUNT_PROPERTIES = frozenset({"externalId", "external_id"})
IDENTITY_COLUMNS = frozenset({"space", "external_id", "externalId"})
_GROUP_BY_PATTERN = re.compile(
    r"\bgroup\s+by\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*)\s*(?:$|\border\s+by\b|\blimit\b|\bhaving\b)",
    flags=re.IGNORECASE,
)


class ViewColumn(BaseModel):
    """A queryable property column of a data model view."""

    column: str = Field(..., description="SQL column / UDTF parameter name (reserved words get a trailing _)")
    property: str = Field(..., description="CDF view property identifier")
    value_kind: str = Field(..., description="Spark value kind, e.g. string, long, double, timestamp")


class DataModelViewMetadata(BaseModel):
    """Columns of one view, so the rewriter only pushes what CDF and the generated UDTF can serve."""

    view_name: str
    columns: list[ViewColumn] = Field(default_factory=list)

    @property
    def by_column(self) -> dict[str, ViewColumn]:
        """Convenience property for dict-like access."""
        return {column.column: column for column in self.columns}

    def is_numeric(self, column: str) -> bool:
        """Whether CDF can aggregate MIN / MAX on this column."""
        view_column = self.by_column.get(column)
        return view_column is not None and view_column.value_kind in NUMERIC_VALUE_KINDS

    @classmethod
    def from_view(cls, view: View) -> DataModelViewMetadata:
        """Build metadata from the same UDTF fields used to generate the view's UDTF."""
        from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator

        return cls(
            view_name=view.external_id,
            columns=[
                ViewColumn(column=field.name, property=field.prop_name, value_kind=field.value_kind)
                for field in SparkMultiAPIGenerator.udtf_fields_for_view(view)
            ],
        )


class AggregateSelectItem(BaseModel):
    """One column of a rewritten aggregate select list, in the order the analyst wrote it."""

    kind: Literal["group", "aggregate"]
    column: str = ""
    fn: str = ""
    property: str = ""


class QueryNotPushdownCompatible(ValueError):
    """The statement cannot be pushed into the UDTF, so no SQL function is created."""

    def __init__(self, sql_query: str, reasons: list[str]) -> None:
        self.sql_query = sql_query
        self.reasons = reasons
        detail = "; ".join(reasons) if reasons else "nothing to push down"
        super().__init__(f"SQL statement is not compatible with UDTF pushdown: {detail}")


class SqlFunctionArgument(BaseModel):
    """One argument of a Unity Catalog SQL function created from a pushed statement."""

    name: str
    sql_type: str


class SqlFunctionColumn(BaseModel):
    """One column of the SQL function's RETURNS TABLE clause."""

    name: str
    sql_type: str


class CreatedSqlFunction(BaseModel):
    """A Unity Catalog SQL function whose body is a pushed UDTF call."""

    catalog: str
    schema_name: str
    name: str
    arguments: list[SqlFunctionArgument] = Field(default_factory=list)
    return_columns: list[SqlFunctionColumn] = Field(default_factory=list)
    statement: str

    @property
    def full_name(self) -> str:
        """Three-part function name."""
        return f"{self.catalog}.{self.schema_name}.{self.name}"


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
    aggregate_select: list[AggregateSelectItem] = Field(default_factory=list)
    pushdown_supported: bool = True
    skip_reasons: list[str] = Field(default_factory=list)
    # Filled from view metadata: CDF property -> SQL column (they differ for reserved words)
    column_for_property: dict[str, str] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class DataModelQueryRewriter:
    """Analyze and rewrite catalog SQL into UDTF calls with pushdown params."""

    @staticmethod
    def analyze(sql_query: str, view_metadata: DataModelViewMetadata | None = None) -> DataModelPushdown:
        """Extract data-model pushdown hints from a SQL query.

        Unsupported patterns (ORDER BY + LIMIT, OFFSET, joins, COUNT DISTINCT,
        HAVING, GROUP BY expressions, and a select list that is not the grouped
        columns plus aggregates) set ``pushdown_supported=False`` and leave list-scan defaults.

        With ``view_metadata``, columns the view does not have and MIN / MAX on non-numeric columns also
        disable pushdown, and JSON pushdown args use CDF property names.
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
        if view_metadata is not None:
            DataModelQueryRewriter._apply_view_metadata(result, view_metadata)
        return result

    @staticmethod
    def _apply_view_metadata(result: DataModelPushdown, metadata: DataModelViewMetadata) -> None:
        if result.view_name is not None and result.view_name != metadata.view_name:
            raise ValueError(f"View metadata is for '{metadata.view_name}', but the query reads '{result.view_name}'")
        by_column = metadata.by_column

        aggregate_columns = [m["property"] for m in result.aggregates if m["property"] not in COUNT_PROPERTIES]
        referenced = [
            *result.property_equals,
            *result.exists_properties,
            *result.not_exists_properties,
            *result.gt,
            *result.gte,
            *result.lt,
            *result.lte,
            *aggregate_columns,
            *result.group_by,
        ]
        unknown = sorted(
            {column for column in referenced if column not in by_column and column not in IDENTITY_COLUMNS}
        )
        if unknown:
            result.pushdown_supported = False
            result.skip_reasons.append(f"columns not pushed for view {metadata.view_name}: {', '.join(unknown)}")
            return

        non_numeric = sorted(
            {
                m["property"]
                for m in result.aggregates
                if m["fn"] in {"min", "max"} and not metadata.is_numeric(m["property"])
            }
        )
        if non_numeric:
            result.pushdown_supported = False
            result.skip_reasons.append(
                f"MIN/MAX on non-numeric column(s) {', '.join(non_numeric)} run in Spark "
                "(CDF aggregates only numeric properties)"
            )
            return

        # JSON pushdown args are resolved against CDF property names inside the UDTF
        def to_property(column: str) -> str:
            return by_column[column].property

        result.exists_properties = [to_property(c) for c in result.exists_properties]
        result.not_exists_properties = [to_property(c) for c in result.not_exists_properties]
        result.gt = {to_property(c): v for c, v in result.gt.items()}
        result.gte = {to_property(c): v for c, v in result.gte.items()}
        result.lt = {to_property(c): v for c, v in result.lt.items()}
        result.lte = {to_property(c): v for c, v in result.lte.items()}
        result.group_by = [_cdf_group_property(column, by_column) for column in result.group_by]
        result.aggregates = [
            m if m["property"] in COUNT_PROPERTIES else {**m, "property": to_property(m["property"])}
            for m in result.aggregates
        ]
        remapped_select: list[AggregateSelectItem] = []
        for item in result.aggregate_select:
            if item.kind == "aggregate" and item.property not in COUNT_PROPERTIES:
                remapped_select.append(item.model_copy(update={"property": to_property(item.property)}))
            else:
                remapped_select.append(item)
        result.aggregate_select = remapped_select
        result.column_for_property = {column.property: column.column for column in metadata.columns}

    @staticmethod
    def rewrite_to_udtf_sql(
        sql_query: str,
        *,
        udtf_fqn: str | None = None,
        secret_scope: str = "cdf_credentials",
        credential_args: dict[str, str] | None = None,
        view_metadata: DataModelViewMetadata | None = None,
    ) -> str | None:
        """Rewrite a simple catalog view query into a UDTF call with pushdown args.

        Args:
            view_metadata: Optional view columns; without it the rewriter cannot tell numeric from text
                columns or detect columns the view does not have, and the call only carries bound arguments
                (Unity Catalog requires all of them). With it, unbound parameters are passed as NULL.

        Returns:
            Rewritten SQL, or None when pushdown is not supported / nothing to push (run the SQL in Spark).
        """
        hints = DataModelQueryRewriter.analyze(sql_query, view_metadata=view_metadata)
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
            # Match registration: SmallBoat -> small_boat_udtf (not SmallBoat_udtf).
            udtf_fqn = f"{hints.catalog}.{hints.schema_name}.{to_udtf_function_name(hints.view_name)}"

        creds = {
            "client_id": f"SECRET('{secret_scope}', 'client_id')",
            "client_secret": f"SECRET('{secret_scope}', 'client_secret')",
            "tenant_id": f"SECRET('{secret_scope}', 'tenant_id')",
            "cdf_cluster": f"SECRET('{secret_scope}', 'cdf_cluster')",
            "project": f"SECRET('{secret_scope}', 'project')",
            base_url_parameter.name: f"SECRET('{secret_scope}', '{base_url_parameter.name}')",
            **(credential_args or {}),
        }

        args: list[str] = [
            f"client_id => {creds['client_id']}",
            f"client_secret => {creds['client_secret']}",
            f"tenant_id => {creds['tenant_id']}",
            f"cdf_cluster => {creds['cdf_cluster']}",
            f"project => {creds['project']}",
        ]

        property_args = {prop: _sql_literal(value) for prop, value in hints.property_equals.items()}
        pushdown_args = DataModelQueryRewriter._pushdown_args(hints)
        if view_metadata is None:
            args.extend(f"{name} => {value}" for name, value in {**property_args, **pushdown_args}.items())
        else:
            # Unity Catalog Python UDTFs cannot declare defaults: pass every parameter, NULL when unbound
            args.extend(f"{c.column} => {property_args.get(c.column, 'NULL')}" for c in view_metadata.columns)
            args.extend(f"{name} => {pushdown_args.get(name, 'NULL')}" for name in data_model_pushdown_parameters.names)
        args.append(f"{base_url_parameter.name} => {creds[base_url_parameter.name]}")

        # Aggregate rows are padded into the full UDTF outputSchema; select named columns.
        select_list = "*"
        if hints.query_mode == "aggregate" and hints.aggregates:
            select_list = _aggregate_select_sql(hints) or select_list

        return f"SELECT {select_list} FROM {udtf_fqn}(\n    " + ",\n    ".join(args) + "\n)"

    @staticmethod
    def build_sql_function(
        name: str,
        sql_query: str,
        *,
        secret_scope: str,
        view_metadata: DataModelViewMetadata,
    ) -> CreatedSqlFunction:
        """Build a CREATE FUNCTION statement for one pushed query.

        Literals in the statement become function arguments. Secrets stay SECRET() references.
        Raises QueryNotPushdownCompatible when rewrite_query would not push the statement.
        """
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Function name must be a SQL identifier: {name}")

        hints = DataModelQueryRewriter.analyze(sql_query, view_metadata=view_metadata)
        reasons = list(hints.skip_reasons)
        if not hints.pushdown_supported:
            if not reasons:
                reasons.append("the statement is not compatible with UDTF pushdown")
            raise QueryNotPushdownCompatible(sql_query, reasons)
        if reasons:
            raise QueryNotPushdownCompatible(sql_query, reasons)
        if hints.view_name is None or not hints.catalog or not hints.schema_name:
            raise QueryNotPushdownCompatible(sql_query, ["FROM clause must be a catalog.schema.view name"])

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
            raise QueryNotPushdownCompatible(sql_query, ["nothing would be pushed"])

        arguments, expressions = _function_arguments(hints, view_metadata)
        udtf_fqn = f"{hints.catalog}.{hints.schema_name}.{to_udtf_function_name(hints.view_name)}"
        body = _parameterized_udtf_sql(hints, udtf_fqn, secret_scope, view_metadata, expressions)
        return_columns = _return_columns(hints, view_metadata)
        signature = (
            "(\n  " + ",\n  ".join(f"{arg.name} {arg.sql_type}" for arg in arguments) + "\n)" if arguments else "()"
        )
        returns = ",\n  ".join(f"{column.name} {column.sql_type}" for column in return_columns)
        statement = (
            f"CREATE OR REPLACE FUNCTION {hints.catalog}.{hints.schema_name}.{name}{signature}\n"
            f"RETURNS TABLE (\n  {returns}\n)\n"
            "SQL SECURITY DEFINER\n"
            f"RETURN\n{body}"
        )
        return CreatedSqlFunction(
            catalog=hints.catalog,
            schema_name=hints.schema_name,
            name=name,
            arguments=arguments,
            return_columns=return_columns,
            statement=statement,
        )

    @staticmethod
    def _pushdown_args(hints: DataModelPushdown) -> dict[str, str]:
        """SQL literals for the bound pushdown parameters, keyed by parameter name."""
        args: dict[str, str] = {}
        if hints.instance_space is not None:
            args["instance_space"] = _sql_literal(hints.instance_space)
        if hints.external_id is not None:
            args["external_id"] = _sql_literal(hints.external_id)
        if hints.exists_properties:
            args["_exists"] = _sql_literal(json.dumps(hints.exists_properties))
        if hints.not_exists_properties:
            args["_not_exists"] = _sql_literal(json.dumps(hints.not_exists_properties))
        for name, bounds in (("_gt", hints.gt), ("_gte", hints.gte), ("_lt", hints.lt), ("_lte", hints.lte)):
            if bounds:
                args[name] = _sql_literal(json.dumps(bounds))
        if hints.row_limit is not None and hints.query_mode == "list":
            args["_row_limit"] = str(hints.row_limit)
        if hints.query_mode == "aggregate":
            args["_query_mode"] = "'aggregate'"
            args["_aggregates"] = _sql_literal(json.dumps(hints.aggregates))
            if hints.group_by:
                args["_group_by"] = _sql_literal(json.dumps(hints.group_by))
        return args

    @staticmethod
    def _extract_null_checks(where_clause: str, result: DataModelPushdown) -> None:
        # ``prop IS NOT NULL``
        for match in re.finditer(
            r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+is\s+not\s+null\b",
            where_clause,
            flags=re.IGNORECASE,
        ):
            prop = match.group(1)
            if prop.lower() not in {"space", "external_id"}:
                result.exists_properties.append(prop)

        # ``NOT prop IS NULL`` is equivalent to ``prop IS NOT NULL``.
        for match in re.finditer(
            r"\bnot\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+is\s+null\b",
            where_clause,
            flags=re.IGNORECASE,
        ):
            prop = match.group(1)
            if prop.lower() not in {"space", "external_id"} and prop not in result.exists_properties:
                result.exists_properties.append(prop)

        # ``prop IS NULL`` (does not match ``IS NOT NULL`` because of the ``not`` token).
        for match in re.finditer(
            r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+is\s+null\b",
            where_clause,
            flags=re.IGNORECASE,
        ):
            prop = match.group(1)
            prefix = where_clause[max(0, match.start() - 8) : match.start()].lower()
            if re.search(r"\bnot\s*$", prefix):
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
        select_match = re.search(r"\bselect\b(.+?)\bfrom\b", sql, flags=re.IGNORECASE)
        if not select_match:
            return
        select_clause = select_match.group(1).strip()
        has_group_by = bool(re.search(r"\bgroup\s+by\b", sql, flags=re.IGNORECASE))
        group_match = _GROUP_BY_PATTERN.search(sql)
        if has_group_by and group_match is None:
            result.pushdown_supported = False
            result.skip_reasons.append("GROUP BY expressions are not pushed")
            return
        group_columns = [part.strip() for part in group_match.group(1).split(",")] if group_match else []

        if select_clause == "*":
            if has_group_by:
                result.pushdown_supported = False
                result.skip_reasons.append("GROUP BY requires the grouped columns in the select list")
            return

        items = [_parse_select_item(part) for part in _split_select_items(select_clause)]
        if any(item is None for item in items):
            if has_group_by:
                result.pushdown_supported = False
                result.skip_reasons.append("GROUP BY expressions are not pushed")
            return
        select_items = [item for item in items if item is not None]
        plain = [item.column for item in select_items if item.kind == "group"]
        metrics = [{"fn": item.fn, "property": item.property} for item in select_items if item.kind == "aggregate"]

        if set(plain) != set(group_columns):
            if has_group_by:
                result.pushdown_supported = False
                result.skip_reasons.append(
                    "GROUP BY columns must be selected, and every selected column must be grouped or aggregated"
                )
            return
        if has_group_by and not metrics:
            result.pushdown_supported = False
            result.skip_reasons.append("GROUP BY without count, min, or max is not pushed")
            return

        for metric in metrics:
            grouped = {column.lower() for column in group_columns}
            if metric["fn"] == "count" and metric["property"] in COUNT_PROPERTIES and "external_id" in grouped:
                result.pushdown_supported = False
                result.skip_reasons.append("GROUP BY external_id collides with count, which is returned on external_id")
                return
            if metric["fn"] in {"min", "max"} and metric["property"] in group_columns:
                result.pushdown_supported = False
                result.skip_reasons.append(
                    f"GROUP BY {metric['property']} collides with {metric['fn']} on the same output column"
                )
                return

        if metrics:
            result.query_mode = "aggregate"
            result.aggregates = metrics
            result.aggregate_select = select_items
            result.group_by = plain
            # LIMIT on aggregates is a groupBy bucket cap, not a list row limit
            result.row_limit = None


def _split_select_items(select_clause: str) -> list[str]:
    """Split a select list on commas that are not inside parentheses."""
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in select_clause:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


_AGGREGATE_ITEM = re.compile(
    r"^(count|min|max)\s*\(\s*(\*|[a-zA-Z_][a-zA-Z0-9_]*)\s*\)(?:\s+(?:as\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?$",
    flags=re.IGNORECASE,
)
_COLUMN_ITEM = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(?:as\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?$",
    flags=re.IGNORECASE,
)


def _parse_select_item(part: str) -> AggregateSelectItem | None:
    """Parse one select item into a group column or a count/min/max."""
    aggregate = _AGGREGATE_ITEM.match(part.strip())
    if aggregate:
        fn = aggregate.group(1).lower()
        target = aggregate.group(2)
        if target == "*":
            if fn != "count":
                return None
            return AggregateSelectItem(kind="aggregate", fn="count", property="externalId")
        return AggregateSelectItem(kind="aggregate", fn=fn, property=target, column=target)
    column = _COLUMN_ITEM.match(part.strip())
    if column:
        return AggregateSelectItem(kind="group", column=column.group(1))
    return None


def _cdf_group_property(column: str, by_column: dict[str, ViewColumn]) -> str:
    """CDF groupBy name for a SQL column. Identity columns are not view properties."""
    if column == "space":
        return "space"
    if column in {"external_id", "externalId"}:
        return "externalId"
    return by_column[column].property


def _aggregate_select_sql(hints: DataModelPushdown) -> str:
    """Render the aggregate select list, including grouped columns."""
    parts: list[str] = []
    for item in hints.aggregate_select:
        if item.kind == "group":
            parts.append(item.column)
            continue
        if item.fn == "count" and item.property in COUNT_PROPERTIES:
            parts.append("external_id AS count_externalId")
            continue
        column = hints.column_for_property.get(item.property, item.property)
        parts.append(f"{column} AS {item.fn}_{column}")
    return ", ".join(parts)


def _sql_type_for_value_kind(value_kind: str) -> str:
    """SQL type for a view column. Long stays BIGINT so a CDF long is not narrowed to INT."""
    from pyspark.sql.types import BooleanType, DateType, DoubleType, LongType, StringType, TimestampType

    from cognite.databricks.type_converter import TypeConverter

    spark_types = {
        "string": StringType(),
        "long": LongType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
    }
    spark_type = spark_types.get(value_kind)
    if spark_type is None:
        return "STRING"
    sql_type, _ = TypeConverter.spark_to_sql_type_info(spark_type)
    if value_kind == "long":
        return "BIGINT"
    return sql_type


def _array_sql_type(values: list[object]) -> str:
    if values and all(isinstance(value, bool) for value in values):
        return "ARRAY<BOOLEAN>"
    if values and all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return "ARRAY<BIGINT>"
    if values and all(isinstance(value, int | float) and not isinstance(value, bool) for value in values):
        return "ARRAY<DOUBLE>"
    return "ARRAY<STRING>"


def _function_arguments(
    hints: DataModelPushdown, metadata: DataModelViewMetadata
) -> tuple[list[SqlFunctionArgument], dict[str, str]]:
    """Function arguments, and the UDTF expression each pushed parameter should use."""
    arguments: list[SqlFunctionArgument] = []
    expressions: dict[str, str] = {}

    if isinstance(hints.instance_space, list):
        arguments.append(SqlFunctionArgument(name="space", sql_type="ARRAY<STRING>"))
        expressions["instance_space"] = "to_json(space)"
    elif hints.instance_space is not None:
        arguments.append(SqlFunctionArgument(name="space", sql_type="STRING"))
        expressions["instance_space"] = "space"

    if isinstance(hints.external_id, list):
        arguments.append(SqlFunctionArgument(name="external_id", sql_type=_array_sql_type(hints.external_id)))
        expressions["external_id"] = "to_json(external_id)"
    elif hints.external_id is not None:
        arguments.append(SqlFunctionArgument(name="external_id", sql_type="STRING"))
        expressions["external_id"] = "external_id"

    for column in metadata.columns:
        if column.column not in hints.property_equals:
            continue
        value = hints.property_equals[column.column]
        if isinstance(value, list):
            arguments.append(SqlFunctionArgument(name=column.column, sql_type=_array_sql_type(value)))
            expressions[column.column] = f"to_json({column.column})"
        else:
            arguments.append(
                SqlFunctionArgument(name=column.column, sql_type=_sql_type_for_value_kind(column.value_kind))
            )
            expressions[column.column] = column.column

    for operator in ("gt", "gte", "lt", "lte"):
        bounds: dict[str, object] = getattr(hints, operator)
        if not bounds:
            continue
        pairs: list[str] = []
        for column in metadata.columns:
            if column.property not in bounds:
                continue
            parameter = f"{column.column}_{operator}"
            arguments.append(SqlFunctionArgument(name=parameter, sql_type=_sql_type_for_value_kind(column.value_kind)))
            pairs.append(f"'{column.property}', {parameter}")
        if pairs:
            expressions[f"_{operator}"] = f"to_json(map({', '.join(pairs)}))"

    if hints.exists_properties:
        expressions["_exists"] = _sql_literal(json.dumps(hints.exists_properties))
    if hints.not_exists_properties:
        expressions["_not_exists"] = _sql_literal(json.dumps(hints.not_exists_properties))
    if hints.row_limit is not None and hints.query_mode == "list":
        arguments.append(SqlFunctionArgument(name="row_limit", sql_type="BIGINT"))
        expressions["_row_limit"] = "row_limit"
    if hints.query_mode == "aggregate":
        expressions["_query_mode"] = "'aggregate'"
        expressions["_aggregates"] = _sql_literal(json.dumps(hints.aggregates))
        if hints.group_by:
            expressions["_group_by"] = _sql_literal(json.dumps(hints.group_by))
    return arguments, expressions


def _parameterized_udtf_sql(
    hints: DataModelPushdown,
    udtf_fqn: str,
    secret_scope: str,
    metadata: DataModelViewMetadata,
    expressions: dict[str, str],
) -> str:
    """UDTF call with SECRET() credentials and function arguments in place of literals."""
    args = [
        f"client_id => SECRET('{secret_scope}', 'client_id')",
        f"client_secret => SECRET('{secret_scope}', 'client_secret')",
        f"tenant_id => SECRET('{secret_scope}', 'tenant_id')",
        f"cdf_cluster => SECRET('{secret_scope}', 'cdf_cluster')",
        f"project => SECRET('{secret_scope}', 'project')",
    ]
    args.extend(f"{column.column} => {expressions.get(column.column, 'NULL')}" for column in metadata.columns)
    args.extend(f"{name} => {expressions.get(name, 'NULL')}" for name in data_model_pushdown_parameters.names)
    args.append(f"{base_url_parameter.name} => SECRET('{secret_scope}', '{base_url_parameter.name}')")
    select_list = "*"
    if hints.query_mode == "aggregate" and hints.aggregates:
        select_list = _aggregate_select_sql(hints) or select_list
    return f"SELECT {select_list} FROM {udtf_fqn}(\n    " + ",\n    ".join(args) + "\n)"


def _return_columns(hints: DataModelPushdown, metadata: DataModelViewMetadata) -> list[SqlFunctionColumn]:
    """RETURNS TABLE columns. A list query returns the UDTF row. An aggregate returns the rewritten select."""
    if hints.query_mode != "aggregate" or not hints.aggregate_select:
        columns = [
            SqlFunctionColumn(name=column.column, sql_type=_sql_type_for_value_kind(column.value_kind))
            for column in metadata.columns
        ]
        columns.extend(
            [
                SqlFunctionColumn(name="space", sql_type="STRING"),
                SqlFunctionColumn(name="external_id", sql_type="STRING"),
                SqlFunctionColumn(name="createdTime", sql_type="TIMESTAMP"),
                SqlFunctionColumn(name="lastUpdatedTime", sql_type="TIMESTAMP"),
                SqlFunctionColumn(name="deletedTime", sql_type="TIMESTAMP"),
            ]
        )
        return columns

    columns = []
    for item in hints.aggregate_select:
        if item.kind == "group":
            if item.column in {"space", "external_id"}:
                sql_type = "STRING"
            else:
                sql_type = _sql_type_for_value_kind(metadata.by_column[item.column].value_kind)
            columns.append(SqlFunctionColumn(name=item.column, sql_type=sql_type))
            continue
        if item.fn == "count" and item.property in COUNT_PROPERTIES:
            columns.append(SqlFunctionColumn(name="count_externalId", sql_type="STRING"))
            continue
        column = hints.column_for_property.get(item.property, item.property)
        columns.append(
            SqlFunctionColumn(
                name=f"{item.fn}_{column}",
                sql_type=_sql_type_for_value_kind(metadata.by_column[column].value_kind),
            )
        )
    return columns


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
    """Parse SQL IN-list contents, preserving commas inside quoted strings."""
    items: list[Any] = []
    pattern = r"'((?:''|[^'])*)'|(-?\d+(?:\.\d+)?)"
    for match in re.finditer(pattern, raw):
        str_val, num_val = match.groups()
        if str_val is not None:
            items.append(str_val.replace("''", "'"))
        elif num_val is not None:
            items.append(float(num_val) if "." in num_val else int(num_val))
    return items
