"""CLI filter token parser: converts CLI triples to FilterExpression."""

from __future__ import annotations

import json
from typing import Any

from ontologia.filters import ComparisonExpression, FilterExpression, LogicalExpression

# Map CLI operator tokens to internal operator strings
_OP_MAP: dict[str, str] = {
    "eq": "==",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
    "is_null": "IS_NULL",
}


def parse_cli_filters(triples: list[tuple[str, str, str]]) -> FilterExpression | None:
    """Parse CLI filter triples (PATH, OP, VALUE_JSON) into a FilterExpression.

    Multiple filters are AND-combined.
    """
    if not triples:
        return None

    exprs: list[FilterExpression] = []
    for path, op_token, value_json in triples:
        op = _OP_MAP.get(op_token)
        if op is None:
            raise ValueError(
                f"Unknown filter operator '{op_token}'. "
                f"Valid operators: {', '.join(sorted(_OP_MAP.keys()))}"
            )

        value: Any = None
        if op not in ("IS_NULL",):
            value = json.loads(value_json)

        exprs.append(ComparisonExpression(field_path=path, op=op, value=value))

    if len(exprs) == 1:
        return exprs[0]
    return LogicalExpression(op="AND", children=exprs)
