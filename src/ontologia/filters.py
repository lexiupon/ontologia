"""Filter expression types for the Ontologia query DSL."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# --- Path validation helpers (RFC 0006 §3.2) ---

_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_segment(segment: str) -> None:
    """Validate a single path segment (identifier)."""
    if not _SEGMENT_RE.match(segment):
        raise ValueError(f"Invalid path segment '{segment}': must match [A-Za-z_][A-Za-z0-9_]*")


def _validate_path(path: str) -> None:
    """Validate a dotted sub-path (one or more segments)."""
    if not path:
        raise ValueError("Path must not be empty")
    for segment in path.split("."):
        _validate_segment(segment)


def resolve_nested_path(data: dict[str, Any], dotted_path: str) -> Any:
    """Resolve a dotted path against a nested dict, returning None on missing keys."""
    current: Any = data
    for segment in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
        if current is None:
            return None
    return current


NULL_EQ_ERROR = "Use .is_null() instead of == None in Ontologia query expressions."
NULL_NE_ERROR = "Use .is_not_null() instead of != None in Ontologia query expressions."
TRUE_EQ_ERROR = "Use .is_true() instead of == True in Ontologia query expressions."
FALSE_EQ_ERROR = "Use .is_false() instead of == False in Ontologia query expressions."
TRUE_NE_ERROR = "Use .is_false() instead of != True in Ontologia query expressions."
FALSE_NE_ERROR = "Use .is_true() instead of != False in Ontologia query expressions."


class FilterExpression:
    """Base class for filter expressions."""

    def __and__(self, other: FilterExpression) -> LogicalExpression:
        return LogicalExpression(op="AND", children=[self, other])

    def __or__(self, other: FilterExpression) -> LogicalExpression:
        return LogicalExpression(op="OR", children=[self, other])

    def __invert__(self) -> LogicalExpression:
        return LogicalExpression(op="NOT", children=[self])


@dataclass
class ComparisonExpression(FilterExpression):
    """A comparison between a field path and a value.

    field_path uses dot notation:
      - "$.name" for direct field access
      - "left.$.tier" for left endpoint field
      - "right.$.price" for right endpoint field
    """

    field_path: str
    op: str  # "==", "!=", ">", ">=", "<", "<=", "LIKE", "IN", "IS_NULL", "IS_NOT_NULL"
    value: Any = None

    def __hash__(self) -> int:
        v = self.value
        if isinstance(v, list):
            v = tuple(v)
        return hash((self.field_path, self.op, v))

    def __eq__(self, other: object) -> bool:  # type: ignore[override]
        if not isinstance(other, ComparisonExpression):
            return NotImplemented
        return (
            self.field_path == other.field_path
            and self.op == other.op
            and self.value == other.value
        )


@dataclass
class LogicalExpression(FilterExpression):
    """A logical combination of filter expressions."""

    op: str  # "AND", "OR", "NOT"
    children: list[FilterExpression] = field(default_factory=list)


class EndpointProxy:
    """Proxy for accessing endpoint entity fields in relation queries.

    Usage: left(Subscription).tier == "Gold"
    """

    def __init__(self, prefix: str, relation_type: type) -> None:
        self._prefix = prefix
        self._relation_type = relation_type

    def __getattr__(self, name: str) -> FieldProxy:
        return FieldProxy(f"{self._prefix}.$.{name}")


class FieldProxy:
    """Proxy that generates FilterExpression from field operations."""

    def __init__(self, field_path: str) -> None:
        self._field_path = field_path

    def __eq__(self, other: object) -> ComparisonExpression:  # type: ignore[override]
        if other is None:
            raise TypeError(NULL_EQ_ERROR)
        if other is True:
            raise TypeError(TRUE_EQ_ERROR)
        if other is False:
            raise TypeError(FALSE_EQ_ERROR)
        return ComparisonExpression(self._field_path, "==", other)

    def __ne__(self, other: object) -> ComparisonExpression:  # type: ignore[override]
        if other is None:
            raise TypeError(NULL_NE_ERROR)
        if other is True:
            raise TypeError(TRUE_NE_ERROR)
        if other is False:
            raise TypeError(FALSE_NE_ERROR)
        return ComparisonExpression(self._field_path, "!=", other)

    def __gt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, ">", other)

    def __ge__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, ">=", other)

    def __lt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "<", other)

    def __le__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "<=", other)

    def startswith(self, prefix: str) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "LIKE", f"{prefix}%")

    def endswith(self, suffix: str) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "LIKE", f"%{suffix}")

    def contains(self, substring: str) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "LIKE", f"%{substring}%")

    def in_(self, values: list[Any]) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "IN", values)

    def is_null(self) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "IS_NULL")

    def is_not_null(self) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "IS_NOT_NULL")

    def is_true(self) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "==", True)

    def is_false(self) -> ComparisonExpression:
        return ComparisonExpression(self._field_path, "==", False)

    def path(self, sub_path: str) -> FieldProxy:
        """Navigate into a nested TypedDict field via dotted sub-path."""
        _validate_path(sub_path)
        return FieldProxy(f"{self._field_path}.{sub_path}")

    def __getitem__(self, segment: str) -> FieldProxy:
        """Navigate into a nested TypedDict field via single segment."""
        _validate_segment(segment)
        return FieldProxy(f"{self._field_path}.{segment}")

    def any_path(self, sub_path: str) -> AnyPathProxy:
        """Create an existential predicate proxy for a list-of-TypedDict field.

        Raises ValueError if called on an endpoint proxy (left.$/right.$).
        """
        if self._field_path.startswith("left.$.") or self._field_path.startswith("right.$."):
            raise ValueError("any_path() is not supported on endpoint proxies")
        _validate_path(sub_path)
        return AnyPathProxy(self._field_path, sub_path)


@dataclass
class ExistsComparisonExpression(FilterExpression):
    """An existential predicate over list-of-TypedDict fields.

    Compiles to EXISTS (SELECT 1 FROM json_each(...) WHERE ...).
    """

    list_field_path: str  # e.g. "$.events"
    item_path: str  # e.g. "kind" (dotted path within each list item)
    op: str  # same ops as ComparisonExpression
    value: Any = None

    def __hash__(self) -> int:
        v = self.value
        if isinstance(v, list):
            v = tuple(v)
        return hash((self.list_field_path, self.item_path, self.op, v))

    def __eq__(self, other: object) -> bool:  # type: ignore[override]
        if not isinstance(other, ExistsComparisonExpression):
            return NotImplemented
        return (
            self.list_field_path == other.list_field_path
            and self.item_path == other.item_path
            and self.op == other.op
            and self.value == other.value
        )


class AnyPathProxy:
    """Proxy for existential predicates on list-of-TypedDict fields.

    Usage: User.events.any_path("kind") == "click"
    """

    def __init__(self, list_field_path: str, item_path: str) -> None:
        self._list_field_path = list_field_path
        self._item_path = item_path

    def __eq__(self, other: object) -> ExistsComparisonExpression:  # type: ignore[override]
        if other is None:
            raise TypeError(NULL_EQ_ERROR)
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "==", other)

    def __ne__(self, other: object) -> ExistsComparisonExpression:  # type: ignore[override]
        if other is None:
            raise TypeError(NULL_NE_ERROR)
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "!=", other)

    def __gt__(self, other: Any) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, ">", other)

    def __ge__(self, other: Any) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, ">=", other)

    def __lt__(self, other: Any) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "<", other)

    def __le__(self, other: Any) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "<=", other)

    def in_(self, values: list[Any]) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "IN", values)

    def is_null(self) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "IS_NULL")

    def is_not_null(self) -> ExistsComparisonExpression:
        return ExistsComparisonExpression(self._list_field_path, self._item_path, "IS_NOT_NULL")


def left(relation_type: type) -> EndpointProxy:
    """Create a proxy for accessing left endpoint fields in relation queries."""
    return EndpointProxy("left", relation_type)


def right(relation_type: type) -> EndpointProxy:
    """Create a proxy for accessing right endpoint fields in relation queries."""
    return EndpointProxy("right", relation_type)
