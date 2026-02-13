"""Entity, Relation, Field, and Meta types for Ontologia."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Protocol, TypeVar, get_args

from pydantic import BaseModel, create_model

from ontologia.errors import MetadataUnavailableError
from ontologia.filters import (
    NULL_EQ_ERROR,
    NULL_NE_ERROR,
    ComparisonExpression,
    FieldProxy,
    FilterExpression,
)

T = TypeVar("T")
L = TypeVar("L")
R = TypeVar("R")

_SENTINEL = object()


class Field(Generic[T]):
    """Type-safe field descriptor for Entity and Relation schemas.

    Supports comparison operators that return FilterExpression for query building.
    """

    def __init__(
        self,
        default: Any = _SENTINEL,
        *,
        default_factory: Any | None = None,
        primary_key: bool = False,
        instance_key: bool = False,
        index: bool = False,
    ) -> None:
        self.default = default
        self.default_factory = default_factory
        self.primary_key = primary_key
        self.instance_key = instance_key
        self.index = index
        self.name: str = ""
        self.annotation: Any = None

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            # Class-level access returns a FieldProxy for query building
            return FieldProxy(f"$.{self.name}")
        return obj.__dict__.get(self.name, _SENTINEL)

    def __set__(self, obj: Any, value: Any) -> None:
        obj.__dict__[self.name] = value

    # Comparison operators for class-level query building (when accessed on class)
    def __eq__(self, other: object) -> ComparisonExpression | bool:  # type: ignore[override]
        if isinstance(other, Field):
            return self is other
        if other is None:
            raise TypeError(NULL_EQ_ERROR)
        return ComparisonExpression(f"$.{self.name}", "==", other)

    def __ne__(self, other: object) -> ComparisonExpression | bool:  # type: ignore[override]
        if isinstance(other, Field):
            return self is not other
        if other is None:
            raise TypeError(NULL_NE_ERROR)
        return ComparisonExpression(f"$.{self.name}", "!=", other)

    def __gt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(f"$.{self.name}", ">", other)

    def __ge__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(f"$.{self.name}", ">=", other)

    def __lt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(f"$.{self.name}", "<", other)

    def __le__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(f"$.{self.name}", "<=", other)

    def startswith(self, prefix: str) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "LIKE", f"{prefix}%")

    def endswith(self, suffix: str) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "LIKE", f"%{suffix}")

    def contains(self, substring: str) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "LIKE", f"%{substring}%")

    def in_(self, values: list[Any]) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "IN", values)

    def is_null(self) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "IS_NULL")

    def is_not_null(self) -> FilterExpression:
        return ComparisonExpression(f"$.{self.name}", "IS_NOT_NULL")

    def has_default(self) -> bool:
        return self.default is not _SENTINEL or self.default_factory is not None

    def get_default(self) -> Any:
        if self.default_factory is not None:
            return self.default_factory()
        if self.default is not _SENTINEL:
            return self.default
        raise ValueError(f"Field '{self.name}' has no default")


@dataclass
class Meta:
    """Metadata for query-hydrated Entity and Relation instances."""

    commit_id: int
    type_name: str
    key: str | None = None
    left_key: str | None = None
    right_key: str | None = None
    instance_key: str | None = None


class SupportsMeta(Protocol):
    """Protocol for objects that expose query metadata."""

    def meta(self) -> Meta: ...


def meta(obj: SupportsMeta) -> Meta:
    """Convenience wrapper for obj.meta()."""
    return obj.meta()


def _resolve_annotation(ann: Any, module_name: str) -> Any:
    """Resolve string annotations and extract the inner type from Field[T]."""
    # If it's a string, try to evaluate it
    if isinstance(ann, str):
        module = sys.modules.get(module_name, None)
        ns = vars(module) if module else {}
        try:
            ann = eval(ann, ns)  # noqa: S307
        except Exception:
            return Any

    origin = getattr(ann, "__origin__", None)
    if origin is Field:
        args = get_args(ann)
        return args[0] if args else Any
    return ann


def _collect_fields(cls: type, ns: dict[str, Any]) -> dict[str, Field[Any]]:
    """Collect Field descriptors from class annotations."""
    fields: dict[str, Field[Any]] = {}

    # Get annotations from the class (not parents)
    annotations = {}
    if "__annotations__" in cls.__dict__:
        annotations = cls.__dict__["__annotations__"]

    for name, ann in annotations.items():
        # Resolve the annotation to check if it's Field[T]
        resolved = _resolve_annotation(ann, cls.__module__)

        # Check if this annotation is Field[T]
        origin = getattr(ann, "__origin__", None)
        is_field_ann = origin is Field
        if isinstance(ann, str) and "Field" in ann:
            is_field_ann = True

        if not is_field_ann:
            continue

        # Get the Field descriptor from class dict or create one
        val = ns.get(name, _SENTINEL)
        if val is _SENTINEL:
            val = cls.__dict__.get(name, _SENTINEL)

        field_desc: Field[Any]
        if isinstance(val, Field):
            field_desc = val
        elif val is None:
            # `email: Field[str | None] = None` shorthand
            field_desc = Field(default=None)
        elif val is _SENTINEL:
            # Required field (no default)
            field_desc = Field()
        else:
            field_desc = Field(default=val)

        field_desc.name = name
        field_desc.annotation = resolved
        fields[name] = field_desc

        # Ensure the descriptor is set on the class
        if not isinstance(cls.__dict__.get(name), Field):
            setattr(cls, name, field_desc)

    return fields


def _build_pydantic_model(model_name: str, fields: dict[str, Field[Any]]) -> type[BaseModel]:
    """Build a Pydantic model from Field definitions."""
    pydantic_fields: dict[str, Any] = {}
    for name, f in fields.items():
        ann = f.annotation if f.annotation is not None else Any
        if f.default_factory is not None:
            from pydantic import Field as PydanticField

            pydantic_fields[name] = (ann, PydanticField(default_factory=f.default_factory))
        elif f.default is not _SENTINEL:
            pydantic_fields[name] = (ann, f.default)
        else:
            pydantic_fields[name] = (ann, ...)

    return create_model(model_name, **pydantic_fields)  # type: ignore[call-overload]


class Entity:
    """Base class for typed entities with automatic validation."""

    __entity_name__: ClassVar[str]
    __entity_fields__: ClassVar[tuple[str, ...]]
    _pydantic_model: ClassVar[type[BaseModel]]
    _field_definitions: ClassVar[dict[str, Field[Any]]]
    __onto_meta__: Meta | None = None

    def __init_subclass__(cls, name: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        cls.__entity_name__ = name or cls.__name__

        # Collect fields
        fields = _collect_fields(cls, {})
        cls._field_definitions = fields
        cls.__entity_fields__ = tuple(fields.keys())

        # Validate no instance_key on entities
        ik_fields = [n for n, f in fields.items() if f.instance_key]
        if ik_fields:
            raise TypeError(
                f"Entity '{cls.__entity_name__}' cannot use Field(instance_key=True). "
                f"instance_key is only valid on Relation fields."
            )

        # Validate exactly one primary key
        pk_fields = [n for n, f in fields.items() if f.primary_key]
        if len(pk_fields) == 0:
            raise TypeError(
                f"Entity '{cls.__entity_name__}' must define exactly one Field(primary_key=True)"
            )
        if len(pk_fields) > 1:
            raise TypeError(
                f"Entity '{cls.__entity_name__}' has multiple primary keys: {pk_fields}"
            )
        cls._primary_key_field = pk_fields[0]

        # Build pydantic model
        cls._pydantic_model = _build_pydantic_model(f"_{cls.__entity_name__}Model", fields)

    def __init__(self, **data: Any) -> None:
        # Validate through pydantic
        validated = self._pydantic_model(**data)
        for name in self.__entity_fields__:
            setattr(self, name, getattr(validated, name))

    def model_dump(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__entity_fields__}

    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> Any:
        return cls(**data)

    def meta(self) -> Meta:
        m = getattr(self, "__onto_meta__", None)
        if m is None:
            raise MetadataUnavailableError()
        return m

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={getattr(self, k)!r}" for k in self.__entity_fields__)
        return f"{self.__class__.__name__}({fields})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.model_dump() == other.model_dump()


class Relation(Generic[L, R]):
    """Base class for typed relationships with generic endpoints."""

    __relation_name__: ClassVar[str]
    __relation_fields__: ClassVar[tuple[str, ...]]
    _pydantic_model: ClassVar[type[BaseModel]]
    _field_definitions: ClassVar[dict[str, Field[Any]]]
    _left_type: ClassVar[type]
    _right_type: ClassVar[type]
    __onto_meta__: Meta | None = None

    left_key: str
    right_key: str

    def __init_subclass__(cls, name: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        cls.__relation_name__ = name or cls.__name__

        # Extract L, R from generic bases
        for base in getattr(cls, "__orig_bases__", []):
            origin = getattr(base, "__origin__", None)
            if origin is Relation:
                args = get_args(base)
                if len(args) == 2:
                    cls._left_type = args[0]
                    cls._right_type = args[1]
                break

        # Collect fields (attribute fields only, not left_key/right_key)
        fields = _collect_fields(cls, {})

        # Validate no primary_key on relations
        pk_fields = [n for n, f in fields.items() if f.primary_key]
        if pk_fields:
            raise TypeError(
                f"Relation '{cls.__relation_name__}' cannot use Field(primary_key=True). "
                f"primary_key is only valid on Entity fields."
            )

        # Detect instance_key field
        ik_fields = [n for n, f in fields.items() if f.instance_key]
        if len(ik_fields) > 1:
            raise TypeError(
                f"Relation '{cls.__relation_name__}' has multiple instance_key "
                f"fields: {ik_fields}. "
                f"At most one Field(instance_key=True) is allowed."
            )
        if ik_fields:
            ik_name = ik_fields[0]
            ik_field = fields[ik_name]
            # Must be str type
            if ik_field.annotation is not str:
                raise TypeError(
                    f"Relation '{cls.__relation_name__}' instance_key field '{ik_name}' "
                    f"must be of type str, got {ik_field.annotation}"
                )
            # Must not have a default
            if ik_field.has_default():
                raise TypeError(
                    f"Relation '{cls.__relation_name__}' instance_key field '{ik_name}' "
                    f"must not have a default value"
                )
            cls._instance_key_field: str | None = ik_name
        else:
            cls._instance_key_field = None

        # Exclude instance_key field from relation fields (it's part of identity, not data)
        data_fields = {n: f for n, f in fields.items() if not f.instance_key}
        cls._field_definitions = fields  # keep all for schema/validation
        cls.__relation_fields__ = tuple(data_fields.keys())

        # Build pydantic model for attribute validation (data fields only)
        cls._pydantic_model = _build_pydantic_model(f"_{cls.__relation_name__}Model", data_fields)

    def __init__(self, **data: Any) -> None:
        self.left_key = data.pop("left_key", "")
        self.right_key = data.pop("right_key", "")

        # Handle instance_key via declared field name
        ik_field = self._instance_key_field
        if ik_field is not None:
            ik_value = data.pop(ik_field, None)
            if ik_value is None:
                raise ValueError(
                    f"Relation '{self.__relation_name__}' requires '{ik_field}' "
                    f"(instance key field)"
                )
            if not isinstance(ik_value, str):
                raise TypeError(f"'{ik_field}' must be str, got {type(ik_value).__name__}")
            if not ik_value.strip():
                raise ValueError(f"'{ik_field}' must not be empty or whitespace-only")
            self.instance_key: str = ik_value
            setattr(self, ik_field, ik_value)
        else:
            self.instance_key = ""

        # Validate attribute fields through pydantic
        if data or self.__relation_fields__:
            validated = self._pydantic_model(**data)
            for name in self.__relation_fields__:
                setattr(self, name, getattr(validated, name))

        # Typed endpoint accessors (populated by query hydration)
        self.left: Any = None
        self.right: Any = None

    def model_dump(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__relation_fields__}

    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> Any:
        return cls(**data)

    def meta(self) -> Meta:
        m = getattr(self, "__onto_meta__", None)
        if m is None:
            raise MetadataUnavailableError()
        return m

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={getattr(self, k)!r}" for k in self.__relation_fields__)
        extras = f"left_key={self.left_key!r}, right_key={self.right_key!r}"
        if self._instance_key_field:
            extras += f", instance_key={self.instance_key!r}"
        if fields:
            extras += ", " + fields
        return f"{self.__class__.__name__}({extras})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return (
            self.left_key == other.left_key
            and self.right_key == other.right_key
            and self.instance_key == other.instance_key
            and self.model_dump() == other.model_dump()
        )
