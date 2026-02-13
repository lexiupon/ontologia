"""Typed event and scheduling primitives for the RFC 0005 runtime."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Self

from pydantic import BaseModel

from ontologia.types import Field, _build_pydantic_model, _collect_fields


class Event:
    """Base class for typed events."""

    __event_type__: ClassVar[str]
    __event_fields__: ClassVar[tuple[str, ...]]
    _pydantic_model: ClassVar[type[BaseModel]]
    _field_definitions: ClassVar[dict[str, Field[Any]]]
    __default_priority__: ClassVar[int]

    id: str | None
    created_at: str | None
    priority: int
    root_event_id: str | None
    chain_depth: int

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        explicit_type = cls.__dict__.get("__event_type_name__")
        if isinstance(explicit_type, str) and explicit_type:
            cls.__event_type__ = explicit_type
        else:
            cls.__event_type__ = cls._derive_event_type(cls.__name__)

        fields = _collect_fields(cls, {})
        cls._field_definitions = fields
        cls.__event_fields__ = tuple(fields.keys())
        cls._pydantic_model = _build_pydantic_model(f"_{cls.__name__}EventModel", fields)

        # Allow class-level priority override: `priority: int = 50`.
        raw_priority = cls.__dict__.get("priority", 100)
        cls.__default_priority__ = int(raw_priority) if isinstance(raw_priority, int) else 100

    def __init__(self, **data: Any) -> None:
        validated = self._pydantic_model(**data)
        for name in self.__event_fields__:
            setattr(self, name, getattr(validated, name))

        self.id = None
        self.created_at = None
        self.priority = self.__class__.__default_priority__
        self.root_event_id = None
        self.chain_depth = 0

    def model_dump(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__event_fields__}

    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> Self:
        return cls(**data)

    @staticmethod
    def _derive_event_type(class_name: str) -> str:
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1.\2", class_name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1.\2", s1).lower()


class EventDeadLetter(Event):
    """Built-in event emitted when a handler dead-letters an event."""

    __event_type_name__ = "event.dead_letter"

    event_id: Field[str]
    handler_id: Field[str]
    attempts: Field[int]
    last_error: Field[str]


@dataclass(frozen=True)
class Schedule:
    """Schedule definition that emits an event using cron syntax."""

    event: Event
    cron: str
