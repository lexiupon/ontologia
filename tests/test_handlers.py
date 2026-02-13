"""Tests for RFC 0005 event handler decorators and context."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

from ontologia import Entity, Event, Field
from ontologia.event_handlers import HandlerContext, on_event


class UserCreated(Event):
    user_id: Field[str]


class FollowUp(Event):
    user_id: Field[str]


class Marker(Entity):
    id: Field[str] = Field(primary_key=True)


class _DummySession:
    def __init__(self) -> None:
        self.ensured: list[Any] = []
        self.commit_calls: list[dict[str, Any]] = []

    def ensure(self, obj: Any) -> None:
        self.ensured.append(obj)

    def _commit_from_handler(
        self,
        ctx: HandlerContext[Any],
        *,
        event: Event | None,
        commit_meta: dict[str, str],
    ) -> int:
        self.commit_calls.append(
            {
                "event": event,
                "meta": dict(commit_meta),
                "source_event_type": ctx.event.__class__.__event_type__,
            }
        )
        return 42


class TestDecorators:
    def test_on_event_default(self):
        @on_event(UserCreated)
        def my_handler(ctx: HandlerContext[UserCreated]) -> None:
            del ctx

        meta = cast(Any, my_handler)._ontologia_event_handler
        assert meta.event_cls is UserCreated
        assert meta.priority == 100
        assert meta.handler_id.endswith(".my_handler")

    def test_on_event_priority(self):
        @on_event(UserCreated, priority=50)
        def my_handler(ctx: HandlerContext[UserCreated]) -> None:
            del ctx

        meta = cast(Any, my_handler)._ontologia_event_handler
        assert meta.priority == 50

    def test_handler_id_uses_qualified_name(self):
        @on_event(UserCreated)
        def nested(ctx: HandlerContext[UserCreated]) -> None:
            del ctx

        meta = cast(Any, nested)._ontologia_event_handler
        assert nested.__module__ in meta.handler_id
        assert nested.__qualname__ in meta.handler_id


class TestHandlerContext:
    def test_basic_context(self):
        session = _DummySession()
        evt = UserCreated(user_id="u1")
        lease_until = datetime.now(timezone.utc)

        ctx = HandlerContext(event=evt, session=cast(Any, session), lease_until=lease_until)
        assert ctx.event.user_id == "u1"
        assert ctx.session is session
        assert ctx.lease_until == lease_until

    def test_ensure_delegates_to_session(self):
        session = _DummySession()
        ctx = HandlerContext(event=UserCreated(user_id="u1"), session=cast(Any, session))

        payload = Marker(id="m1")
        ctx.ensure(payload)

        assert session.ensured == [payload]

    def test_emit_buffers_events(self):
        session = _DummySession()
        ctx = HandlerContext(event=UserCreated(user_id="u1"), session=cast(Any, session))

        ctx.emit(FollowUp(user_id="u1"))
        assert len(ctx._buffered_events) == 1
        assert isinstance(ctx._buffered_events[0], FollowUp)
        assert ctx._buffered_events[0].user_id == "u1"

    def test_add_commit_meta_last_write_wins(self):
        session = _DummySession()
        ctx = HandlerContext(event=UserCreated(user_id="u1"), session=cast(Any, session))

        ctx.add_commit_meta("source", "api")
        ctx.add_commit_meta("source", "worker")
        ctx.add_commit_meta("job", "job-1")

        assert ctx._commit_meta == {"source": "worker", "job": "job-1"}

    def test_commit_delegates_and_clears_meta(self):
        session = _DummySession()
        ctx = HandlerContext(event=UserCreated(user_id="u1"), session=cast(Any, session))
        ctx.add_commit_meta("source", "import")

        commit_id = ctx.commit(event=FollowUp(user_id="u1"))

        assert commit_id == 42
        assert len(session.commit_calls) == 1
        assert session.commit_calls[0]["meta"] == {"source": "import"}
        assert isinstance(session.commit_calls[0]["event"], FollowUp)
        assert ctx._commit_meta == {}
