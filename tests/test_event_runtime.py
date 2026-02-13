"""Integration tests for RFC 0005 event runtime."""

from __future__ import annotations

from ontologia import Event, Field, HandlerContext, OntologiaConfig, Session, on_event
from tests.conftest import Customer


class UserCreated(Event):
    user_id: Field[str]


class FollowUp(Event):
    user_id: Field[str]


def test_handler_without_commit_produces_no_state(tmp_db: str) -> None:
    config = OntologiaConfig(event_poll_interval_ms=10)
    seen: list[str] = []

    @on_event(UserCreated)
    def on_user(ctx: HandlerContext[UserCreated]) -> None:
        seen.append(ctx.event.user_id)
        ctx.ensure(Customer(id=ctx.event.user_id, name="Alice", age=30))
        # Explicit commit is intentionally omitted.

    with Session(
        datastore_uri=f"sqlite:///{tmp_db}",
        config=config,
        entity_types=[Customer],
    ) as session:
        session.commit(event=UserCreated(user_id="c1"))
        session.run([on_user], max_iterations=8)

        customers = session.query().entities(Customer).collect()
        assert seen == ["c1"]
        assert customers == []


def test_handler_commit_persists_state(tmp_db: str) -> None:
    config = OntologiaConfig(event_poll_interval_ms=10)

    @on_event(UserCreated)
    def on_user(ctx: HandlerContext[UserCreated]) -> None:
        ctx.ensure(Customer(id=ctx.event.user_id, name="Bob", age=29))
        ctx.commit()

    with Session(
        datastore_uri=f"sqlite:///{tmp_db}",
        config=config,
        entity_types=[Customer],
    ) as session:
        session.commit(event=UserCreated(user_id="c2"))
        session.run([on_user], max_iterations=8)

        customers = session.query().entities(Customer).collect()
        assert len(customers) == 1
        assert customers[0].id == "c2"


def test_emit_buffering_and_chaining(tmp_db: str) -> None:
    config = OntologiaConfig(event_poll_interval_ms=10)
    processed: list[str] = []

    @on_event(UserCreated)
    def on_created(ctx: HandlerContext[UserCreated]) -> None:
        ctx.ensure(Customer(id=ctx.event.user_id, name="Cat", age=31))
        ctx.emit(FollowUp(user_id=ctx.event.user_id))
        ctx.commit()

    @on_event(FollowUp)
    def on_follow_up(ctx: HandlerContext[FollowUp]) -> None:
        processed.append(ctx.event.user_id)

    with Session(
        datastore_uri=f"sqlite:///{tmp_db}",
        config=config,
        entity_types=[Customer],
    ) as session:
        session.commit(event=UserCreated(user_id="c3"))
        session.run([on_created, on_follow_up], max_iterations=12)

        customers = session.query().entities(Customer).collect()
        assert len(customers) == 1
        assert customers[0].id == "c3"
        assert processed == ["c3"]
