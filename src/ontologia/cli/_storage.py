"""CLI helpers for backend-aware repository/runtime construction."""

from __future__ import annotations

import os
from typing import Any

from ontologia.config import OntologiaConfig
from ontologia.event_store import EventStore, create_event_store
from ontologia.runtime import Ontology
from ontologia.session import Session
from ontologia.storage import RepositoryProtocol, open_repository


def resolve_storage_binding() -> tuple[str | None, str | None]:
    """Return (db_path, storage_uri) from CLI state."""
    from ontologia.cli import state

    if state.storage_uri:
        return None, state.storage_uri
    return state.db, None


def _config_from_env() -> OntologiaConfig:
    """Build runtime config from CLI environment defaults."""
    endpoint = os.getenv("ONTOLOGIA_S3_ENDPOINT_URL") or os.getenv("ONTOLOGIA_S3_ENDPOINT")
    region = os.getenv("ONTOLOGIA_S3_REGION")
    return OntologiaConfig(
        s3_region=region,
        s3_endpoint_url=endpoint,
    )


def open_repo() -> RepositoryProtocol:
    """Open repository using global CLI storage selection."""
    db_path, storage_uri = resolve_storage_binding()
    return open_repository(db_path, storage_uri=storage_uri, config=_config_from_env())


def open_ontology(
    *,
    entity_types: list[type[Any]] | None = None,
    relation_types: list[type[Any]] | None = None,
) -> Ontology:
    """Open ontology runtime using global CLI storage selection."""
    db_path, storage_uri = resolve_storage_binding()
    return Ontology(
        db_path,
        config=_config_from_env(),
        storage_uri=storage_uri,
        entity_types=entity_types,
        relation_types=relation_types,
    )


def open_session(
    *,
    namespace: str | None = None,
) -> Session:
    """Open RFC 0005 session using global CLI storage selection."""
    db_path, storage_uri = resolve_storage_binding()
    config = _config_from_env()
    datastore_uri = storage_uri or f"sqlite:///{db_path}"
    return Session(
        datastore_uri=datastore_uri,
        namespace=namespace,
        config=config,
    )


def open_event_store() -> tuple[RepositoryProtocol, EventStore]:
    """Open repository + event store for backend-agnostic event CLI commands."""
    db_path, storage_uri = resolve_storage_binding()
    config = _config_from_env()
    repo = open_repository(db_path, storage_uri=storage_uri, config=config)
    datastore_uri = storage_uri or f"sqlite:///{db_path}"
    store = create_event_store(
        datastore_uri=datastore_uri,
        repo=repo,
        config=config,
    )
    return repo, store
