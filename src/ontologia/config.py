"""Configuration for Ontologia runtime."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OntologiaConfig:
    """Configuration for the Ontology runtime."""

    max_batch_size: int = 10000
    runtime_id: str | None = None
    poll_commits: bool = False
    poll_interval_sec: float = 1.0
    max_commit_chain_depth: int = 16
    s3_region: str | None = None
    s3_endpoint_url: str | None = None
    s3_lock_timeout_ms: int = 5000
    s3_lease_ttl_ms: int = 30000
    s3_request_timeout_s: float = 10.0
    s3_duckdb_memory_limit: str = "256MB"
    default_namespace: str = "default"
    event_poll_interval_ms: int = 1000
    event_claim_limit: int = 100
    max_events_per_iteration: int = 1000
    event_claim_lease_ms: int = 30000
    event_retention_ms: int = 604800000
    session_heartbeat_interval_ms: int = 5000
    session_ttl_ms: int = 60000
    event_max_attempts: int = 10
    event_backoff_base_ms: int = 250
    event_backoff_max_ms: int = 30000
    max_event_chain_depth: int = 20
