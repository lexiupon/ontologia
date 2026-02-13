"""S3 backend integration tests (MinIO-compatible)."""

from __future__ import annotations

import json
import os
import uuid

import boto3
import pytest
from typer.testing import CliRunner

from ontologia import Session
from ontologia.cli import app
from ontologia.config import OntologiaConfig
from ontologia.storage import open_repository
from ontologia.storage_s3 import S3Repository
from tests.conftest import Customer, Product, Subscription

pytestmark = pytest.mark.s3


@pytest.fixture
def s3_backend() -> dict[str, str]:
    if os.getenv("ONTOLOGIA_S3_TEST") != "1":
        pytest.skip("S3 integration tests disabled (set ONTOLOGIA_S3_TEST=1)")

    endpoint = os.getenv("ONTOLOGIA_S3_ENDPOINT", "http://127.0.0.1:9000")
    bucket = os.getenv("ONTOLOGIA_S3_BUCKET", "ontologia-test")
    region = os.getenv("ONTOLOGIA_S3_REGION", "us-east-1")

    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    prefix = f"it/{uuid.uuid4().hex}"
    storage_uri = f"s3://{bucket}/{prefix}"

    repo = S3Repository(
        bucket=bucket,
        prefix=prefix,
        storage_uri=storage_uri,
        config=OntologiaConfig(s3_region=region, s3_endpoint_url=endpoint),
        allow_uninitialized=True,
    )
    try:
        repo.initialize_storage(dry_run=False)
    finally:
        repo.close()

    return {
        "storage_uri": storage_uri,
        "endpoint": endpoint,
        "region": region,
        "bucket": bucket,
        "prefix": prefix,
    }


def _cfg(backend: dict[str, str], runtime_id: str) -> OntologiaConfig:
    return OntologiaConfig(
        runtime_id=runtime_id,
        s3_region=backend["region"],
        s3_endpoint_url=backend["endpoint"],
        s3_lock_timeout_ms=5000,
        s3_lease_ttl_ms=30000,
        s3_duckdb_memory_limit="256MB",
    )


def test_s3_query_uses_httpfs_not_local_download(monkeypatch, s3_backend):
    onto = Session(
        datastore_uri=s3_backend["storage_uri"],
        config=_cfg(s3_backend, "it-httpfs"),
        entity_types=[Customer, Product],
        relation_types=[Subscription],
    )

    with onto.session() as s:
        s.ensure(Customer(id="c1", name="Alice", age=30, tier="Gold"))
        s.ensure(Product(sku="p1", name="Widget", price=9.99))
        s.ensure(
            Subscription(
                left_key="c1",
                right_key="p1",
                seat_count=2,
                started_at="2025-01-01",
            )
        )
        s.commit()

    repo = onto.repo

    def _fail_download(_path: str) -> str:
        raise AssertionError("query path should use DuckDB httpfs, not local downloads")

    monkeypatch.setattr(repo, "_download", _fail_download)
    rows = onto.query().entities(Customer).where(Customer.tier == "Gold").collect()
    assert len(rows) == 1
    assert rows[0].name == "Alice"
    onto.close()


def test_schema_drop_on_s3_backend(s3_backend):
    onto = Session(
        datastore_uri=s3_backend["storage_uri"],
        config=_cfg(s3_backend, "it-schema-drop-seed"),
        entity_types=[Customer, Product],
        relation_types=[Subscription],
    )
    with onto.session() as s:
        s.ensure(Customer(id="c1", name="Alice", age=30, tier="Gold"))
        s.ensure(Product(sku="p1", name="Widget", price=9.99))
        s.ensure(
            Subscription(
                left_key="c1",
                right_key="p1",
                seat_count=5,
                started_at="2025-01-01",
            )
        )
        s.commit()
    onto.close()

    runner = CliRunner()
    dry = runner.invoke(
        app,
        [
            "--storage-uri",
            s3_backend["storage_uri"],
            "--json",
            "schema",
            "drop",
            "relation",
            "Subscription",
            "--purge-history",
        ],
        catch_exceptions=False,
    )
    assert dry.exit_code == 0
    dry_payload = json.loads(dry.output)
    token = dry_payload["token"]

    apply = runner.invoke(
        app,
        [
            "--storage-uri",
            s3_backend["storage_uri"],
            "--json",
            "schema",
            "drop",
            "relation",
            "Subscription",
            "--purge-history",
            "--apply",
            "--token",
            token,
        ],
        catch_exceptions=False,
    )
    assert apply.exit_code == 0

    repo = open_repository(
        storage_uri=s3_backend["storage_uri"],
        config=_cfg(s3_backend, "it-schema-drop-check"),
    )
    try:
        assert repo.count_latest_relations("Subscription") == 0
        assert repo.get_current_schema_version("relation", "Subscription") is None
    finally:
        repo.close()
