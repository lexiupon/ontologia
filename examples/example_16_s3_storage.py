"""Example 16: S3 Storage Backend and Distributed Writes.

This example demonstrates S3 storage for cloud-native deployments:
- S3StorageConfig initialization with bucket and prefix
- endpoint_url parameter for MinIO/LocalStack testing
- lease_duration_seconds for lock tuning
- Multi-writer scenarios with lock contention
- Distributed lock mechanics
- Production deployment patterns

IMPORTANT: This example documents S3 features and includes patterns for
MinIO/LocalStack testing. Actual S3 execution requires:
  1. MinIO server running: docker run -p 9000:9000 minio/minio server /data
  2. AWS credentials configured (environment or AWS profile)
  3. Active S3-compatible endpoint

References: SPEC §Storage S3, storage-s3.md lines 87-314
"""

import threading
import time

from ontologia import Entity, Field, Session


class Task(Entity):
    """Distributed task entity."""

    task_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    status: Field[str] = Field(default="pending")  # pending, running, completed
    worker_id: Field[str | None] = Field(default=None)
    attempts: Field[int] = Field(default=0)
    created_at: Field[str] = Field(default="2024-01-01T00:00:00Z")


def main():
    """Run S3 storage example."""
    print("=" * 80)
    print("EXAMPLE 16: S3 STORAGE BACKEND AND DISTRIBUTED WRITES")
    print("=" * 80)

    # Section 1: S3 Storage Configuration (Documentation)
    print("\n" + "=" * 80)
    print("SECTION 1: S3 STORAGE CONFIGURATION")
    print("=" * 80)

    print("\nS3StorageConfig Parameters:")
    print("=" * 40)

    print("\n1. Basic S3 Configuration:")
    print("""
  from ontologia.storage import S3StorageConfig

  config = S3StorageConfig(
      bucket="my-ontologia-bucket",
      prefix="production/",
  )
  onto = Session(storage=config)
    """)

    print("\n2. AWS Region Selection:")
    print("""
  config = S3StorageConfig(
      bucket="my-ontologia-bucket",
      prefix="us-east-1/",
      region="us-east-1",  # AWS region
  )
  # Other options: us-west-2, eu-west-1, ap-southeast-1, etc.
    """)

    print("\n3. MinIO/LocalStack for Development:")
    print("""
  config = S3StorageConfig(
      bucket="ontologia-dev",
      prefix="test/",
      endpoint_url="http://localhost:9000",  # MinIO endpoint
      aws_access_key_id="minioadmin",
      aws_secret_access_key="minioadmin",
  )
  # MinIO setup: docker run -p 9000:9000 minio/minio server /data
    """)

    print("\n4. Named AWS Profiles:")
    print("""
  config = S3StorageConfig(
      bucket="my-ontologia-bucket",
      prefix="staging/",
      profile="staging",  # From ~/.aws/credentials
  )
  # Allows separate credentials for dev/staging/production
    """)

    # Section 2: S3 Object Structure
    print("\n" + "=" * 80)
    print("SECTION 2: S3 OBJECT STRUCTURE")
    print("=" * 80)

    print("""
When initialized, S3StorageConfig creates this structure:

s3://bucket/prefix/
├── commits/
│   ├── 0000000001.json      # Commit metadata
│   ├── 0000000002.json
│   └── ...
├── entities/
│   ├── Task/
│   │   ├── task-001.json    # Entity state
│   │   ├── task-002.json
│   │   └── ...
│   └── [OtherEntity]/
├── relations/
│   ├── RelationType/
│   │   ├── left_id:right_id.json
│   │   └── ...
└── metadata/
    ├── schema.json          # Schema definition
    └── locks/
        └── writer.lock      # Distributed write lock
    """)

    # Section 3: Basic Operations (Using SQLite as proxy)
    print("\n" + "=" * 80)
    print("SECTION 3: BASIC CRUD OPERATIONS")
    print("=" * 80)

    print("""
S3 operations are identical to SQLite from the API perspective:

  config = S3StorageConfig(bucket="my-bucket", prefix="ontologia/")
  onto = Session(storage=config)

  # Create
  with onto.session() as session:
      session.ensure([Task(task_id="t1", name="Task 1")])

  # Read
  tasks = list(onto.query().entities(Task).collect())

  # Update
  with onto.session() as session:
      session.ensure([Task(task_id="t1", name="Task 1", status="running")])

  # Delete (via state transition)
  with onto.session() as session:
      session.ensure([Task(task_id="t1", name="Task 1", status="completed")])
    """)

    # Demonstrate with SQLite (since S3 requires external service)
    print("\n✓ For demonstration, using SQLite backend")
    print("  (Same API applies to S3 storage)")

    onto = Session(datastore_uri="tmp/s3_storage.db")
    print("\n✓ Ontology initialized: tmp/s3_storage.db")

    # Load initial tasks
    print("\nLoading tasks...")
    with onto.session() as session:
        session.ensure(
            [Task(task_id=f"task-{i:03d}", name=f"Process item {i}") for i in range(1, 6)]
        )
    print("✓ Created 5 tasks")

    # Section 4: Distributed Lock Mechanics (Simulated)
    print("\n" + "=" * 80)
    print("SECTION 4: DISTRIBUTED LOCK MECHANICS")
    print("=" * 80)

    print("\nIn S3 backend, write consistency is maintained via:")
    print("""
  1. Distributed Lock File (s3://bucket/prefix/metadata/locks/writer.lock)
     - Single lock across all processes
     - Acquired before writing any changes
     - Renewed during long transactions

  2. Lock Lease Mechanism
     - Lease duration: configurable (default 60 seconds)
     - Lock auto-expires if process crashes
     - Prevents deadlocks in distributed systems

  3. Lock Contention Handling
     - Process polls for lock availability
     - ConcurrentWriteError raised if lock held
     - Automatic retry with backoff recommended
    """)

    print("Lock Configuration:")
    print("""
  config = S3StorageConfig(
      bucket="my-bucket",
      lease_duration_seconds=60,      # How long lock is held
      lock_poll_interval_seconds=1,   # How often to check lock
  )
    """)

    # Section 5: Multi-Writer Scenario (Simulated with threads)
    print("\n" + "=" * 80)
    print("SECTION 5: MULTI-WRITER SCENARIO (SIMULATED)")
    print("=" * 80)

    print("\nSimulating multiple workers updating tasks:")

    def worker_process(worker_id, onto_inst, task_ids):
        """Simulate a worker processing tasks."""
        print(f"\n  Worker {worker_id} starting...")
        for task_id in task_ids:
            try:
                with onto_inst.session() as session:
                    # Read task
                    tasks = list(
                        onto_inst.query().entities(Task).where(Task.task_id == task_id).collect()
                    )

                    if tasks:
                        task = tasks[0]
                        # Update status
                        updated_task = Task(
                            task_id=task.task_id,
                            name=task.name,
                            status="completed",
                            worker_id=worker_id,
                            attempts=task.attempts + 1,
                        )
                        session.ensure([updated_task])
                        print(f"  Worker {worker_id} completed {task_id}")
            except Exception as e:
                print(f"  Worker {worker_id} error processing {task_id}: {e}")
            time.sleep(0.1)  # Simulate processing

    # Create worker threads
    workers = []
    task_assignments = [
        ("worker-1", ["task-001", "task-003"]),
        ("worker-2", ["task-002", "task-004"]),
    ]

    for worker_id, task_ids in task_assignments:
        thread = threading.Thread(target=worker_process, args=(worker_id, onto, task_ids))
        workers.append(thread)
        thread.start()

    # Wait for workers to finish
    for thread in workers:
        thread.join()

    print("\n✓ All workers completed")

    # Verify results
    print("\nFinal task states:")
    tasks = sorted(onto.query().entities(Task).collect(), key=lambda t: t.task_id)
    for task in tasks:
        status = f"{task.status:10} (worker: {task.worker_id or 'unassigned'})"
        print(f"  {task.task_id}: {status}")

    # Section 6: Lease Duration Effects
    print("\n" + "=" * 80)
    print("SECTION 6: LEASE DURATION TUNING")
    print("=" * 80)

    print("""
Lease Duration Configuration:

  Short Lease (15 seconds):
    config = S3StorageConfig(..., lease_duration_seconds=15)
    - Pros: Fast failover on worker crash
    - Cons: Higher lock renewal overhead
    - Use: Low-latency, many workers

  Default Lease (60 seconds):
    config = S3StorageConfig(..., lease_duration_seconds=60)
    - Pros: Balanced, reasonable timeout
    - Cons: Slower failover
    - Use: Most production systems

  Long Lease (300 seconds):
    config = S3StorageConfig(..., lease_duration_seconds=300)
    - Pros: Fewer lock renewals, lower S3 API calls
    - Cons: Slow failover on process crash
    - Use: Batch processing, long transactions
    """)

    # Section 7: Lock Contention Demonstration
    print("\n" + "=" * 80)
    print("SECTION 7: LOCK CONTENTION AND RETRY LOGIC")
    print("=" * 80)

    print("""
When multiple writers contend for the lock:

  1. First writer acquires lock
  2. Second writer gets ConcurrentWriteError
  3. Second writer retries with backoff

  Example retry logic:

  from ontologia import ConcurrentWriteError
  import time

  max_retries = 3
  backoff_seconds = 0.1

  for attempt in range(max_retries):
      try:
          with onto.session() as session:
              session.ensure([...])
          break  # Success
      except ConcurrentWriteError:
          if attempt < max_retries - 1:
              wait = backoff_seconds * (2 ** attempt)
              time.sleep(wait)
          else:
              raise
    """)

    # Section 8: Production Deployment Patterns
    print("\n" + "=" * 80)
    print("SECTION 8: PRODUCTION DEPLOYMENT PATTERNS")
    print("=" * 80)

    print("\nPattern 1: Environment-specific Prefixes")
    print("""
  import os

  env = os.getenv("ENVIRONMENT", "dev")  # dev, staging, prod

  config = S3StorageConfig(
      bucket="my-ontologia-bucket",
      prefix=f"{env}/ontologia/",  # Separates environments
      region=os.getenv("AWS_REGION", "us-east-1"),
      profile=os.getenv("AWS_PROFILE", "default"),
  )
    """)

    print("\nPattern 2: Multi-Region Failover")
    print("""
  config = S3StorageConfig(
      bucket=f"ontologia-{region}",
      prefix="data/",
      region=region,  # us-east-1, eu-west-1, etc.
  )
  # Switch bucket per region for data residency
    """)

    print("\nPattern 3: Distributed Task Processing")
    print("""
  config = S3StorageConfig(
      bucket="task-queue",
      prefix="tasks/",
      lease_duration_seconds=30,      # Short lease for fast failover
      lock_poll_interval_seconds=0.5, # Rapid lock checking
  )

  def worker():
      onto = Session(storage=config)
      while True:
          try:
              with onto.session() as session:
                  # Get pending task
                  task = session.query(Task).where(
                      Task.status == "pending"
                  ).first()

                  if task:
                      # Process and update
                      session.ensure([
                          Task(..., status="completed", worker_id=my_id)
                      ])
          except ConcurrentWriteError:
              time.sleep(0.1)
    """)

    print("\nPattern 4: Serverless Lambda Functions")
    print("""
  # Each Lambda invocation is independent, S3 lock prevents conflicts

  def lambda_handler(event, context):
      config = S3StorageConfig(
          bucket="ontologia-serverless",
          prefix="data/",
          lease_duration_seconds=30,  # Fast cleanup on timeout
      )
      onto = Session(storage=config)

      with onto.session() as session:
          # Read/write operations
          session.ensure([...])

      return {"statusCode": 200}
    """)

    # Section 9: Comparison Table
    print("\n" + "=" * 80)
    print("SECTION 9: SQLITE VS S3 STORAGE COMPARISON")
    print("=" * 80)

    print("\n┌─────────────────┬──────────────────────┬─────────────────────┐")
    print("│ Feature         │ SQLite               │ S3                  │")
    print("├─────────────────┼──────────────────────┼─────────────────────┤")
    print("│ Setup           │ Instant (file based) │ Requires S3/MinIO   │")
    print("│ Scalability     │ Single machine       │ Unlimited (cloud)   │")
    print("│ Multi-writer    │ Limited (file locks) │ Full (distributed)  │")
    print("│ Consistency     │ Strong (ACID)        │ Strong (lock-based) │")
    print("│ Performance     │ Local SSD fast       │ Network latency     │")
    print("│ Cost            │ Free (file storage)  │ Pay-per-API-call    │")
    print("│ Development     │ Perfect              │ MinIO emulation     │")
    print("│ Production      │ Embedded systems     │ Serverless/K8s      │")
    print("└─────────────────┴──────────────────────┴─────────────────────┘")

    # Summary
    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)

    print("\nKey concepts demonstrated:")
    print("  ✓ S3StorageConfig initialization")
    print("  ✓ Bucket and prefix configuration")
    print("  ✓ endpoint_url for MinIO/LocalStack")
    print("  ✓ Distributed lock mechanics")
    print("  ✓ Multi-writer scenarios")
    print("  ✓ Lease duration tuning")
    print("  ✓ Production deployment patterns")
    print("  ✓ SQLite vs S3 comparison")

    print("\nNext steps:")
    print("  1. Set up MinIO: docker run -p 9000:9000 minio/minio server /data")
    print("  2. Create S3StorageConfig with endpoint_url")
    print("  3. Test with multiple workers in parallel")
    print("  4. Monitor lock contention and adjust lease_duration_seconds")
    print("  5. Deploy to cloud: AWS Lambda, ECS, K8s, etc.")

    print("\nDatabase file: tmp/s3_storage.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
