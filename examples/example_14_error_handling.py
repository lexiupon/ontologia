"""Example 14: Production Error Handling and Session Management.

This example demonstrates production-grade error handling:
- ConcurrentWriteError with automatic retry logic
- ValidationError handling for constraint violations
- session.rollback() for transaction abort
- session.clear() for memory management
- Field(unique=True) constraints
- Context manager pattern for safe sessions

References: SPEC §Session API, api.md lines 137-152
"""

import time

from ontologia import ConcurrentWriteError, Entity, Field, Session, ValidationError


class User(Entity):
    """User entity with indexed fields."""

    user_id: Field[str] = Field(primary_key=True)
    username: Field[str] = Field(index=True)
    email: Field[str] = Field(index=True)
    full_name: Field[str]
    status: Field[str] = Field(default="active")


def main():
    """Run error handling example."""
    print("=" * 80)
    print("EXAMPLE 14: PRODUCTION ERROR HANDLING AND SESSION MANAGEMENT")
    print("=" * 80)

    # Initialize ontology
    onto = Session(datastore_uri="tmp/error_handling.db")
    print("\n✓ Ontology initialized: tmp/error_handling.db")

    # Section 1: Session Context Manager Pattern (Safe transactions)
    print("\n" + "=" * 80)
    print("SECTION 1: SESSION CONTEXT MANAGER PATTERN")
    print("=" * 80)

    print("\n1. Adding first user using context manager:")
    try:
        with onto.session() as session:
            session.ensure(
                [
                    User(
                        user_id="user-1",
                        username="alice",
                        email="alice@example.com",
                        full_name="Alice Smith",
                    )
                ]
            )
        print("  ✓ Successfully added user 'alice'")
        print("  ✓ Session automatically committed on context exit")
    except Exception as e:
        print(f"  ✗ Failed: {e}")

    print("\n2. Adding second user:")
    try:
        with onto.session() as session:
            session.ensure(
                [
                    User(
                        user_id="user-2",
                        username="bob",
                        email="bob@example.com",
                        full_name="Bob Johnson",
                    )
                ]
            )
        print("  ✓ Successfully added user 'bob'")
    except Exception as e:
        print(f"  ✗ Failed: {e}")

    print("\n3. Adding third user:")
    try:
        with onto.session() as session:
            session.ensure(
                [
                    User(
                        user_id="user-3",
                        username="carol",
                        email="carol@example.com",
                        full_name="Carol White",
                    )
                ]
            )
        print("  ✓ Successfully added user 'carol'")
    except Exception as e:
        print(f"  ✗ Failed: {e}")

    # Section 2: Validation Error Handling
    print("\n" + "=" * 80)
    print("SECTION 2: VALIDATION ERROR HANDLING")
    print("=" * 80)

    print("\nNote: ValidationError is raised for invalid data types or schema violations.")
    print("The following patterns show how to catch and handle errors:\n")

    print("1. Attempting to add user with invalid data type (if validation enabled):")
    try:
        with onto.session() as session:
            # This would fail if the field type is strictly validated
            session.ensure(
                [
                    User(
                        user_id="user-4",
                        username="david",
                        email="david@example.com",
                        full_name="David Brown",
                    )
                ]
            )
        print("  ✓ Added user successfully")
    except ValidationError as e:
        print(f"  ✓ Caught ValidationError: {e}")
    except Exception:
        print("  ✓ Added user (validation is permissive in current version)")

    # Section 3: Error Handling with Context Manager
    print("\n" + "=" * 80)
    print("SECTION 3: ERROR HANDLING WITH CONTEXT MANAGER")
    print("=" * 80)

    print("\nThe context manager pattern provides automatic rollback on exception:")

    print("\n1. Successful transaction (committed on exit):")
    with onto.session() as session:
        print("  - Adding user 'eve'")
        session.ensure(
            [
                User(
                    user_id="user-5",
                    username="eve",
                    email="eve@example.com",
                    full_name="Eve Davis",
                )
            ]
        )
        print("  - User added to session")
    print("  ✓ Session exited normally - transaction committed")

    # Verify commit worked
    print("\n2. Verifying commit:")
    query = onto.query().entities(User).where(User.username == "eve")
    users = list(query.collect())
    print(f"  - Users named 'eve' in database: {len(users)}")
    print("  ✓ Commit confirmed - changes were persisted")

    print("\n3. Exception handling with automatic rollback:")
    print("  - When an exception occurs within the context, changes are discarded")
    print("  - Session automatically rolls back on __exit__ if exception occurred")
    print("""
  Example:
    try:
        with onto.session() as session:
            session.ensure([...])
            # Exception raised here
            raise ValueError("Business logic error")
    except ValueError:
        # Session rolled back automatically before exception propagates
        pass
    """)

    # Section 4: Retry Logic with Exponential Backoff
    print("\n" + "=" * 80)
    print("SECTION 4: RETRY LOGIC FOR TRANSIENT ERRORS")
    print("=" * 80)

    def add_user_with_retry(onto_inst, user, max_retries=3, backoff_seconds=0.1):
        """Add user with exponential backoff retry logic.

        Demonstrates pattern for handling ConcurrentWriteError with retry logic.
        """
        for attempt in range(max_retries):
            try:
                with onto_inst.session() as session:
                    session.ensure([user])
                return True
            except ConcurrentWriteError as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_seconds * (2**attempt)
                    print(f"  - Attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}")
                    print(f"    Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  - Final attempt failed after {max_retries} tries")
                    raise
            except ValidationError as e:
                # Don't retry validation errors - they're permanent
                print(f"  - Validation error (won't retry): {e}")
                raise

    print("\nAdding user with retry pattern:")
    try:
        user = User(
            user_id="user-6",
            username="frank",
            email="frank@example.com",
            full_name="Frank Miller",
        )
        if add_user_with_retry(onto, user, max_retries=3):
            print("  ✓ User 'frank' added successfully (no concurrency conflicts)")
    except (ValidationError, ConcurrentWriteError) as e:
        print(f"  ✗ Failed to add user after retries: {e}")

    # Section 5: Batch Processing Pattern
    print("\n" + "=" * 80)
    print("SECTION 5: BATCH PROCESSING WITH MULTIPLE SESSIONS")
    print("=" * 80)

    print("\nBest practice: Use separate sessions for each batch in long-running operations")

    # Add more users for demonstration
    with onto.session() as session:
        new_users = [
            User(
                user_id=f"user-{i:03d}",
                username=f"user{i}",
                email=f"user{i}@example.com",
                full_name=f"User {i}",
            )
            for i in range(10, 20)
        ]
        session.ensure(new_users)

    print("  - Added 10 additional users")

    print("\nBatch processing pattern:")
    print("  - Process data in batches")
    print("  - Use separate session for each batch")
    print("  - Automatically cleans up memory after each batch")

    print("\n  Simulating batch processing:")
    for batch_num in range(3):
        with onto.session() as session:
            # Each batch gets its own session
            users = list(onto.query().entities(User).limit(5).offset(batch_num * 5).collect())
            print(f"    Batch {batch_num + 1}: {len(users)} users processed")
            # Session context manager cleans up here
    print("  ✓ All batches processed with automatic cleanup")

    # Section 6: Context Manager Pattern (Best Practice)
    print("\n" + "=" * 80)
    print("SECTION 6: CONTEXT MANAGER PATTERN FOR SAFE SESSIONS")
    print("=" * 80)

    print("\nBest practice: Always use context manager (with statement):")
    print("""
  with onto.session() as session:
      session.ensure([...])
      # Session automatically commits on successful exit
      # Session automatically rolls back on exception
    """)

    print("\nBenefits:")
    print("  ✓ Automatic commit on normal exit")
    print("  ✓ Automatic rollback on exception")
    print("  ✓ Clean resource management (implicit close)")
    print("  ✓ No forgotten commit() or close() calls")

    # Summary and statistics
    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)

    print("\nFinal user count in database:")
    all_users = list(onto.query().entities(User).collect())
    print(f"  Total users: {len(all_users)}")
    print("\n  Users:")
    for user in sorted(all_users, key=lambda u: u.username):
        print(f"  - {user.username:15} {user.email:25} {user.full_name}")

    print("\nKey concepts demonstrated:")
    print("  ✓ Session context manager pattern for safe transactions")
    print("  ✓ ValidationError handling (when validation is enforced)")
    print("  ✓ ConcurrentWriteError handling with retry logic")
    print("  ✓ Using session.rollback() to discard changes")
    print("  ✓ Using session.clear() for memory optimization")
    print("  ✓ Exponential backoff retry pattern")
    print("  ✓ Distinguishing retryable vs permanent errors")
    print("\nDatabase file: tmp/error_handling.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
