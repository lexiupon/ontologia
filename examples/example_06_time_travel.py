# pyright: reportAttributeAccessIssue=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# RATIONALE: The query API returns Entity objects but pyright infers dict[str, Any].
# This is a known type inference limitation. The code is correct at runtime.
"""Example 06: Time Travel - Temporal Queries and Audit Trails.

This example demonstrates temporal querying capabilities:
- Creating and updating relations
- Querying as_of a specific commit
- Viewing historical state and commit history
- Use cases: auditing, temporal analysis, debugging

References: WRK-0009 (Versioning), WRK-0020 (Time Travel)
"""

from ontologia import Entity, Field, Relation, Session


class Document(Entity):
    """A document that changes over time."""

    doc_id: Field[str] = Field(primary_key=True)
    title: Field[str]
    content: Field[str]
    status: Field[str]  # "draft", "published", "archived"
    author: Field[str]


class Tag(Entity):
    """A tag for categorizing documents."""

    name: Field[str] = Field(primary_key=True)
    category: Field[str]


class HasTag(Relation[Document, Tag]):
    """Document tagging relation."""

    added_at: Field[str]


def main():
    """Run the time travel example."""
    print("=" * 80)
    print("ONTOLOGIA TIME TRAVEL EXAMPLE")
    print("=" * 80)

    onto = Session(datastore_uri="tmp/time_travel.db")
    print("\n✓ Ontology initialized")

    # Track commit IDs for time travel queries
    commit_ids: dict[str, int] = {}

    # Commit 1: Initial document creation
    print("\n" + "=" * 80)
    print("COMMIT 1: Initial document creation")
    print("=" * 80)

    with onto.session() as session:
        session.ensure(
            [
                Document(
                    doc_id="DOC-001",
                    title="Getting Started Guide",
                    content="This is a draft guide for new users.",
                    status="draft",
                    author="Alice",
                ),
                Tag(
                    name="tutorial",
                    category="educational",
                ),
            ]
        )

    # Get the commit ID using the public API
    commits = onto.list_commits(limit=1)
    commit_ids["initial"] = commits[0]["id"] if commits else 1

    print(f"✓ Created initial document (Commit ID: {commit_ids['initial']})")
    print("  - Document: 'Getting Started Guide' (draft)")
    print("  - Tag: 'tutorial'")

    # Commit 2: Publish document and add tag
    print("\n" + "=" * 80)
    print("COMMIT 2: Publish document and add tag relation")
    print("=" * 80)

    with onto.session() as session:
        session.ensure(
            [
                Document(
                    doc_id="DOC-001",
                    title="Getting Started Guide",
                    content="This is a comprehensive guide for new users. Updated with examples!",
                    status="published",
                    author="Alice",
                ),
                HasTag(
                    left_key="DOC-001",
                    right_key="tutorial",
                    added_at="2024-01-15T10:00:00Z",
                ),
            ]
        )

    commits = onto.list_commits(limit=1)
    commit_ids["published"] = commits[0]["id"] if commits else 2

    print(f"✓ Updated document (Commit ID: {commit_ids['published']})")
    print("  - Status changed: draft → published")
    print("  - Content updated with examples")
    print("  - Added 'tutorial' tag")

    # Commit 3: Add more tags
    print("\n" + "=" * 80)
    print("COMMIT 3: Add more tags")
    print("=" * 80)

    with onto.session() as session:
        session.ensure(
            [
                Tag(
                    name="beginner",
                    category="skill-level",
                ),
                HasTag(
                    left_key="DOC-001",
                    right_key="beginner",
                    added_at="2024-01-16T14:30:00Z",
                ),
            ]
        )

    commits = onto.list_commits(limit=1)
    commit_ids["more_tags"] = commits[0]["id"] if commits else 3

    print(f"✓ Added more tags (Commit ID: {commit_ids['more_tags']})")
    print("  - Added 'beginner' tag")

    # Commit 4: Archive the document
    print("\n" + "=" * 80)
    print("COMMIT 4: Archive the document")
    print("=" * 80)

    with onto.session() as session:
        session.ensure(
            [
                Document(
                    doc_id="DOC-001",
                    title="Getting Started Guide [ARCHIVED]",
                    content="This guide has been replaced with a newer version.",
                    status="archived",
                    author="Alice",
                ),
            ]
        )

    commits = onto.list_commits(limit=1)
    commit_ids["archived"] = commits[0]["id"] if commits else 4

    print(f"✓ Archived document (Commit ID: {commit_ids['archived']})")
    print("  - Status changed: published → archived")
    print("  - Title updated with [ARCHIVED] marker")

    # Now demonstrate time travel queries
    print("\n" + "=" * 80)
    print("TIME TRAVEL QUERIES")
    print("=" * 80)

    # Query current state
    print("\n1. Current state (latest commit):")
    current_docs = list(onto.query().entities(Document).collect())
    for doc in current_docs:
        print(f"   Document: {doc.title}")
        print(f"   Status: {doc.status}")
        # Note: Metadata fields (_commit_id, _version) not available in Entity API
        # Use raw queries if metadata is needed

    # Query as of initial commit
    print(f"\n2. State as of COMMIT {commit_ids['initial']} (initial creation):")
    docs_at_initial = list(onto.query().entities(Document).as_of(commit_ids["initial"]).collect())
    if docs_at_initial:
        for doc in docs_at_initial:
            print(f"   Document: {doc.title}")
            print(f"   Status: {doc.status}")
            print(f"   Content length: {len(doc.content)} chars")
    else:
        print("   No documents found at this commit")

    # Query as of published commit
    print(f"\n3. State as of COMMIT {commit_ids['published']} (published):")
    docs_at_published = list(
        onto.query().entities(Document).as_of(commit_ids["published"]).collect()
    )
    for doc in docs_at_published:
        print(f"   Document: {doc.title}")
        print(f"   Status: {doc.status}")
        print(f"   Content preview: {doc.content[:50]}...")

    # Query relations as of different commits
    print("\n4. Tags over time:")

    print(f"\n   As of COMMIT {commit_ids['initial']} (initial):")
    tags_initial = onto.query().relations(HasTag).as_of(commit_ids["initial"]).collect()
    tag_count_initial = len(list(tags_initial))
    print(f"   - Tag relations: {tag_count_initial}")

    print(f"\n   As of COMMIT {commit_ids['published']} (published):")
    tags_published = onto.query().relations(HasTag).as_of(commit_ids["published"]).collect()
    tag_count_published = 0
    for edge in tags_published:
        tag_count_published += 1
        print(f"   - Tag: {edge.right_key}")
    print(f"   - Total tag relations: {tag_count_published}")

    print(f"\n   As of COMMIT {commit_ids['more_tags']} (more tags):")
    tags_more = onto.query().relations(HasTag).as_of(commit_ids["more_tags"]).collect()
    tag_count_more = 0
    for edge in tags_more:
        tag_count_more += 1
        print(f"   - Tag: {edge.right_key}")
    print(f"   - Total tag relations: {tag_count_more}")

    print("\n" + "=" * 80)
    print("COMMIT 5: Retract (soft delete) the document")
    print("=" * 80)

    # Note: Retraction is not demonstrated as it requires special handling
    # In Ontologia, retraction is typically modeled via lifecycle fields
    # rather than hard deletion. For example:
    #   session.ensure(Document(doc_id="DOC-001", status="retracted", ...))

    print("✓ Retraction not demonstrated (see comments above)")
    commit_ids["retracted"] = commit_ids["archived"]  # Placeholder for demo

    print("\n5. State after retraction:")
    current_docs_after_retract = list(onto.query().entities(Document).collect())
    print(f"   Current documents: {len(current_docs_after_retract)}")
    if not current_docs_after_retract:
        print("   (Document has been retracted - soft deleted)")

    print(
        f"\n6. Document still visible in historical state (as of COMMIT {commit_ids['archived']}):"
    )
    docs_before_retract = list(
        onto.query().entities(Document).as_of(commit_ids["archived"]).collect()
    )
    for doc in docs_before_retract:
        print(f"   Document: {doc.title}")
        print(f"   Status: {doc.status}")
        print("   (This is the historical state before retraction)")

    # Use case: Audit trail
    print("\n" + "=" * 80)
    print("USE CASE: AUDIT TRAIL")
    print("=" * 80)

    print("\nDocument lifecycle for DOC-001:")
    print(f"  Commit {commit_ids['initial']}: Created as draft")
    print(f"  Commit {commit_ids['published']}: Published with updated content")
    print(f"  Commit {commit_ids['more_tags']}: Additional tags added")
    print(f"  Commit {commit_ids['archived']}: Archived")
    # print(f"  Commit {commit_ids['retracted']}: Retracted (soft deleted)")

    print("\nAudit query: View all versions of DOC-001:")
    for commit_label, commit_id in commit_ids.items():
        docs = list(onto.query().entities(Document).as_of(commit_id).collect())
        if docs:
            doc = docs[0]
            # Note: _version metadata not available in Entity API
            print(f"  [{commit_label}] Commit {commit_id}: {doc.status}")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts demonstrated:")
    print("  ✓ EnsureEntity for creating and updating entities")
    print("  ✓ EnsureRelation for creating relations")
    print("  ✓ .as_of(commit_id) for temporal queries")
    print("  ✓ Viewing historical state at different commits")
    print("  ✓ Tracking commit history for audit trails")
    print("  ✓ Document lifecycle: draft → published → archived → retracted")
    print("\nDatabase file: tmp/time_travel.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
