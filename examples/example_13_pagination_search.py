"""Example 13: Pagination, Ordering, and Text Search.

This example demonstrates pagination and search capabilities:
- Basic pagination with limit() and offset()
- Ordering results with order_by()
- String search operators: startswith(), endswith(), contains()
- Multi-value filtering with in_()
- Cursor-based pagination for performance
- Field indexing for search performance

References: SPEC §Query API, api.md lines 309-322, 249-254
"""

from ontologia import Entity, Field, Session


class Article(Entity):
    """Blog article with searchable fields."""

    article_id: Field[str] = Field(primary_key=True)
    title: Field[str] = Field(index=True)  # Indexed for search
    author: Field[str] = Field(index=True)  # Indexed for filtering
    content: Field[str]  # Not indexed - less frequently searched
    published_at: Field[str]  # ISO datetime
    view_count: Field[int] = Field(default=0)
    category: Field[str] = Field(index=True)


def setup_sample_data(onto: Session) -> None:
    """Create 50 articles with diverse data for pagination demonstrations."""
    articles = []

    categories = ["Technology", "Business", "Science", "Culture", "Health"]
    authors = [
        "Alice Johnson",
        "Bob Smith",
        "Carol White",
        "David Brown",
        "Eve Davis",
    ]

    # Create 50 articles
    for i in range(50):
        category = categories[i % len(categories)]
        author = authors[i % len(authors)]

        article = Article(
            article_id=f"article-{i + 1:03d}",
            title=f"{category} Article #{i + 1}: Deep Dive into Modern Trends",
            author=author,
            content=f"This is a detailed article about {category.lower()} topic #{i + 1}. "
            f"It covers important insights and best practices. Content continues...",
            published_at=f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T10:00:00Z",
            view_count=i * 10 + (i % 5) * 100,
            category=category,
        )
        articles.append(article)

    with onto.session() as session:
        session.ensure(articles)


def main():
    """Run pagination and search example."""
    print("=" * 80)
    print("EXAMPLE 13: PAGINATION, ORDERING, AND TEXT SEARCH")
    print("=" * 80)

    # Initialize ontology
    onto = Session(datastore_uri="tmp/pagination_search.db")
    print("\n✓ Ontology initialized: tmp/pagination_search.db")

    # Create sample data
    print("\n" + "=" * 80)
    print("SETTING UP SAMPLE DATA")
    print("=" * 80)
    setup_sample_data(onto)
    print("✓ Created 50 articles across 5 categories and 5 authors")

    # Section 1: Basic Pagination
    print("\n" + "=" * 80)
    print("SECTION 1: BASIC PAGINATION")
    print("=" * 80)
    print("\nPaginating through articles: 5 articles per page")

    page_size = 5
    for page_num in range(3):  # Show first 3 pages
        offset = page_num * page_size
        query = onto.query().entities(Article).limit(page_size).offset(offset)
        articles = list(query.collect())

        print(f"\nPage {page_num + 1} (offset {offset}):")
        for article in articles:
            print(f"  - {article.title[:50]}... (views: {article.view_count})")
        print(f"  {len(articles)} articles on this page")

    # Section 2: Ordering Results
    print("\n" + "=" * 80)
    print("SECTION 2: ORDERING RESULTS")
    print("=" * 80)

    print("\n1. Articles ordered by publication date (ascending):")
    query = onto.query().entities(Article).order_by("published_at").limit(5)
    articles = list(query.collect())
    for article in articles:
        print(f"  - {article.published_at}: {article.title[:40]}...")

    print("\n2. Articles ordered by view count (descending - most popular):")
    query = onto.query().entities(Article).order_by("-view_count").limit(5)
    articles = list(query.collect())
    for article in articles:
        print(f"  - Views: {article.view_count:5d} | {article.title[:40]}...")

    print("\n3. Articles ordered by author (alphabetical):")
    query = onto.query().entities(Article).order_by("author").limit(5)
    articles = list(query.collect())
    current_author = None
    for article in articles:
        if article.author != current_author:
            print(f"\n  Author: {article.author}")
            current_author = article.author
        print(f"    - {article.title[:40]}...")

    # Section 3: Text Search - Exact and Prefix
    print("\n" + "=" * 80)
    print("SECTION 3: TEXT SEARCH PATTERNS")
    print("=" * 80)

    print("\n1. Articles with title starting with 'Technology':")
    query = onto.query().entities(Article).where(Article.title.startswith("Technology"))
    articles = list(query.collect())
    print(f"  Found {len(articles)} articles:")
    for article in articles[:3]:
        print(f"  - {article.title}")

    print("\n2. Articles with title containing 'Trends':")
    query = onto.query().entities(Article).where(Article.title.contains("Trends"))
    articles = list(query.collect())
    print(f"  Found {len(articles)} articles:")
    for article in articles[:3]:
        print(f"  - {article.title}")

    print("\n3. Articles with title ending with 'Trends' or 'Best Practices':")
    query = onto.query().entities(Article).where(Article.title.endswith("Trends"))
    articles = list(query.collect())
    print(f"  Found {len(articles)} articles")

    # Section 4: Multi-value Filtering
    print("\n" + "=" * 80)
    print("SECTION 4: MULTI-VALUE FILTERING")
    print("=" * 80)

    tech_authors = ["Alice Johnson", "Bob Smith"]
    print(f"\nArticles by specific authors: {tech_authors}")
    query = onto.query().entities(Article).where(Article.author.in_(tech_authors))
    articles = list(query.collect())
    print(f"  Found {len(articles)} articles:")
    for article in articles[:5]:
        print(f"  - {article.author}: {article.title[:45]}...")

    categories_to_search = ["Technology", "Science"]
    print(f"\nArticles in categories: {categories_to_search}")
    query = (
        onto.query()
        .entities(Article)
        .where(Article.category.in_(categories_to_search))
        .order_by("category")
    )
    articles = list(query.collect())
    print(f"  Found {len(articles)} articles:")
    current_category = None
    for article in articles[:8]:
        if article.category != current_category:
            print(f"\n  Category: {article.category}")
            current_category = article.category
        print(f"    - {article.title[:40]}...")

    # Section 5: Cursor-based Pagination (Performance optimization)
    print("\n" + "=" * 80)
    print("SECTION 5: CURSOR-BASED PAGINATION")
    print("=" * 80)
    print("\nCursor-based pagination is more efficient than offset for large datasets")

    print("\nFetching first batch of articles sorted by ID:")
    first_batch_size = 10
    first_batch = list(
        onto.query().entities(Article).order_by("article_id").limit(first_batch_size).collect()
    )
    print(f"  Fetched {len(first_batch)} articles")
    for article in first_batch[:3]:
        print(f"  - {article.article_id}")

    if first_batch:
        last_id = first_batch[-1].article_id
        print(f"\n  Cursor: {last_id} (last article in batch)")

        print(f"\nFetching next batch starting after cursor '{last_id}':")
        next_batch = list(
            onto.query()
            .entities(Article)
            .where(Article.article_id > last_id)
            .order_by("article_id")
            .limit(first_batch_size)
            .collect()
        )
        print(f"  Fetched {len(next_batch)} articles:")
        for article in next_batch[:3]:
            print(f"  - {article.article_id}")

    # Section 6: Index Performance Comparison
    print("\n" + "=" * 80)
    print("SECTION 6: INDEXED VS NON-INDEXED FIELDS")
    print("=" * 80)

    print("\n1. Querying INDEXED field (author) - typically faster on large datasets:")
    print("  Index definition: author = Field(index=True)")
    query = onto.query().entities(Article).where(Article.author == "Alice Johnson")
    results = list(query.collect())
    print(f"  Query returned {len(results)} articles")

    print("\n2. Querying NON-INDEXED field (content) - full table scan on large datasets:")
    print("  Index definition: content = Field()  # No index")
    query = onto.query().entities(Article).where(Article.content.contains("topic"))
    results = list(query.collect())
    print(f"  Query returned {len(results)} articles")

    print("\nNote: For this 50-article dataset, both are instant.")
    print("On production databases with millions of records, indexed queries")
    print("would be significantly faster (100-1000x in some cases).")

    # Summary
    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts demonstrated:")
    print("  ✓ Basic pagination with limit() and offset()")
    print("  ✓ Ordering with order_by() (ascending and descending)")
    print("  ✓ String search: startswith(), endswith(), contains()")
    print("  ✓ Multi-value filtering with in_()")
    print("  ✓ Cursor-based pagination for better performance")
    print("  ✓ Impact of indexing on query performance")
    print("\nDatabase file: tmp/pagination_search.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
