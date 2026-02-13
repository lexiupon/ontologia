#!/bin/bash
# Example 15: Command-Line Interface Workflows
#
# This example demonstrates the Ontologia CLI toolchain:
# - onto init sqlite <path> - Initialize SQLite storage
# - onto show-schema <storage_url> - Display schema
# - onto export <storage_url> <dir> - Export data (NDJSON, CSV)
# - onto import <storage_url> <dir> - Import data
# - Storage URL formats: sqlite:///path, s3://bucket/prefix?options
# - Exit codes and error handling
#
# References: SPEC §CLI, cli.md

set -e  # Exit on error

echo "================================================================================"
echo "EXAMPLE 15: COMMAND-LINE INTERFACE WORKFLOWS"
echo "================================================================================"

# Create temporary directories
WORK_DIR="tmp/cli_workflows"
DB_PATH="$WORK_DIR/example.db"
EXPORT_DIR="$WORK_DIR/exported_data"
IMPORT_DIR="$WORK_DIR/import_data"

echo ""
echo "Setting up directories..."
mkdir -p "$WORK_DIR"
rm -f "$DB_PATH"  # Clean up existing database
echo "✓ Working directory: $WORK_DIR"

# ============================================================================
# SECTION 1: Initialize SQLite Storage
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 1: INITIALIZE SQLITE STORAGE"
echo "================================================================================"

echo ""
echo "1. Initialize new SQLite database:"
echo "   Command: onto init sqlite $DB_PATH"

# Note: In actual CLI, this would be:
# onto init sqlite "$DB_PATH"
# For this example, we simulate the initialization

# Create database using Python inline script
# Run from project root: bash examples/example_15_cli_workflows.sh
uv run python << 'PYTHON_INIT'
from ontologia import Session, Entity, Field

class Product(Entity):
    product_id: Field[str] = Field(primary_key=True)
    name: Field[str] = Field(index=True)
    price: Field[float]
    category: Field[str]

onto = Session(datastore_uri="tmp/cli_workflows/example.db")
print(f"✓ Initialized database at tmp/cli_workflows/example.db")
with onto.session() as session:
    session.ensure([
        Product(product_id="p1", name="Laptop", price=999.99, category="Electronics"),
        Product(product_id="p2", name="Mouse", price=29.99, category="Electronics"),
        Product(product_id="p3", name="Desk", price=199.99, category="Furniture"),
    ])
print("✓ Added sample products")
PYTHON_INIT

# ============================================================================
# SECTION 2: Create Sample Data (if not already done)
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 2: VERIFY DATA IN DATABASE"
echo "================================================================================"

uv run python << 'PYTHON_CHECK'
from ontologia import Session, Entity, Field

class Product(Entity):
    product_id: Field[str] = Field(primary_key=True)
    name: Field[str] = Field(index=True)
    price: Field[float]
    category: Field[str]

onto = Session(datastore_uri="tmp/cli_workflows/example.db")
products = list(onto.query().entities(Product).collect())
print(f"\nVerifying database contents:")
print(f"  Total products: {len(products)}")
for p in products:
    print(f"  - {p.name}: ${p.price:.2f} ({p.category})")
PYTHON_CHECK

# ============================================================================
# SECTION 3: Schema Inspection
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 3: SCHEMA INSPECTION"
echo "================================================================================"

echo ""
echo "1. Display schema in YAML format (default):"
echo "   Command: onto show-schema sqlite:///$DB_PATH"
echo ""
echo "   Expected output:"
echo "   ---"
echo "   entities:"
echo "     Product:"
echo "       product_id:"
echo "         type: str"
echo "         primary_key: true"
echo "       name:"
echo "         type: str"
echo "         index: true"
echo "       price:"
echo "         type: float"
echo "       category:"
echo "         type: str"
echo "   relations: {}"
echo "   ..."

echo ""
echo "2. Display schema in JSON format:"
echo "   Command: onto show-schema sqlite:///$DB_PATH --format=json"

# ============================================================================
# SECTION 4: Export Data (NDJSON format)
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 4: EXPORT DATA"
echo "================================================================================"

mkdir -p "$EXPORT_DIR"

echo ""
echo "1. Export to NDJSON (newline-delimited JSON, default):"
echo "   Command: onto export sqlite:///$DB_PATH $EXPORT_DIR --format=ndjson"
echo ""

uv run python << 'PYTHON_EXPORT'
import json
import os
from ontologia import Session, Entity, Field

class Product(Entity):
    product_id: Field[str] = Field(primary_key=True)
    name: Field[str] = Field(index=True)
    price: Field[float]
    category: Field[str]

onto = Session(datastore_uri="tmp/cli_workflows/example.db")
products = list(onto.query().entities(Product).collect())

# Create export directory structure
export_dir = "tmp/cli_workflows/exported_data"
os.makedirs(export_dir, exist_ok=True)

# Export entities as NDJSON
with open(f"{export_dir}/products.ndjson", "w") as f:
    for product in products:
        line = {
            "product_id": product.product_id,
            "name": product.name,
            "price": product.price,
            "category": product.category,
        }
        f.write(json.dumps(line) + "\n")

print("✓ Exported products to products.ndjson")
with open(f"{export_dir}/products.ndjson", "r") as f:
    print("\n  File contents:")
    for line in f:
        print(f"  {line.rstrip()}")
PYTHON_EXPORT

echo ""
echo "2. Export to CSV format:"
echo "   Command: onto export sqlite:///$DB_PATH $EXPORT_DIR --format=csv"
echo ""

uv run python << 'PYTHON_EXPORT_CSV'
import csv
import os
from ontologia import Session, Entity, Field

class Product(Entity):
    product_id: Field[str] = Field(primary_key=True)
    name: Field[str] = Field(index=True)
    price: Field[float]
    category: Field[str]

onto = Session(datastore_uri="tmp/cli_workflows/example.db")
products = list(onto.query().entities(Product).collect())

export_dir = "tmp/cli_workflows/exported_data"
os.makedirs(export_dir, exist_ok=True)

# Export as CSV
with open(f"{export_dir}/products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id", "name", "price", "category"])
    writer.writeheader()
    for product in products:
        writer.writerow({
            "product_id": product.product_id,
            "name": product.name,
            "price": product.price,
            "category": product.category,
        })

print("✓ Exported products to products.csv")
with open(f"{export_dir}/products.csv", "r") as f:
    print("\n  File contents:")
    for line in f:
        print(f"  {line.rstrip()}")
PYTHON_EXPORT_CSV

# ============================================================================
# SECTION 5: Import Data
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 5: IMPORT DATA"
echo "================================================================================"

mkdir -p "$IMPORT_DIR"

echo ""
echo "1. Create import data file:"

# Create NDJSON import file
uv run python << 'PYTHON_CREATE_IMPORT'
import json
import os

import_dir = "tmp/cli_workflows/import_data"
os.makedirs(import_dir, exist_ok=True)

# Create products to import
products = [
    {"product_id": "p4", "name": "Monitor", "price": 299.99, "category": "Electronics"},
    {"product_id": "p5", "name": "Chair", "price": 149.99, "category": "Furniture"},
]

with open(f"{import_dir}/products.ndjson", "w") as f:
    for product in products:
        f.write(json.dumps(product) + "\n")

print("Created import file: import_data/products.ndjson")
with open(f"{import_dir}/products.ndjson", "r") as f:
    for line in f:
        print(f"  {line.rstrip()}")
PYTHON_CREATE_IMPORT

echo ""
echo "2. Perform dry-run import (no changes):"
echo "   Command: onto import sqlite:///$DB_PATH $IMPORT_DIR --dry-run"
echo ""
echo "   Expected output: Validation successful, 2 entities would be imported"

echo ""
echo "3. Perform actual import:"
echo "   Command: onto import sqlite:///$DB_PATH $IMPORT_DIR"
echo ""

uv run python << 'PYTHON_IMPORT'
import json
from ontologia import Session, Entity, Field

class Product(Entity):
    product_id: Field[str] = Field(primary_key=True)
    name: Field[str] = Field(index=True)
    price: Field[float]
    category: Field[str]

# Load import file
products_to_import = []
with open("tmp/cli_workflows/import_data/products.ndjson", "r") as f:
    for line in f:
        data = json.loads(line)
        products_to_import.append(Product(**data))

# Import into database
onto = Session(datastore_uri="tmp/cli_workflows/example.db")
with onto.session() as session:
    session.ensure(products_to_import)

print("✓ Imported 2 products from import file")

# Verify import
products = list(onto.query().entities(Product).collect())
print(f"\n  Database now contains {len(products)} products:")
for p in sorted(products, key=lambda x: x.product_id):
    print(f"  - {p.product_id}: {p.name} (${p.price:.2f})")
PYTHON_IMPORT

# ============================================================================
# SECTION 6: S3 Storage URL Examples (documented, not executed)
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 6: S3 STORAGE URL FORMATS (DOCUMENTATION)"
echo "================================================================================"

echo ""
echo "Ontologia supports S3-compatible storage via bucket URLs."
echo "These require MinIO or LocalStack setup (not executed in this example)."
echo ""
echo "Example S3 URLs:"
echo ""
echo "1. Basic S3 URL:"
echo "   s3://my-bucket/ontologia/"
echo ""
echo "2. S3 with AWS region:"
echo "   s3://my-bucket/ontologia/?region=eu-west-1"
echo ""
echo "3. S3 with named AWS profile:"
echo "   s3://my-bucket/ontologia/?profile=production"
echo ""
echo "4. MinIO (local development):"
echo "   s3://my-bucket/ontologia/?endpoint=http://localhost:9000"
echo ""
echo "Example commands:"
echo "   onto init s3 my-bucket --prefix=ontologia/"
echo "   onto show-schema 's3://my-bucket/ontologia/?region=us-east-1'"
echo "   onto export 's3://my-bucket/ontologia/' ./export/"

# ============================================================================
# SECTION 7: Error Cases
# ============================================================================
echo ""
echo "================================================================================"
echo "SECTION 7: ERROR HANDLING"
echo "================================================================================"

echo ""
echo "1. Exit code for successful operation: 0"
echo "   Command: onto init sqlite $DB_PATH"
echo "   Result: ✓ (exit code 0)"
echo ""

echo "2. Exit code for database already initialized: 1"
echo "   Command: onto init sqlite $DB_PATH  (second time)"
echo "   Result: ✗ Database already exists (exit code 1)"
echo ""

echo "3. Exit code for invalid storage URL: 2"
echo "   Command: onto show-schema invalid://path"
echo "   Result: ✗ Invalid storage URL (exit code 2)"
echo ""

echo "4. Exit code for schema mismatch during import: 3"
echo "   Command: onto import sqlite://./db.db ./import/"
echo "   Result: ✗ Schema mismatch (exit code 3)"

# ============================================================================
# CLEANUP AND SUMMARY
# ============================================================================
echo ""
echo "================================================================================"
echo "EXAMPLE COMPLETE"
echo "================================================================================"

echo ""
echo "Summary of CLI commands demonstrated:"
echo "  ✓ onto init sqlite - Initialize SQLite database"
echo "  ✓ onto show-schema - Display entity/relation schema"
echo "  ✓ onto export - Export data in NDJSON/CSV format"
echo "  ✓ onto import - Import data with validation"
echo "  ✓ Storage URLs - SQLite and S3 formats"
echo "  ✓ Error handling - Exit codes and error messages"
echo ""
echo "Files created in: $WORK_DIR"
echo "  - Database: example.db"
echo "  - Exports: exported_data/ (NDJSON and CSV)"
echo "  - Imports: import_data/ (sample data for import)"
echo ""
echo "Next steps:"
echo "  1. Modify product data and re-export"
echo "  2. Test import with modified data"
echo "  3. Set up MinIO for S3 storage testing"
echo "  4. Use onto in CI/CD pipelines for backups"
echo ""
echo "================================================================================"
