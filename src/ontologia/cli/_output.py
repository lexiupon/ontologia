"""Output formatting helpers for the CLI."""

from __future__ import annotations

import json
import sys
from typing import Any


def print_table(headers: list[str], rows: list[list[Any]], *, json_mode: bool = False) -> None:
    """Print data as a table (text) or JSON array."""
    if json_mode:
        data = [dict(zip(headers, row)) for row in rows]
        print(json.dumps(data, indent=2, default=str))
        return

    if not rows:
        return

    # Compute column widths
    widths = [len(h) for h in headers]
    str_rows = [[str(v) for v in row] for row in rows]
    for row in str_rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(val))

    # Print header
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * w for w in widths))
    for row in str_rows:
        print(
            "  ".join(val.ljust(widths[i]) if i < len(widths) else val for i, val in enumerate(row))
        )


def print_object(data: dict[str, Any] | list[Any], *, json_mode: bool = False) -> None:
    """Print a single object or list as JSON or key-value pairs."""
    if json_mode:
        print(json.dumps(data, indent=2, default=str))
        return

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for k, v in item.items():
                    print(f"  {k}: {v}")
                print()
            else:
                print(f"  {item}")
        return

    for k, v in data.items():
        print(f"{k}: {v}")


def print_error(msg: str) -> None:
    """Print an error message to stderr."""
    print(f"Error: {msg}", file=sys.stderr)
