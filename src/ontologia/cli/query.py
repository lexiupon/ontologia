"""onto query — run manual reads for entities, relations, and traversals."""

from __future__ import annotations

from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._filters import parse_cli_filters
from ontologia.cli._loader import load_models
from ontologia.cli._output import print_error, print_object, print_table
from ontologia.cli._storage import open_ontology

app = typer.Typer(no_args_is_help=True)


@app.command(name="entities")
def query_entities_cmd(
    type_name: str = typer.Argument(..., help="Entity type name"),
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    filter_args: Optional[list[str]] = typer.Option(
        None, "--filter", help="PATH OP VALUE_JSON (repeatable)"
    ),
    limit: Optional[int] = typer.Option(None, "--limit", help="Max results"),
    offset: Optional[int] = typer.Option(None, "--offset", help="Skip first N results"),
    as_of: Optional[int] = typer.Option(None, "--as-of", help="Historical snapshot at commit ID"),
    with_history: bool = typer.Option(False, "--with-history", help="Return all versions"),
    history_since: Optional[int] = typer.Option(
        None, "--history-since", help="Changes since commit ID"
    ),
) -> None:
    """Query entities of a given type."""
    from ontologia.cli import state

    json_mode = state.json_output

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    if type_name not in entity_types:
        print_error(f"Entity type '{type_name}' not found in models")
        raise typer.Exit(ec.USAGE_ERROR)

    filter_expr = _parse_filter_args(filter_args)

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        cls = entity_types[type_name]
        q = onto.query().entities(cls)

        if filter_expr:
            q = q.where(filter_expr)
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        if as_of is not None:
            q = q.as_of(as_of)
        if with_history:
            q = q.with_history()
        if history_since is not None:
            q = q.history_since(history_since)

        results = q.collect()
        _print_entity_results(results, cls, json_mode)
        _print_repo_query_diagnostics(onto.repo, json_mode=json_mode)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        try:
            onto.close()
        except Exception:
            pass


@app.command(name="relations")
def query_relations_cmd(
    type_name: str = typer.Argument(..., help="Relation type name"),
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    filter_args: Optional[list[str]] = typer.Option(
        None, "--filter", help="PATH OP VALUE_JSON (repeatable)"
    ),
    limit: Optional[int] = typer.Option(None, "--limit", help="Max results"),
    offset: Optional[int] = typer.Option(None, "--offset", help="Skip first N results"),
    as_of: Optional[int] = typer.Option(None, "--as-of", help="Historical snapshot at commit ID"),
    with_history: bool = typer.Option(False, "--with-history", help="Return all versions"),
    history_since: Optional[int] = typer.Option(
        None, "--history-since", help="Changes since commit ID"
    ),
) -> None:
    """Query relations of a given type."""
    from ontologia.cli import state

    json_mode = state.json_output

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    if type_name not in relation_types:
        print_error(f"Relation type '{type_name}' not found in models")
        raise typer.Exit(ec.USAGE_ERROR)

    filter_expr = _parse_filter_args(filter_args)

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        cls = relation_types[type_name]
        q = onto.query().relations(cls)

        if filter_expr:
            q = q.where(filter_expr)
        if limit is not None:
            q = q.limit(limit)
        if offset is not None:
            q = q.offset(offset)
        if as_of is not None:
            q = q.as_of(as_of)
        if with_history:
            q = q.with_history()
        if history_since is not None:
            q = q.history_since(history_since)

        results = q.collect()
        _print_relation_results(results, cls, json_mode)
        _print_repo_query_diagnostics(onto.repo, json_mode=json_mode)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        try:
            onto.close()
        except Exception:
            pass


@app.command(name="traverse")
def query_traverse_cmd(
    root_type: str = typer.Argument(..., help="Root entity type name"),
    via: list[str] = typer.Option(..., "--via", help="Relation types to traverse"),
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    filter_args: Optional[list[str]] = typer.Option(
        None, "--filter", help="PATH OP VALUE_JSON (repeatable)"
    ),
    without_relations: bool = typer.Option(
        False, "--without-relations", help="Return only destination entities"
    ),
) -> None:
    """Traverse from entities via relations."""
    from ontologia.cli import state

    json_mode = state.json_output

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    if root_type not in entity_types:
        print_error(f"Entity type '{root_type}' not found in models")
        raise typer.Exit(ec.USAGE_ERROR)

    for v in via:
        if v not in relation_types:
            print_error(f"Relation type '{v}' not found in models")
            raise typer.Exit(ec.USAGE_ERROR)

    filter_expr = _parse_filter_args(filter_args)

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        root_cls = entity_types[root_type]
        q = onto.query().entities(root_cls)
        if filter_expr:
            q = q.where(filter_expr)

        # Chain .via() calls
        tq = q.via(relation_types[via[0]])
        for v in via[1:]:
            tq = tq.via(relation_types[v])

        if without_relations:
            entities = tq.without_relations()
            data = []
            for e in entities:
                item: dict[str, Any] = {"key": e.meta().key, **e.model_dump()}
                if hasattr(e, "__onto_meta__") and e.__onto_meta__:
                    item["commit_id"] = e.__onto_meta__.commit_id
                data.append(item)
            print_object(data, json_mode=json_mode)
        else:
            paths = tq.collect()
            data = []
            for path in paths:
                source_data: dict[str, Any] = {
                    "source_key": path.source.meta().key,
                    "source": path.source.model_dump(),
                }
                rels = []
                for rel in path.relations:
                    rels.append(
                        {
                            "left_key": rel.left_key,
                            "right_key": rel.right_key,
                            "fields": rel.model_dump(),
                        }
                    )
                source_data["relations"] = rels
                data.append(source_data)
            print_object(data, json_mode=json_mode)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        try:
            onto.close()
        except Exception:
            pass


def _parse_filter_args(filter_args: list[str] | None) -> Any:
    """Parse --filter args (each is a group of 3 tokens: PATH OP VALUE_JSON)."""
    if not filter_args:
        return None

    # Typer passes each --filter value as a single string, but the spec says
    # --filter consumes exactly 3 tokens: PATH OP VALUE_JSON
    # With typer, each --filter gets one string, so we expect "PATH OP VALUE_JSON" space-separated
    # or three separate --filter args. Let's handle both.
    triples: list[tuple[str, str, str]] = []

    # Try to parse as groups of 3
    if len(filter_args) % 3 == 0:
        for i in range(0, len(filter_args), 3):
            triples.append((filter_args[i], filter_args[i + 1], filter_args[i + 2]))
    else:
        # Try each as "PATH OP VALUE_JSON" space-separated
        for arg in filter_args:
            parts = arg.split(None, 2)
            if len(parts) != 3:
                from ontologia.cli._output import print_error

                print_error(f"Invalid filter (expected 'PATH OP VALUE_JSON'): {arg}")
                raise typer.Exit(ec.USAGE_ERROR)
            triples.append((parts[0], parts[1], parts[2]))

    return parse_cli_filters(triples)


def _print_entity_results(results: list[Any], cls: type, json_mode: bool) -> None:
    """Print entity query results."""
    if json_mode:
        data = []
        for e in results:
            item: dict[str, Any] = {"key": e.meta().key, **e.model_dump()}
            item["commit_id"] = e.meta().commit_id
            data.append(item)
        print_object(data, json_mode=True)
        return

    if not results:
        print("No results.")
        return

    # Build table
    field_names = list(cls.__entity_fields__)
    headers = ["key"] + field_names + ["commit_id"]
    rows = []
    for e in results:
        row: list[Any] = [e.meta().key]
        for f in field_names:
            row.append(getattr(e, f))
        row.append(e.meta().commit_id)
        rows.append(row)
    print_table(headers, rows)


def _print_relation_results(results: list[Any], cls: type, json_mode: bool) -> None:
    """Print relation query results."""
    if json_mode:
        data = []
        for r in results:
            item: dict[str, Any] = {
                "left_key": r.left_key,
                "right_key": r.right_key,
                **r.model_dump(),
            }
            item["commit_id"] = r.meta().commit_id
            if r.instance_key:
                item["instance_key"] = r.instance_key
            data.append(item)
        print_object(data, json_mode=True)
        return

    if not results:
        print("No results.")
        return

    field_names = list(cls.__relation_fields__)
    headers = ["left_key", "right_key"] + field_names + ["commit_id"]
    rows = []
    for r in results:
        row: list[Any] = [r.left_key, r.right_key]
        for f in field_names:
            row.append(getattr(r, f))
        row.append(r.meta().commit_id)
        rows.append(row)
    print_table(headers, rows)


def _print_repo_query_diagnostics(repo: Any, *, json_mode: bool) -> None:
    if json_mode:
        return
    if not hasattr(repo, "get_last_query_diagnostics"):
        return
    diag = repo.get_last_query_diagnostics()
    if not isinstance(diag, dict):
        return
    if diag.get("reason") == "commit_before_activation":
        activation = diag.get("activation_commit_id")
        print(
            f"Warning: requested commit is before schema activation boundary "
            f"(activation_commit_id={activation}); returned empty result."
        )
