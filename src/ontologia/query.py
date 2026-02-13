"""Query DSL: EntityQuery, RelationQuery, TraversalQuery, aggregation builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ontologia.filters import ExistsComparisonExpression, FilterExpression
from ontologia.types import Entity, Field, Meta, Relation

if TYPE_CHECKING:
    from ontologia.storage import RepositoryProtocol

E = TypeVar("E", bound=Entity)
R = TypeVar("R", bound=Relation)  # type: ignore[type-arg]


@dataclass
class Path(Generic[E]):
    """A traversal result rooted at a source entity."""

    source: E
    relations: list[Any] = field(default_factory=list)

    @property
    def entities(self) -> list[Any]:
        """Reconstruct the path of entities from source and relations."""
        result: list[Any] = [self.source]
        current = self.source
        for rel in self.relations:
            # Determine next entity based on current
            # Note: This assumes relations are connected correctly
            if getattr(rel, "left", None) == current:
                current = rel.right
            elif getattr(rel, "right", None) == current:
                current = rel.left
            elif getattr(rel, "left_key", None) == getattr(current.meta(), "key"):
                # Fallback to keys if object identity fails (e.g. different instances)
                current = rel.right
            elif getattr(rel, "right_key", None) == getattr(current.meta(), "key"):
                current = rel.left

            result.append(current)
        return result


class AggBuilder:
    """Aggregation builder for group_by().agg() calls."""

    def __init__(self, func: str, field_ref: Any = None) -> None:
        self.func = func
        self._field_ref = field_ref
        self._field_name: str | None = None

        # Extract field name from the field reference
        if field_ref is not None:
            from ontologia.filters import FieldProxy

            if isinstance(field_ref, FieldProxy):
                self._field_name = field_ref._field_path.removeprefix("$.")
            elif isinstance(field_ref, str):
                self._field_name = field_ref
            elif isinstance(field_ref, Field):
                self._field_name = field_ref.name

    def __gt__(self, other: Any) -> HavingExpr:
        return HavingExpr(self, ">", other)

    def __ge__(self, other: Any) -> HavingExpr:
        return HavingExpr(self, ">=", other)

    def __lt__(self, other: Any) -> HavingExpr:
        return HavingExpr(self, "<", other)

    def __le__(self, other: Any) -> HavingExpr:
        return HavingExpr(self, "<=", other)

    def __eq__(self, other: object) -> HavingExpr:  # type: ignore[override]
        return HavingExpr(self, "=", other)

    def __ne__(self, other: object) -> HavingExpr:  # type: ignore[override]
        return HavingExpr(self, "!=", other)

    def to_sql_expr(self, table_alias: str = "eh") -> str:
        if self.func.upper() == "COUNT":
            return "COUNT(*)"
        fname = self._field_name
        return f"{self.func}(json_extract({table_alias}.fields_json, '$.{fname}'))"

    def to_spec(self) -> tuple[str, str | None]:
        return (self.func, self._field_name)


@dataclass
class HavingExpr:
    """A HAVING clause expression."""

    agg: AggBuilder
    op: str
    value: Any


# Module-level aggregation builder functions
def count() -> AggBuilder:
    return AggBuilder("COUNT")


def sum(field_ref: Any) -> AggBuilder:
    return AggBuilder("SUM", field_ref)


def avg(field_ref: Any) -> AggBuilder:
    return AggBuilder("AVG", field_ref)


def min(field_ref: Any) -> AggBuilder:
    return AggBuilder("MIN", field_ref)


def max(field_ref: Any) -> AggBuilder:
    return AggBuilder("MAX", field_ref)


class GroupedQuery:
    """Query after group_by(), before agg()."""

    def __init__(
        self,
        repo: RepositoryProtocol,
        type_name: str,
        entity_cls: type | None,
        relation_cls: type | None,
        group_field: str,
        filter_expr: FilterExpression | None,
        query_kind: str,  # "entity" or "relation"
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
    ) -> None:
        self._repo = repo
        self._type_name = type_name
        self._entity_cls = entity_cls
        self._relation_cls = relation_cls
        self._group_field = group_field
        self._filter_expr = filter_expr
        self._query_kind = query_kind
        self._having_expr: HavingExpr | None = None
        self._left_entity_type = left_entity_type
        self._right_entity_type = right_entity_type

    def having(self, expr: HavingExpr) -> GroupedQuery:
        self._having_expr = expr
        return self

    def agg(self, **kwargs: AggBuilder) -> list[dict[str, Any]]:
        agg_specs: dict[str, tuple[str, str | None]] = {}
        for alias, builder in kwargs.items():
            agg_specs[alias] = builder.to_spec()

        having_sql: str | None = None
        having_params: list[Any] | None = None
        if self._having_expr is not None:
            h = self._having_expr
            table_alias = "eh" if self._query_kind == "entity" else "rh"
            having_sql = f"{h.agg.to_sql_expr(table_alias)} {h.op} ?"
            having_params = [h.value]

        if self._query_kind == "entity":
            return self._repo.group_by_entities(
                self._type_name,
                self._group_field,
                agg_specs,
                filter_expr=self._filter_expr,
                having_sql_fragment=having_sql,
                having_params=having_params,
            )
        else:
            return self._repo.group_by_relations(
                self._type_name,
                self._group_field,
                agg_specs,
                left_entity_type=self._left_entity_type,
                right_entity_type=self._right_entity_type,
                filter_expr=self._filter_expr,
                having_sql_fragment=having_sql,
                having_params=having_params,
            )


class EntityQuery(Generic[E]):
    """Type-safe query builder for entities."""

    def __init__(
        self,
        repo: RepositoryProtocol,
        entity_cls: type[E],
        current_schema_version_id: int | None = None,
    ) -> None:
        self._repo = repo
        self._entity_cls = entity_cls
        self._type_name = entity_cls.__entity_name__
        self._filter: FilterExpression | None = None
        self._order_by: str | None = None
        self._order_desc: bool = False
        self._limit: int | None = None
        self._offset: int | None = None
        self._with_history: bool = False
        self._history_since: int | None = None
        self._as_of: int | None = None
        self._current_schema_version_id = current_schema_version_id

    def where(self, expr: FilterExpression) -> EntityQuery[E]:
        if self._filter is not None:
            self._filter = self._filter & expr
        else:
            self._filter = expr
        return self

    def order_by(self, field_ref: Any) -> EntityQuery[E]:
        from ontologia.filters import FieldProxy

        if isinstance(field_ref, FieldProxy):
            self._order_by = field_ref._field_path
        elif isinstance(field_ref, str):
            self._order_by = field_ref
        elif isinstance(field_ref, Field):
            self._order_by = f"$.{field_ref.name}"
        return self

    def limit(self, n: int) -> EntityQuery[E]:
        self._limit = n
        return self

    def offset(self, n: int) -> EntityQuery[E]:
        self._offset = n
        return self

    def with_history(self) -> EntityQuery[E]:
        self._with_history = True
        return self

    def history_since(self, commit_id: int) -> EntityQuery[E]:
        self._history_since = commit_id
        return self

    def as_of(self, commit_id: int) -> EntityQuery[E]:
        self._as_of = commit_id
        return self

    def collect(self) -> list[E]:
        rows = self._repo.query_entities(
            self._type_name,
            filter_expr=self._filter,
            order_by=self._order_by,
            order_desc=self._order_desc,
            limit=self._limit,
            offset=self._offset,
            with_history=self._with_history,
            history_since=self._history_since,
            as_of=self._as_of,
            schema_version_id=self._current_schema_version_id,
        )
        return [self._hydrate(r) for r in rows]

    def first(self) -> E | None:
        self._limit = 1
        results = self.collect()
        return results[0] if results else None

    def via(self, relation_type: type) -> TraversalQuery[E]:
        return TraversalQuery(
            repo=self._repo,
            source_cls=self._entity_cls,
            source_filter=self._filter,
            traversals=[(relation_type,)],
        )

    # Aggregations
    def count(self) -> int:
        return self._repo.count_entities(self._type_name, filter_expr=self._filter)

    def sum(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_entities(
            self._type_name, "SUM", fname, filter_expr=self._filter
        )

    def avg(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_entities(
            self._type_name, "AVG", fname, filter_expr=self._filter
        )

    def min(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_entities(
            self._type_name, "MIN", fname, filter_expr=self._filter
        )

    def max(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_entities(
            self._type_name, "MAX", fname, filter_expr=self._filter
        )

    def count_where(self, predicate: ExistsComparisonExpression) -> int:
        """Count entities matching the current filter AND the existential predicate."""
        combined = self._filter & predicate if self._filter else predicate
        return self._repo.count_entities(self._type_name, filter_expr=combined)

    def avg_len(self, field_ref: Any) -> float | None:
        """Compute AVG(json_array_length(...)) for a list field. NULL excluded, [] = 0."""
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_entities(
            self._type_name, "AVG_LEN", fname, filter_expr=self._filter
        )

    def group_by(self, field_ref: Any) -> GroupedQuery:
        fname = _extract_field_name(field_ref)
        return GroupedQuery(
            repo=self._repo,
            type_name=self._type_name,
            entity_cls=self._entity_cls,
            relation_cls=None,
            group_field=fname,
            filter_expr=self._filter,
            query_kind="entity",
        )

    def _hydrate(self, row: dict[str, Any]) -> E:
        entity = self._entity_cls(**row["fields"])
        entity.__onto_meta__ = Meta(  # type: ignore[attr-defined]
            commit_id=row["commit_id"],
            type_name=self._type_name,
            key=row["key"],
        )
        return entity


class RelationQuery(Generic[R]):
    """Type-safe query builder for relations."""

    def __init__(
        self,
        repo: RepositoryProtocol,
        relation_cls: type[R],
        current_schema_version_id: int | None = None,
    ) -> None:
        self._repo = repo
        self._relation_cls = relation_cls
        self._type_name = relation_cls.__relation_name__  # type: ignore[attr-defined]
        self._left_type = relation_cls._left_type  # type: ignore[attr-defined]
        self._right_type = relation_cls._right_type  # type: ignore[attr-defined]
        self._filter: FilterExpression | None = None
        self._order_by: str | None = None
        self._order_desc: bool = False
        self._limit: int | None = None
        self._offset: int | None = None
        self._with_history: bool = False
        self._history_since: int | None = None
        self._as_of: int | None = None
        self._current_schema_version_id = current_schema_version_id

    @property
    def _left_entity_type(self) -> str:
        return self._left_type.__entity_name__

    @property
    def _right_entity_type(self) -> str:
        return self._right_type.__entity_name__

    def where(self, expr: FilterExpression) -> RelationQuery[R]:
        if self._filter is not None:
            self._filter = self._filter & expr
        else:
            self._filter = expr
        return self

    def order_by(self, field_ref: Any) -> RelationQuery[R]:
        from ontologia.filters import FieldProxy

        if isinstance(field_ref, FieldProxy):
            self._order_by = field_ref._field_path
        elif isinstance(field_ref, str):
            self._order_by = field_ref
        return self

    def limit(self, n: int) -> RelationQuery[R]:
        self._limit = n
        return self

    def offset(self, n: int) -> RelationQuery[R]:
        self._offset = n
        return self

    def with_history(self) -> RelationQuery[R]:
        self._with_history = True
        return self

    def history_since(self, commit_id: int) -> RelationQuery[R]:
        self._history_since = commit_id
        return self

    def as_of(self, commit_id: int) -> RelationQuery[R]:
        self._as_of = commit_id
        return self

    def collect(self) -> list[R]:
        rows = self._repo.query_relations(
            self._type_name,
            left_entity_type=self._left_entity_type,
            right_entity_type=self._right_entity_type,
            filter_expr=self._filter,
            order_by=self._order_by,
            order_desc=self._order_desc,
            limit=self._limit,
            offset=self._offset,
            with_history=self._with_history,
            history_since=self._history_since,
            as_of=self._as_of,
            schema_version_id=self._current_schema_version_id,
        )
        return [self._hydrate(r) for r in rows]

    def first(self) -> R | None:
        self._limit = 1
        results = self.collect()
        return results[0] if results else None

    # Aggregations
    def count(self) -> int:
        return self._repo.count_relations(
            self._type_name,
            left_entity_type=self._left_entity_type,
            right_entity_type=self._right_entity_type,
            filter_expr=self._filter,
        )

    def sum(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_relations(
            self._type_name, "SUM", fname, filter_expr=self._filter
        )

    def avg(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_relations(
            self._type_name, "AVG", fname, filter_expr=self._filter
        )

    def min(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_relations(
            self._type_name, "MIN", fname, filter_expr=self._filter
        )

    def max(self, field_ref: Any) -> Any:
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_relations(
            self._type_name, "MAX", fname, filter_expr=self._filter
        )

    def count_where(self, predicate: ExistsComparisonExpression) -> int:
        """Count relations matching the current filter AND the existential predicate."""
        combined = self._filter & predicate if self._filter else predicate
        return self._repo.count_relations(
            self._type_name,
            left_entity_type=self._left_entity_type,
            right_entity_type=self._right_entity_type,
            filter_expr=combined,
        )

    def avg_len(self, field_ref: Any) -> float | None:
        """Compute AVG(json_array_length(...)) for a list field. NULL excluded, [] = 0."""
        fname = _extract_field_name(field_ref)
        return self._repo.aggregate_relations(
            self._type_name, "AVG_LEN", fname, filter_expr=self._filter
        )

    def group_by(self, field_ref: Any) -> GroupedQuery:
        from ontologia.filters import FieldProxy

        if isinstance(field_ref, FieldProxy):
            fname = field_ref._field_path
            if fname.startswith("$."):
                fname = fname[2:]
        elif isinstance(field_ref, str):
            fname = field_ref
        else:
            fname = str(field_ref)

        return GroupedQuery(
            repo=self._repo,
            type_name=self._type_name,
            entity_cls=None,
            relation_cls=self._relation_cls,
            group_field=fname,
            filter_expr=self._filter,
            query_kind="relation",
            left_entity_type=self._left_entity_type,
            right_entity_type=self._right_entity_type,
        )

    def _hydrate(self, row: dict[str, Any]) -> R:
        data = {**row["fields"], "left_key": row["left_key"], "right_key": row["right_key"]}
        ik = row.get("instance_key", "")
        if (
            ik
            and hasattr(self._relation_cls, "_instance_key_field")
            and self._relation_cls._instance_key_field
        ):
            data[self._relation_cls._instance_key_field] = ik
        rel = self._relation_cls(**data)
        rel.__onto_meta__ = Meta(  # type: ignore[attr-defined]
            commit_id=row["commit_id"],
            type_name=self._type_name,
            left_key=row["left_key"],
            right_key=row["right_key"],
            instance_key=ik if ik else None,
        )
        # Hydrate endpoint entities
        left_data = self._repo.get_latest_entity(self._left_entity_type, row["left_key"])
        if left_data:
            left_entity = self._left_type(**left_data["fields"])
            left_entity.__onto_meta__ = Meta(
                commit_id=left_data["commit_id"],
                type_name=self._left_entity_type,
                key=row["left_key"],
            )
            rel.left = left_entity

        right_data = self._repo.get_latest_entity(self._right_entity_type, row["right_key"])
        if right_data:
            right_entity = self._right_type(**right_data["fields"])
            right_entity.__onto_meta__ = Meta(
                commit_id=right_data["commit_id"],
                type_name=self._right_entity_type,
                key=row["right_key"],
            )
            rel.right = right_entity

        return rel


class TraversalQuery(Generic[E]):
    """Traversal query starting from entities and following relations."""

    def __init__(
        self,
        repo: RepositoryProtocol,
        source_cls: type[E],
        source_filter: FilterExpression | None,
        traversals: list[tuple[type, ...]],
    ) -> None:
        self._repo = repo
        self._source_cls = source_cls
        self._source_filter = source_filter
        self._traversals = traversals

    def via(self, relation_type: type) -> TraversalQuery[E]:
        return TraversalQuery(
            repo=self._repo,
            source_cls=self._source_cls,
            source_filter=self._source_filter,
            traversals=self._traversals + [(relation_type,)],
        )

    def where(self, expr: FilterExpression) -> TraversalQuery[E]:
        if self._source_filter is not None:
            self._source_filter = self._source_filter & expr
        else:
            self._source_filter = expr
        return self

    def collect(self) -> list[Path[E]]:
        # Get source entities
        source_rows = self._repo.query_entities(
            self._source_cls.__entity_name__,
            filter_expr=self._source_filter,
        )

        paths: list[Path[E]] = []
        for src_row in source_rows:
            source = self._source_cls(**src_row["fields"])
            source.__onto_meta__ = Meta(  # type: ignore[attr-defined]
                commit_id=src_row["commit_id"],
                type_name=self._source_cls.__entity_name__,
                key=src_row["key"],
            )

            # Traverse relations
            all_relations: list[Any] = []
            current_keys = [src_row["key"]]
            current_entity_type = self._source_cls.__entity_name__

            for (rel_type,) in self._traversals:
                rel_name = rel_type.__relation_name__
                left_type = rel_type._left_type
                right_type = rel_type._right_type

                next_keys: list[str] = []
                # Determine traversal direction and far type for this step
                if left_type.__entity_name__ == current_entity_type:
                    direction_for_step = "left"
                    far_type = right_type
                else:
                    direction_for_step = "right"
                    far_type = left_type

                for key in current_keys:
                    rel_rows = self._repo.get_relations_for_entity(
                        rel_name, current_entity_type, key, direction=direction_for_step
                    )
                    for rel_row in rel_rows:
                        rel_data = {
                            **rel_row["fields"],
                            "left_key": rel_row["left_key"],
                            "right_key": rel_row["right_key"],
                        }
                        ik = rel_row.get("instance_key", "")
                        if (
                            ik
                            and hasattr(rel_type, "_instance_key_field")
                            and rel_type._instance_key_field
                        ):
                            rel_data[rel_type._instance_key_field] = ik
                        rel_inst = rel_type(**rel_data)
                        rel_inst.__onto_meta__ = Meta(
                            commit_id=rel_row["commit_id"],
                            type_name=rel_name,
                            left_key=rel_row["left_key"],
                            right_key=rel_row["right_key"],
                            instance_key=ik if ik else None,
                        )

                        # Hydrate endpoints
                        left_data = self._repo.get_latest_entity(
                            left_type.__entity_name__, rel_row["left_key"]
                        )
                        if left_data:
                            left_ent = left_type(**left_data["fields"])
                            left_ent.__onto_meta__ = Meta(
                                commit_id=left_data["commit_id"],
                                type_name=left_type.__entity_name__,
                                key=rel_row["left_key"],
                            )
                            rel_inst.left = left_ent

                        right_data = self._repo.get_latest_entity(
                            right_type.__entity_name__, rel_row["right_key"]
                        )
                        if right_data:
                            right_ent = right_type(**right_data["fields"])
                            right_ent.__onto_meta__ = Meta(
                                commit_id=right_data["commit_id"],
                                type_name=right_type.__entity_name__,
                                key=rel_row["right_key"],
                            )
                            rel_inst.right = right_ent

                        all_relations.append(rel_inst)

                        far_key = (
                            rel_row["right_key"]
                            if direction_for_step == "left"
                            else rel_row["left_key"]
                        )
                        next_keys.append(far_key)

                current_keys = next_keys
                current_entity_type = far_type.__entity_name__

            paths.append(Path(source=source, relations=all_relations))

        return paths

    def without_relations(self) -> list[E]:
        """Return only the destination entities from traversal."""
        path_results = self.collect()
        entities: list[E] = []
        for path in path_results:
            # For each path, the destination entities are the far endpoints of the last traversal
            if path.relations:
                for rel in path.relations:
                    # The far endpoint depends on traversal direction
                    # For simplicity, use .right if source matches .left type
                    if isinstance(path.source, rel._left_type):
                        entities.append(rel.right)
                    else:
                        entities.append(rel.left)
            else:
                entities.append(path.source)
        return entities


class QueryBuilder:
    """Entry point for building queries."""

    def __init__(
        self,
        repo: RepositoryProtocol,
        schema_version_ids: dict[str, int] | None = None,
    ) -> None:
        self._repo = repo
        self._schema_version_ids = schema_version_ids or {}

    def entities(self, entity_cls: type[E]) -> EntityQuery[E]:
        svid = self._schema_version_ids.get(entity_cls.__entity_name__)
        return EntityQuery(self._repo, entity_cls, current_schema_version_id=svid)

    def relations(self, relation_cls: type[R]) -> RelationQuery[R]:
        rel_name = relation_cls.__relation_name__  # type: ignore[attr-defined]
        svid = self._schema_version_ids.get(rel_name)
        return RelationQuery(self._repo, relation_cls, current_schema_version_id=svid)


def _extract_field_name(field_ref: Any) -> str:
    """Extract a field name from various reference types."""
    from ontologia.filters import FieldProxy

    if isinstance(field_ref, FieldProxy):
        path = field_ref._field_path
        if path.startswith("$."):
            return path[2:]
        return path
    if isinstance(field_ref, str):
        if field_ref.startswith("$."):
            return field_ref[2:]
        return field_ref
    if isinstance(field_ref, Field):
        return field_ref.name
    raise ValueError(f"Cannot extract field name from {field_ref!r}")
