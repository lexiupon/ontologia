# Ontologia

Typed, functional-style ontology library for entity/relation data with
declarative reconciliation and auditable commit history.

## What It Is

Ontologia is a typed, functional-style data management library for entity and
relation data. Handlers declare expected state for targeted identities;
Ontologia computes the delta from current state and applies required changes in
an atomic commit. Its append-only history enables deterministic point-in-time
queries and auditing.

## Specs

- Product and behavior: [`spec/vision.md`](spec/vision.md)
- Public API: [`spec/api.md`](spec/api.md)
- Operator CLI: [`spec/cli.md`](spec/cli.md)

## CLI

```bash
onto --help
```

## Examples

See [`examples/`](examples).
