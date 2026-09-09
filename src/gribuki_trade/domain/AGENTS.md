# Domain map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/module-map.md`](../../../docs/architecture/module-map.md), and tests under `tests/unit/` before changing shared value objects.

Keep domain models, events and invariants pure. Domain code must not import adapters, services, storage, GUI or CLI.
