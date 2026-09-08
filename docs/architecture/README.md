# Architecture reading order

Start with [`ARCHITECTURE.md`](../../ARCHITECTURE.md), then choose the smallest
page that matches the task:

- [`module-map.md`](module-map.md): package ownership and representative tests.
- [`execution-boundaries.md`](execution-boundaries.md): PAPER/SHADOW/LIVE,
  research, LLM and secret boundaries.
- [`data-lineage.md`](data-lineage.md): evidence provenance, PIT behavior and
  durable stores.
- [`design-decisions.md`](design-decisions.md): reasons encoded by current
  implementation and regression tests.
- [`verification-map.md`](verification-map.md): quality gates, test families
  and known verification gaps.

If a task changes a boundary or durable invariant, update the matching page and
its tests in the same change.
