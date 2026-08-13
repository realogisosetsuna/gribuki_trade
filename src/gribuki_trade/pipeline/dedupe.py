"""Exact event deduplication with append-only revision semantics."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from gribuki_trade.domain.events import NormalizedEvent


class EventDisposition(StrEnum):
    NEW = "NEW"
    DUPLICATE = "DUPLICATE"
    REVISION = "REVISION"


@dataclass(frozen=True, slots=True)
class DedupeDecision:
    disposition: EventDisposition
    event: NormalizedEvent
    accepted: bool


class EventDeduplicator:
    """Maintain revisions by stable event identity without mutating history."""

    def __init__(self, seed: tuple[NormalizedEvent, ...] = ()) -> None:
        self._revisions: dict[str, list[NormalizedEvent]] = {}
        for event in seed:
            revisions = self._revisions.setdefault(event.event_id, [])
            if revisions and event.revision_number <= revisions[-1].revision_number:
                raise ValueError("seed revisions must be strictly increasing")
            revisions.append(event)

    def ingest(self, event: NormalizedEvent) -> DedupeDecision:
        revisions = self._revisions.get(event.event_id)
        if revisions is None:
            initial = replace(
                event,
                revision_number=1,
                supersedes_revision_id=None,
            )
            self._revisions[event.event_id] = [initial]
            return DedupeDecision(EventDisposition.NEW, initial, True)

        for existing in revisions:
            if existing.content_sha256 == event.content_sha256:
                return DedupeDecision(EventDisposition.DUPLICATE, existing, False)

        previous = revisions[-1]
        revision = replace(
            event,
            event_id=previous.event_id,
            revision_number=previous.revision_number + 1,
            supersedes_revision_id=previous.revision_id,
        )
        revisions.append(revision)
        return DedupeDecision(EventDisposition.REVISION, revision, True)

    def revisions(self, event_id: str) -> tuple[NormalizedEvent, ...]:
        return tuple(self._revisions.get(event_id, ()))

    def latest(self) -> tuple[NormalizedEvent, ...]:
        return tuple(revisions[-1] for revisions in self._revisions.values())
