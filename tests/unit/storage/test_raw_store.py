from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.domain.events import RawDocument
from gribuki_trade.storage.research.raw_store import BodyNotRetainedError, FileRawDocumentStore

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def document(content: bytes, *, seen: datetime = NOW) -> RawDocument:
    return RawDocument(
        source_id="official",
        canonical_url="https://official.example/notices",
        content_type="text/html",
        content=content,
        first_seen_at=seen,
        retrieved_at=seen,
        available_at=seen,
        etag='"one"',
        last_modified="Thu, 13 Aug 2026 02:00:00 GMT",
        encoding="utf-8",
    )


def test_content_addressed_store_round_trips_and_does_not_overwrite(tmp_path) -> None:
    store = FileRawDocumentStore(tmp_path / "raw")
    source = document(b"first")

    created = store.save(source)
    duplicate = store.save(source)
    loaded = store.load(source.document_id)

    assert created.created is True and created.body_path is not None
    assert duplicate.created is False
    assert loaded == source
    assert created.metadata_path.read_text(encoding="utf-8").count("content_sha256") == 1


def test_revisions_are_separate_and_ordered_by_first_observation(tmp_path) -> None:
    store = FileRawDocumentStore(tmp_path / "raw")
    first = document(b"first")
    second = document(b"corrected", seen=NOW + timedelta(minutes=2))
    store.save(second)
    store.save(first)

    assert store.revisions(
        source_id="official",
        canonical_url=first.canonical_url,
    ) == (first.document_id, second.document_id)


def test_metadata_only_record_does_not_persist_response_body(tmp_path) -> None:
    store = FileRawDocumentStore(tmp_path / "raw")
    source = document(b"copyrighted body")

    stored = store.save(source, retain_body=False)

    assert stored.body_retained is False
    assert stored.body_path is None
    with pytest.raises(BodyNotRetainedError):
        store.load(source.document_id)


@pytest.mark.parametrize("document_id", ["../bad", "A" * 64, "f" * 63])
def test_document_id_is_validated_before_path_access(tmp_path, document_id: str) -> None:
    with pytest.raises(ValueError, match="document_id"):
        FileRawDocumentStore(tmp_path / "raw").load(document_id)
