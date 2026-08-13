"""Content-addressed, append-only raw response archive."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from gribuki_trade.domain.events import RawDocument


class BodyNotRetainedError(FileNotFoundError):
    """The metadata exists but source policy disabled body retention."""


@dataclass(frozen=True, slots=True)
class StoredRawDocument:
    document_id: str
    content_sha256: str
    metadata_path: Path
    body_path: Path | None
    body_retained: bool
    created: bool


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir=path.parent) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        assert temporary is not None
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


class FileRawDocumentStore:
    """Store bodies by content hash and one immutable metadata record per URL revision."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._records = self._root / "records"
        self._blobs = self._root / "blobs"

    def _record_path(self, document_id: str) -> Path:
        return self._records / document_id[:2] / f"{document_id}.json"

    def _body_path(self, digest: str) -> Path:
        return self._blobs / digest[:2] / f"{digest}.bin"

    def save(self, document: RawDocument, *, retain_body: bool = True) -> StoredRawDocument:
        record_path = self._record_path(document.document_id)
        if record_path.exists():
            metadata = self._read_metadata(document.document_id)
            body_retained = bool(metadata["body_retained"])
            return StoredRawDocument(
                document_id=document.document_id,
                content_sha256=document.content_sha256,
                metadata_path=record_path,
                body_path=self._body_path(document.content_sha256) if body_retained else None,
                body_retained=body_retained,
                created=False,
            )

        body_path: Path | None = None
        if retain_body:
            body_path = self._body_path(document.content_sha256)
            if not body_path.exists():
                _atomic_write(body_path, document.content)
        metadata = {
            "schema_version": 1,
            "document_id": document.document_id,
            "source_id": document.source_id,
            "canonical_url": document.canonical_url,
            "content_type": document.content_type,
            "content_sha256": document.content_sha256,
            "first_seen_at": document.first_seen_at.isoformat(),
            "retrieved_at": document.retrieved_at.isoformat(),
            "available_at": document.available_at.isoformat(),
            "published_at": (
                document.published_at.isoformat() if document.published_at is not None else None
            ),
            "etag": document.etag,
            "last_modified": document.last_modified,
            "encoding": document.encoding,
            "body_retained": retain_body,
        }
        _atomic_write(
            record_path,
            json.dumps(
                metadata,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
        )
        return StoredRawDocument(
            document_id=document.document_id,
            content_sha256=document.content_sha256,
            metadata_path=record_path,
            body_path=body_path,
            body_retained=retain_body,
            created=True,
        )

    def _read_metadata(self, document_id: str) -> dict[str, Any]:
        is_hex = not any(
            character not in "0123456789abcdef" for character in document_id
        )
        if len(document_id) != 64 or not is_hex:
            raise ValueError("document_id must be a lowercase SHA-256 digest")
        data = json.loads(self._record_path(document_id).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("document_id") != document_id:
            raise ValueError("raw document metadata is corrupt")
        return data

    def load(self, document_id: str) -> RawDocument:
        metadata = self._read_metadata(document_id)
        if not metadata["body_retained"]:
            raise BodyNotRetainedError("raw document body was not retained")
        body = self._body_path(str(metadata["content_sha256"])).read_bytes()
        document = RawDocument(
            source_id=str(metadata["source_id"]),
            canonical_url=str(metadata["canonical_url"]),
            content_type=str(metadata["content_type"]),
            content=body,
            first_seen_at=datetime.fromisoformat(str(metadata["first_seen_at"])),
            retrieved_at=datetime.fromisoformat(str(metadata["retrieved_at"])),
            available_at=datetime.fromisoformat(str(metadata["available_at"])),
            published_at=(
                datetime.fromisoformat(str(metadata["published_at"]))
                if metadata["published_at"] is not None
                else None
            ),
            etag=metadata["etag"],
            last_modified=metadata["last_modified"],
            encoding=metadata["encoding"],
            content_sha256=str(metadata["content_sha256"]),
        )
        if document.document_id != document_id:
            raise ValueError("raw document metadata does not match its content")
        return document

    def revisions(self, *, source_id: str, canonical_url: str) -> tuple[str, ...]:
        records: list[tuple[datetime, str]] = []
        if not self._records.exists():
            return ()
        for path in self._records.glob("*/*.json"):
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("source_id") == source_id and data.get("canonical_url") == canonical_url:
                records.append(
                    (datetime.fromisoformat(str(data["first_seen_at"])), str(data["document_id"]))
                )
        return tuple(document_id for _, document_id in sorted(records))
