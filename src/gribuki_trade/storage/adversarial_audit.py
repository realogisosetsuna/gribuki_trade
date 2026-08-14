"""对抗 LLM 双轨结果的 SQLite 哈希链审计存储。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast


class AdversarialAuditIntegrityError(RuntimeError):
    """审计链内容、顺序或摘要不一致。"""


class SQLiteAdversarialAuditStore:
    """以 ``BEGIN IMMEDIATE`` 串行追加、提交后再返回的跨进程审计账本。"""

    def __init__(self, path: Path) -> None:
        resolved = path.resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            resolved,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA busy_timeout=30000")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS adversarial_audit_records (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id TEXT NOT NULL,
                previous_sha256 TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL
            )
            """
        )

    def append(self, document: Mapping[str, object]) -> str:
        """原子追加完整文档并返回绑定前序记录的哈希。"""

        analysis_id = document.get("analysis_id")
        if not isinstance(analysis_id, str) or not analysis_id.strip():
            raise ValueError("audit document requires a normalized analysis_id")
        payload_json = _canonical_json(document)
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        connection = self._connection
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT record_sha256
                FROM adversarial_audit_records
                ORDER BY sequence DESC
                LIMIT 1
                """
            ).fetchone()
            previous = "0" * 64 if row is None else str(row["record_sha256"])
            record_sha256 = hashlib.sha256(
                f"{previous}|{payload_sha256}".encode()
            ).hexdigest()
            connection.execute(
                """
                INSERT INTO adversarial_audit_records (
                    analysis_id,
                    previous_sha256,
                    payload_sha256,
                    record_sha256,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    analysis_id,
                    previous,
                    payload_sha256,
                    record_sha256,
                    payload_json,
                ),
            )
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        return record_sha256

    def read_all(self) -> tuple[dict[str, object], ...]:
        """按追加顺序读取并验证整条链，任何破坏都拒绝返回部分结果。"""

        rows = self._connection.execute(
            """
            SELECT previous_sha256, payload_sha256, record_sha256, payload_json
            FROM adversarial_audit_records
            ORDER BY sequence ASC
            """
        ).fetchall()
        previous = "0" * 64
        documents: list[dict[str, object]] = []
        for row in rows:
            payload_json = str(row["payload_json"])
            payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            expected_record = hashlib.sha256(
                f"{previous}|{payload_sha256}".encode()
            ).hexdigest()
            if (
                str(row["previous_sha256"]) != previous
                or str(row["payload_sha256"]) != payload_sha256
                or str(row["record_sha256"]) != expected_record
            ):
                raise AdversarialAuditIntegrityError("adversarial audit hash chain mismatch")
            decoded = json.loads(payload_json)
            if not isinstance(decoded, dict):
                raise AdversarialAuditIntegrityError("adversarial audit payload is not an object")
            documents.append(cast(dict[str, object], decoded))
            previous = expected_record
        return tuple(documents)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteAdversarialAuditStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _canonical_json(document: Mapping[str, object]) -> str:
    try:
        return json.dumps(
            cast(dict[str, Any], dict(document)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("audit document must contain strict JSON values") from exc
