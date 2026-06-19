"""SQLite implementation of the :class:`Storage` interface.

This is the default "staging database".  It needs nothing beyond the Python
standard library and works equally well as an on-disk file or an in-memory
store (``":memory:"``) for tests.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from typing import Any, Optional

from ..models import Blob, ColumnMeta, OutputRecord, Route, Signature, SourceFile, now_iso
from .base import Storage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_files (
    source_id         TEXT PRIMARY KEY,
    path              TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    byte_size         INTEGER NOT NULL,
    registered_at     TEXT NOT NULL,
    extra             TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_source_files_hash ON source_files(content_hash);

CREATE TABLE IF NOT EXISTS blobs (
    blob_id        TEXT PRIMARY KEY,
    source_id      TEXT NOT NULL REFERENCES source_files(source_id),
    sheet_name     TEXT NOT NULL,
    sheet_index    INTEGER NOT NULL,
    n_rows         INTEGER NOT NULL,
    n_cols         INTEGER NOT NULL,
    data           TEXT NOT NULL,
    signature_hash TEXT,
    created_at     TEXT NOT NULL,
    processed_at   TEXT,
    processed_by   TEXT,
    process_status TEXT NOT NULL DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS ix_blobs_source ON blobs(source_id);
CREATE INDEX IF NOT EXISTS ix_blobs_signature ON blobs(signature_hash);
CREATE INDEX IF NOT EXISTS ix_blobs_status ON blobs(process_status);

CREATE TABLE IF NOT EXISTS signatures (
    signature_hash TEXT PRIMARY KEY,
    header         TEXT NOT NULL,
    n_columns      INTEGER NOT NULL,
    columns        TEXT NOT NULL DEFAULT '[]',
    first_seen_at  TEXT NOT NULL,
    sample_blob_id TEXT
);

CREATE TABLE IF NOT EXISTS routes (
    signature_hash TEXT PRIMARY KEY,
    handler_name   TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    note           TEXT
);

CREATE TABLE IF NOT EXISTS outputs (
    output_id   TEXT PRIMARY KEY,
    table_name  TEXT NOT NULL,
    blob_id     TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    record      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outputs_table ON outputs(table_name);
CREATE INDEX IF NOT EXISTS ix_outputs_source ON outputs(source_id);
CREATE INDEX IF NOT EXISTS ix_outputs_blob ON outputs(blob_id);
"""


def _columns_to_json(columns: list[ColumnMeta]) -> str:
    return json.dumps([asdict(c) for c in columns])


def _columns_from_json(raw: str) -> list[ColumnMeta]:
    return [ColumnMeta(**c) for c in json.loads(raw or "[]")]


class SQLiteStorage(Storage):
    """A staging store backed by a single SQLite database."""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")

    # -- lifecycle ---------------------------------------------------------
    def initialize(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- Layer 0: source file index ---------------------------------------
    def add_source_file(self, source: SourceFile) -> None:
        self._conn.execute(
            "INSERT INTO source_files "
            "(source_id, path, original_filename, content_hash, byte_size, registered_at, extra) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                source.source_id,
                source.path,
                source.original_filename,
                source.content_hash,
                source.byte_size,
                source.registered_at,
                json.dumps(source.extra),
            ),
        )
        self._conn.commit()

    def get_source_file(self, source_id: str) -> Optional[SourceFile]:
        row = self._conn.execute(
            "SELECT * FROM source_files WHERE source_id = ?", (source_id,)
        ).fetchone()
        return self._row_to_source(row) if row else None

    def find_sources_by_content_hash(self, content_hash: str) -> list[SourceFile]:
        rows = self._conn.execute(
            "SELECT * FROM source_files WHERE content_hash = ? ORDER BY rowid", (content_hash,)
        ).fetchall()
        return [self._row_to_source(r) for r in rows]

    def list_source_files(self) -> list[SourceFile]:
        rows = self._conn.execute("SELECT * FROM source_files ORDER BY rowid").fetchall()
        return [self._row_to_source(r) for r in rows]

    @staticmethod
    def _row_to_source(row: sqlite3.Row) -> SourceFile:
        return SourceFile(
            source_id=row["source_id"],
            path=row["path"],
            original_filename=row["original_filename"],
            content_hash=row["content_hash"],
            byte_size=row["byte_size"],
            registered_at=row["registered_at"],
            extra=json.loads(row["extra"]),
        )

    # -- Layer 1: raw blobs ------------------------------------------------
    def add_blob(self, blob: Blob) -> None:
        self._conn.execute(
            "INSERT INTO blobs "
            "(blob_id, source_id, sheet_name, sheet_index, n_rows, n_cols, data, "
            " signature_hash, created_at, processed_at, processed_by, process_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                blob.blob_id,
                blob.source_id,
                blob.sheet_name,
                blob.sheet_index,
                blob.n_rows,
                blob.n_cols,
                json.dumps(blob.data),
                blob.signature_hash,
                blob.created_at,
                blob.processed_at,
                blob.processed_by,
                blob.process_status,
            ),
        )
        self._conn.commit()

    def get_blob(self, blob_id: str) -> Optional[Blob]:
        row = self._conn.execute("SELECT * FROM blobs WHERE blob_id = ?", (blob_id,)).fetchone()
        return self._row_to_blob(row) if row else None

    def list_blobs(
        self,
        *,
        source_id: Optional[str] = None,
        signature_hash: Optional[str] = None,
        without_signature: bool = False,
        status: Optional[str] = None,
    ) -> list[Blob]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_id is not None:
            clauses.append("source_id = ?")
            params.append(source_id)
        if signature_hash is not None:
            clauses.append("signature_hash = ?")
            params.append(signature_hash)
        if without_signature:
            clauses.append("signature_hash IS NULL")
        if status is not None:
            clauses.append("process_status = ?")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM blobs {where} ORDER BY rowid", params
        ).fetchall()
        return [self._row_to_blob(r) for r in rows]

    def set_blob_signature(self, blob_id: str, signature_hash: str) -> None:
        self._conn.execute(
            "UPDATE blobs SET signature_hash = ? WHERE blob_id = ?", (signature_hash, blob_id)
        )
        self._conn.commit()

    def mark_blob_processed(
        self,
        blob_id: str,
        *,
        status: str,
        handler_name: Optional[str] = None,
        processed_at: Optional[str] = None,
    ) -> None:
        self._conn.execute(
            "UPDATE blobs SET process_status = ?, processed_by = ?, processed_at = ? "
            "WHERE blob_id = ?",
            (status, handler_name, processed_at or now_iso(), blob_id),
        )
        self._conn.commit()

    @staticmethod
    def _row_to_blob(row: sqlite3.Row) -> Blob:
        return Blob(
            blob_id=row["blob_id"],
            source_id=row["source_id"],
            sheet_name=row["sheet_name"],
            sheet_index=row["sheet_index"],
            n_rows=row["n_rows"],
            n_cols=row["n_cols"],
            data=json.loads(row["data"]),
            signature_hash=row["signature_hash"],
            created_at=row["created_at"],
            processed_at=row["processed_at"],
            processed_by=row["processed_by"],
            process_status=row["process_status"],
        )

    # -- Layer 2: signatures ----------------------------------------------
    def upsert_signature(self, signature: Signature) -> None:
        self._conn.execute(
            "INSERT INTO signatures "
            "(signature_hash, header, n_columns, columns, first_seen_at, sample_blob_id) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(signature_hash) DO UPDATE SET "
            "  columns = excluded.columns, sample_blob_id = excluded.sample_blob_id",
            (
                signature.signature_hash,
                json.dumps(signature.header),
                signature.n_columns,
                _columns_to_json(signature.columns),
                signature.first_seen_at,
                signature.sample_blob_id,
            ),
        )
        self._conn.commit()

    def get_signature(self, signature_hash: str) -> Optional[Signature]:
        row = self._conn.execute(
            "SELECT * FROM signatures WHERE signature_hash = ?", (signature_hash,)
        ).fetchone()
        return self._row_to_signature(row) if row else None

    def list_signatures(self) -> list[Signature]:
        rows = self._conn.execute("SELECT * FROM signatures ORDER BY rowid").fetchall()
        return [self._row_to_signature(r) for r in rows]

    @staticmethod
    def _row_to_signature(row: sqlite3.Row) -> Signature:
        return Signature(
            signature_hash=row["signature_hash"],
            header=json.loads(row["header"]),
            n_columns=row["n_columns"],
            columns=_columns_from_json(row["columns"]),
            first_seen_at=row["first_seen_at"],
            sample_blob_id=row["sample_blob_id"],
        )

    # -- Layer 3: routes ---------------------------------------------------
    def set_route(self, route: Route) -> None:
        self._conn.execute(
            "INSERT INTO routes (signature_hash, handler_name, created_at, note) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(signature_hash) DO UPDATE SET "
            "  handler_name = excluded.handler_name, note = excluded.note",
            (route.signature_hash, route.handler_name, route.created_at, route.note),
        )
        self._conn.commit()

    def get_route(self, signature_hash: str) -> Optional[Route]:
        row = self._conn.execute(
            "SELECT * FROM routes WHERE signature_hash = ?", (signature_hash,)
        ).fetchone()
        if not row:
            return None
        return Route(
            signature_hash=row["signature_hash"],
            handler_name=row["handler_name"],
            created_at=row["created_at"],
            note=row["note"],
        )

    def list_routes(self) -> list[Route]:
        rows = self._conn.execute("SELECT * FROM routes ORDER BY rowid").fetchall()
        return [
            Route(
                signature_hash=r["signature_hash"],
                handler_name=r["handler_name"],
                created_at=r["created_at"],
                note=r["note"],
            )
            for r in rows
        ]

    def delete_route(self, signature_hash: str) -> bool:
        cur = self._conn.execute(
            "DELETE FROM routes WHERE signature_hash = ?", (signature_hash,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- Layer 4: outputs --------------------------------------------------
    def add_output(self, record: OutputRecord) -> None:
        self._conn.execute(
            "INSERT INTO outputs (output_id, table_name, blob_id, source_id, record, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                record.output_id,
                record.table_name,
                record.blob_id,
                record.source_id,
                json.dumps(record.record),
                record.created_at,
            ),
        )
        self._conn.commit()

    def fetch_output(
        self,
        table_name: str,
        *,
        source_id: Optional[str] = None,
        blob_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        return [r.record for r in self.fetch_output_records(
            table_name, source_id=source_id, blob_id=blob_id
        )]

    def fetch_output_records(
        self,
        table_name: str,
        *,
        source_id: Optional[str] = None,
        blob_id: Optional[str] = None,
    ) -> list[OutputRecord]:
        clauses = ["table_name = ?"]
        params: list[Any] = [table_name]
        if source_id is not None:
            clauses.append("source_id = ?")
            params.append(source_id)
        if blob_id is not None:
            clauses.append("blob_id = ?")
            params.append(blob_id)
        where = " AND ".join(clauses)
        rows = self._conn.execute(
            f"SELECT * FROM outputs WHERE {where} ORDER BY rowid", params
        ).fetchall()
        return [
            OutputRecord(
                output_id=r["output_id"],
                table_name=r["table_name"],
                blob_id=r["blob_id"],
                source_id=r["source_id"],
                record=json.loads(r["record"]),
                created_at=r["created_at"],
            )
            for r in rows
        ]

    def delete_outputs_for_blob(self, blob_id: str) -> int:
        cur = self._conn.execute("DELETE FROM outputs WHERE blob_id = ?", (blob_id,))
        self._conn.commit()
        return cur.rowcount

    def list_output_tables(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT DISTINCT table_name FROM outputs ORDER BY table_name"
        ).fetchall()
        return [r["table_name"] for r in rows]

    def count_output(self, table_name: Optional[str] = None) -> int:
        if table_name is None:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM outputs").fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM outputs WHERE table_name = ?", (table_name,)
            ).fetchone()
        return int(row["n"])
