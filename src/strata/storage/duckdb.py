"""DuckDB implementation of the :class:`Storage` interface.

An *analytical* staging backend.  Where
:class:`~strata.storage.sqlite.SQLiteStorage` is the zero-dependency default,
DuckDB brings columnar, OLAP-friendly storage that scales to far larger output
tables while speaking standard SQL.

DuckDB enforces stricter typing than SQLite (which is dynamically typed), so the
schema below pins explicit column types -- ``BIGINT`` for byte sizes, ``INTEGER``
for the small counts, ``TEXT`` for ids/JSON payloads.  The shapes otherwise
mirror the SQLite schema one-for-one so the two backends are interchangeable.

Thread safety
-------------
A single DuckDB connection is used and **every** access is serialised through a
re-entrant lock.  DuckDB's transactional layer would otherwise surface
write-write conflicts when several threads touch one connection; serialising the
access turns the store into a safe target for
:meth:`~strata.pipeline.Pipeline.ingest_many`.  The lock is re-entrant so the
handful of methods that delegate to one another (e.g. :meth:`fetch_output`) do
not deadlock.

Install with ``pip install strata-etl[duckdb]``.
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict
from typing import Any, Optional

from ..exceptions import MissingDependencyError
from ..models import Blob, ColumnMeta, OutputRecord, Route, Signature, SourceFile, now_iso
from .base import Storage

# DuckDB is dynamically typed only at the value level; the column types here are
# enforced on write.  ``BIGINT`` keeps room for large files; the JSON payloads
# (``extra``, ``data``, ``columns``, ``record``) live in ``TEXT`` columns exactly
# as they do under SQLite, so the (de)serialisation logic is shared.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_files (
    source_id         TEXT PRIMARY KEY,
    path              TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    byte_size         BIGINT NOT NULL,
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


class DuckDBStorage(Storage):
    """A staging store backed by a single DuckDB database.

    Parameters
    ----------
    path:
        Database location.  Defaults to ``":memory:"`` for an ephemeral,
        in-process store (ideal for tests); pass a filename to persist on disk.
    """

    def __init__(self, path: str = ":memory:"):
        try:
            import duckdb
        except ImportError as exc:  # pragma: no cover - exercised only without duckdb
            raise MissingDependencyError(
                "the DuckDB backend requires duckdb; install with "
                "`pip install strata-etl[duckdb]`"
            ) from exc

        self.path = path
        # Re-entrant so methods that call one another (fetch_output ->
        # fetch_output_records) can hold the lock without deadlocking, while
        # cross-thread access is still fully serialised.
        self._lock = threading.RLock()
        self._conn = duckdb.connect(path)

    # -- helpers -----------------------------------------------------------
    def _query_all(self, sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
        """Run a SELECT and return rows as dicts keyed by column name."""
        with self._lock:
            cur = self._conn.execute(sql, params or [])
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _query_one(self, sql: str, params: Optional[list[Any]] = None) -> Optional[dict[str, Any]]:
        """Run a SELECT and return the first row as a dict (or ``None``)."""
        with self._lock:
            cur = self._conn.execute(sql, params or [])
            columns = [d[0] for d in cur.description]
            row = cur.fetchone()
            return dict(zip(columns, row)) if row is not None else None

    def _execute(self, sql: str, params: Optional[list[Any]] = None) -> None:
        """Run a write statement (commit is implicit under DuckDB autocommit)."""
        with self._lock:
            self._conn.execute(sql, params or [])
            self._conn.commit()

    def _affected(self, sql: str, params: Optional[list[Any]] = None) -> int:
        """Run a DELETE/UPDATE and return the affected row count.

        DuckDB does not populate ``cursor.rowcount`` reliably; instead it yields
        the affected count as the statement's single-row result.
        """
        with self._lock:
            row = self._conn.execute(sql, params or []).fetchone()
            self._conn.commit()
            return int(row[0]) if row else 0

    # -- lifecycle ---------------------------------------------------------
    def initialize(self) -> None:
        with self._lock:
            self._conn.execute(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- Layer 0: source file index ---------------------------------------
    def add_source_file(self, source: SourceFile) -> None:
        self._execute(
            "INSERT INTO source_files "
            "(source_id, path, original_filename, content_hash, byte_size, registered_at, extra) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                source.source_id,
                source.path,
                source.original_filename,
                source.content_hash,
                source.byte_size,
                source.registered_at,
                json.dumps(source.extra),
            ],
        )

    def get_source_file(self, source_id: str) -> Optional[SourceFile]:
        row = self._query_one("SELECT * FROM source_files WHERE source_id = ?", [source_id])
        return self._row_to_source(row) if row else None

    def find_sources_by_content_hash(self, content_hash: str) -> list[SourceFile]:
        rows = self._query_all(
            "SELECT * FROM source_files WHERE content_hash = ? ORDER BY rowid", [content_hash]
        )
        return [self._row_to_source(r) for r in rows]

    def list_source_files(self) -> list[SourceFile]:
        rows = self._query_all("SELECT * FROM source_files ORDER BY rowid")
        return [self._row_to_source(r) for r in rows]

    @staticmethod
    def _row_to_source(row: dict[str, Any]) -> SourceFile:
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
        self._execute(
            "INSERT INTO blobs "
            "(blob_id, source_id, sheet_name, sheet_index, n_rows, n_cols, data, "
            " signature_hash, created_at, processed_at, processed_by, process_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
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
            ],
        )

    def get_blob(self, blob_id: str) -> Optional[Blob]:
        row = self._query_one("SELECT * FROM blobs WHERE blob_id = ?", [blob_id])
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
        rows = self._query_all(f"SELECT * FROM blobs {where} ORDER BY rowid", params)
        return [self._row_to_blob(r) for r in rows]

    def set_blob_signature(self, blob_id: str, signature_hash: str) -> None:
        self._execute(
            "UPDATE blobs SET signature_hash = ? WHERE blob_id = ?", [signature_hash, blob_id]
        )

    def mark_blob_processed(
        self,
        blob_id: str,
        *,
        status: str,
        handler_name: Optional[str] = None,
        processed_at: Optional[str] = None,
    ) -> None:
        self._execute(
            "UPDATE blobs SET process_status = ?, processed_by = ?, processed_at = ? "
            "WHERE blob_id = ?",
            [status, handler_name, processed_at or now_iso(), blob_id],
        )

    @staticmethod
    def _row_to_blob(row: dict[str, Any]) -> Blob:
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
        self._execute(
            "INSERT INTO signatures "
            "(signature_hash, header, n_columns, columns, first_seen_at, sample_blob_id) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(signature_hash) DO UPDATE SET "
            "  columns = excluded.columns, sample_blob_id = excluded.sample_blob_id",
            [
                signature.signature_hash,
                json.dumps(signature.header),
                signature.n_columns,
                _columns_to_json(signature.columns),
                signature.first_seen_at,
                signature.sample_blob_id,
            ],
        )

    def get_signature(self, signature_hash: str) -> Optional[Signature]:
        row = self._query_one(
            "SELECT * FROM signatures WHERE signature_hash = ?", [signature_hash]
        )
        return self._row_to_signature(row) if row else None

    def list_signatures(self) -> list[Signature]:
        rows = self._query_all("SELECT * FROM signatures ORDER BY rowid")
        return [self._row_to_signature(r) for r in rows]

    @staticmethod
    def _row_to_signature(row: dict[str, Any]) -> Signature:
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
        self._execute(
            "INSERT INTO routes (signature_hash, handler_name, created_at, note) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(signature_hash) DO UPDATE SET "
            "  handler_name = excluded.handler_name, note = excluded.note",
            [route.signature_hash, route.handler_name, route.created_at, route.note],
        )

    def get_route(self, signature_hash: str) -> Optional[Route]:
        row = self._query_one("SELECT * FROM routes WHERE signature_hash = ?", [signature_hash])
        if not row:
            return None
        return Route(
            signature_hash=row["signature_hash"],
            handler_name=row["handler_name"],
            created_at=row["created_at"],
            note=row["note"],
        )

    def list_routes(self) -> list[Route]:
        rows = self._query_all("SELECT * FROM routes ORDER BY rowid")
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
        return self._affected(
            "DELETE FROM routes WHERE signature_hash = ?", [signature_hash]
        ) > 0

    # -- Layer 4: outputs --------------------------------------------------
    def add_output(self, record: OutputRecord) -> None:
        self._execute(
            "INSERT INTO outputs (output_id, table_name, blob_id, source_id, record, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                record.output_id,
                record.table_name,
                record.blob_id,
                record.source_id,
                json.dumps(record.record),
                record.created_at,
            ],
        )

    def fetch_output(
        self,
        table_name: str,
        *,
        source_id: Optional[str] = None,
        blob_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        return [
            r.record
            for r in self.fetch_output_records(table_name, source_id=source_id, blob_id=blob_id)
        ]

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
        rows = self._query_all(f"SELECT * FROM outputs WHERE {where} ORDER BY rowid", params)
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
        return self._affected("DELETE FROM outputs WHERE blob_id = ?", [blob_id])

    def list_output_tables(self) -> list[str]:
        rows = self._query_all(
            "SELECT DISTINCT table_name FROM outputs ORDER BY table_name"
        )
        return [r["table_name"] for r in rows]

    def count_output(self, table_name: Optional[str] = None) -> int:
        if table_name is None:
            row = self._query_one("SELECT COUNT(*) AS n FROM outputs")
        else:
            row = self._query_one(
                "SELECT COUNT(*) AS n FROM outputs WHERE table_name = ?", [table_name]
            )
        return int(row["n"]) if row else 0
