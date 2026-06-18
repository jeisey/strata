"""The pluggable storage backend interface.

Every layer of the framework reads and writes through a :class:`Storage`
object -- the "staging database".  The bundled
:class:`~strata.storage.sqlite.SQLiteStorage` is a zero-dependency
implementation; swap in Postgres, DuckDB, etc. by implementing this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from ..models import Blob, OutputRecord, Route, Signature, SourceFile


class Storage(ABC):
    """Abstract staging store spanning all five layers."""

    # -- lifecycle ---------------------------------------------------------
    @abstractmethod
    def initialize(self) -> None:
        """Create tables/indexes if they do not already exist (idempotent)."""

    def close(self) -> None:  # noqa: B027 - optional hook; backends may override
        """Release any resources held by the backend."""

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- Layer 0: source file index ---------------------------------------
    @abstractmethod
    def add_source_file(self, source: SourceFile) -> None: ...

    @abstractmethod
    def get_source_file(self, source_id: str) -> Optional[SourceFile]: ...

    @abstractmethod
    def find_sources_by_content_hash(self, content_hash: str) -> list[SourceFile]: ...

    @abstractmethod
    def list_source_files(self) -> list[SourceFile]: ...

    # -- Layer 1: raw blobs ------------------------------------------------
    @abstractmethod
    def add_blob(self, blob: Blob) -> None: ...

    @abstractmethod
    def get_blob(self, blob_id: str) -> Optional[Blob]: ...

    @abstractmethod
    def list_blobs(
        self,
        *,
        source_id: Optional[str] = None,
        signature_hash: Optional[str] = None,
        without_signature: bool = False,
        status: Optional[str] = None,
    ) -> list[Blob]: ...

    @abstractmethod
    def set_blob_signature(self, blob_id: str, signature_hash: str) -> None: ...

    @abstractmethod
    def mark_blob_processed(
        self,
        blob_id: str,
        *,
        status: str,
        handler_name: Optional[str] = None,
        processed_at: Optional[str] = None,
    ) -> None: ...

    # -- Layer 2: signatures ----------------------------------------------
    @abstractmethod
    def upsert_signature(self, signature: Signature) -> None: ...

    @abstractmethod
    def get_signature(self, signature_hash: str) -> Optional[Signature]: ...

    @abstractmethod
    def list_signatures(self) -> list[Signature]: ...

    # -- Layer 3: routes ---------------------------------------------------
    @abstractmethod
    def set_route(self, route: Route) -> None: ...

    @abstractmethod
    def get_route(self, signature_hash: str) -> Optional[Route]: ...

    @abstractmethod
    def list_routes(self) -> list[Route]: ...

    @abstractmethod
    def delete_route(self, signature_hash: str) -> bool: ...

    # -- Layer 4: outputs --------------------------------------------------
    @abstractmethod
    def add_output(self, record: OutputRecord) -> None: ...

    @abstractmethod
    def fetch_output(
        self,
        table_name: str,
        *,
        source_id: Optional[str] = None,
        blob_id: Optional[str] = None,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def fetch_output_records(
        self,
        table_name: str,
        *,
        source_id: Optional[str] = None,
        blob_id: Optional[str] = None,
    ) -> list[OutputRecord]: ...

    @abstractmethod
    def delete_outputs_for_blob(self, blob_id: str) -> int: ...

    @abstractmethod
    def list_output_tables(self) -> list[str]: ...

    @abstractmethod
    def count_output(self, table_name: Optional[str] = None) -> int: ...
