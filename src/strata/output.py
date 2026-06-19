"""The Layer 4 output writer handed to every handler.

A handler receives a :class:`~strata.schema.StructuredBlob` and an
``OutputWriter``.  Calling :meth:`OutputWriter.emit` writes one typed record to
a named output table while automatically stamping it with lineage (the blob and
source it came from), so any gold-layer row can be traced back to its origin.
"""

from __future__ import annotations

from typing import Any

from .hashing import new_guid
from .models import OutputRecord
from .storage.base import Storage


class OutputWriter:
    """Collects typed records emitted by a handler into output tables."""

    def __init__(self, storage: Storage, *, blob_id: str, source_id: str):
        self._storage = storage
        self.blob_id = blob_id
        self.source_id = source_id
        self.count = 0

    def emit(self, table_name: str, record: dict[str, Any]) -> OutputRecord:
        """Write a single record to ``table_name`` and return it."""
        rec = OutputRecord(
            output_id=new_guid(),
            table_name=table_name,
            blob_id=self.blob_id,
            source_id=self.source_id,
            record=dict(record),
        )
        self._storage.add_output(rec)
        self.count += 1
        return rec

    def emit_many(self, table_name: str, records) -> int:
        """Write many records; return how many were written."""
        n = 0
        for record in records:
            self.emit(table_name, record)
            n += 1
        return n
