"""Core data models shared across the layers.

These are plain dataclasses -- the storage backend is responsible for
persisting them.  They form the vocabulary of the framework:

    SourceFile  -> Layer 0 (Index): a registered file on disk
    Blob        -> Layer 1 (Raw):   one sheet's generic 2D data
    Signature   -> Layer 2:         the MD5 fingerprint of a header row
    ColumnMeta  -> Layer 2:         interpreted schema for one column
    Route       -> Layer 3:         signature -> handler lookup row
    OutputRecord-> Layer 4:         a typed record emitted by a handler
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def now_iso() -> str:
    """Current UTC time as an ISO-8601 string (used for all timestamps)."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SourceFile:
    """Layer 0 -- the index entry for an ingested file.

    The lowest-structure layer: we simply record that a file existed, where it
    came from, and when we saw it.  ``source_id`` is the GUID every downstream
    record points back to.
    """

    source_id: str
    path: str
    original_filename: str
    content_hash: str
    byte_size: int
    registered_at: str = field(default_factory=now_iso)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Blob:
    """Layer 1 -- one sheet's worth of generic 2D string data.

    A workbook explodes into one blob per sheet; a CSV produces a single blob.
    ``data`` is a list of rows, each row a list of cells coerced to ``str`` (or
    ``None`` for blanks).  This is deliberately the *least* structured form:
    every cell is text, no types are imposed yet.
    """

    blob_id: str
    source_id: str
    sheet_name: str
    sheet_index: int
    n_rows: int
    n_cols: int
    data: list[list[Optional[str]]] = field(default_factory=list, repr=False)
    signature_hash: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    processed_at: Optional[str] = None
    processed_by: Optional[str] = None
    process_status: str = "pending"  # pending | processed | no_route | error

    def header(self, header_row: int = 0) -> list[Optional[str]]:
        """Return the header row (line #1 by default), or ``[]`` if empty."""
        if 0 <= header_row < len(self.data):
            return self.data[header_row]
        return []


@dataclass
class ColumnMeta:
    """Layer 2 -- interpreted metadata for a single column of a blob."""

    name: str
    index: int
    inferred_type: str  # one of: empty, bool, int, float, date, datetime, str
    nullable: bool
    n_nonnull: int
    sample: Optional[str] = None


@dataclass
class Signature:
    """Layer 2 -- the MD5 fingerprint of a header row plus its schema.

    Blobs that share a header share a signature.  This is what makes the
    framework robust to files that gain or lose columns over time: a new column
    yields a new signature, which you can route independently.
    """

    signature_hash: str
    header: list[str]
    n_columns: int
    columns: list[ColumnMeta] = field(default_factory=list)
    first_seen_at: str = field(default_factory=now_iso)
    sample_blob_id: Optional[str] = None


@dataclass
class Route:
    """Layer 3 -- a single ``signature -> handler`` lookup row.

    The analogue of the SQL "what stored proc should I run for this hashed
    signature?" table.  ``handler_name`` is resolved against the handler
    registry at processing time (dynamic dispatch).
    """

    signature_hash: str
    handler_name: str
    created_at: str = field(default_factory=now_iso)
    note: Optional[str] = None


@dataclass
class OutputRecord:
    """Layer 4 -- one typed record produced by a handler.

    Carries lineage (``blob_id`` and ``source_id``) so any gold-layer row can
    be traced all the way back to the spreadsheet cell it came from.
    """

    output_id: str
    table_name: str
    blob_id: str
    source_id: str
    record: dict[str, Any]
    created_at: str = field(default_factory=now_iso)
