"""The :class:`Pipeline` -- the orchestrator that wires the five layers together.

Bottom to top, exactly as the two write-ups describe:

    L0  index      register the file (path, name, timestamp, GUID)
    L1  raw        explode each sheet into a generic 2D blob (GUID)
    L2  signature  MD5 the header row; interpret schema metadata
    L3  routing    look up signature -> handler; dynamically dispatch
    L4  semantic   handler emits typed records into output tables

You add structure as you climb: the bottom is maximally flexible, the top makes
the strongest guarantees about the data it produces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

from .hashing import content_hash, header_signature, new_guid
from .models import Blob, Route, Signature, SourceFile
from .output import OutputWriter
from .readers import read_file
from .registry import HandlerFn, HandlerRegistry
from .schema import StructuredBlob
from .storage.base import Storage

PathLike = Union[str, Path]


@dataclass
class IngestResult:
    """Summary of a single :meth:`Pipeline.ingest` call."""

    source: Optional[SourceFile]
    blobs: list[Blob] = field(default_factory=list)
    new_signatures: list[str] = field(default_factory=list)
    skipped_duplicate: bool = False

    @property
    def n_blobs(self) -> int:
        return len(self.blobs)


@dataclass
class ProcessResult:
    """Summary of a :meth:`Pipeline.process` call."""

    processed: int = 0
    records_emitted: int = 0
    no_route: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)
    by_handler: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors


class Pipeline:
    """High-level entry point spanning all five layers.

    Parameters
    ----------
    storage:
        The staging backend (e.g. :class:`~strata.storage.sqlite.SQLiteStorage`).
    registry:
        A handler registry; a fresh one is created if omitted.
    header_row:
        Index of the header row used for signatures and structuring (line #1).
    hash_lower / hash_strip:
        Normalisation applied before hashing the header. Must stay consistent
        across ingests for signatures to line up over time.
    skip_duplicate_files:
        If true (default), re-ingesting a byte-identical file is a no-op.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        registry: Optional[HandlerRegistry] = None,
        header_row: int = 0,
        hash_lower: bool = False,
        hash_strip: bool = True,
        skip_duplicate_files: bool = True,
    ):
        self.storage = storage
        self.registry = registry or HandlerRegistry()
        self.header_row = header_row
        self.hash_lower = hash_lower
        self.hash_strip = hash_strip
        self.skip_duplicate_files = skip_duplicate_files
        self.storage.initialize()

    # ------------------------------------------------------------------ #
    # Layer 0 + 1: ingest
    # ------------------------------------------------------------------ #
    def ingest(
        self,
        path: PathLike,
        *,
        extra: Optional[dict[str, Any]] = None,
        skip_duplicate: Optional[bool] = None,
        compute_signatures: bool = True,
    ) -> IngestResult:
        """Index a file (L0) and explode its sheets into raw blobs (L1).

        By default also computes each blob's signature (L2) so the file is ready
        to route and process.
        """
        p = Path(path)
        raw = p.read_bytes()
        chash = content_hash(raw)

        skip = self.skip_duplicate_files if skip_duplicate is None else skip_duplicate
        if skip:
            existing = self.storage.find_sources_by_content_hash(chash)
            if existing:
                return IngestResult(source=existing[0], skipped_duplicate=True)

        source = SourceFile(
            source_id=new_guid(),
            path=str(p.resolve()),
            original_filename=p.name,
            content_hash=chash,
            byte_size=len(raw),
            extra=extra or {},
        )
        self.storage.add_source_file(source)

        blobs: list[Blob] = []
        new_signatures: list[str] = []
        for sheet in read_file(p):
            blob = Blob(
                blob_id=new_guid(),
                source_id=source.source_id,
                sheet_name=sheet.name,
                sheet_index=sheet.index,
                n_rows=sheet.n_rows,
                n_cols=sheet.n_cols,
                data=sheet.rows,
            )
            self.storage.add_blob(blob)
            blobs.append(blob)
            if compute_signatures:
                _signature, is_new = self._signaturize_blob(blob)
                if is_new:
                    new_signatures.append(_signature.signature_hash)

        return IngestResult(source=source, blobs=blobs, new_signatures=new_signatures)

    # ------------------------------------------------------------------ #
    # Layer 2: signatures
    # ------------------------------------------------------------------ #
    def signature_for(self, header_cells) -> str:
        """Compute the signature hash for a header row using this pipeline's rules."""
        return header_signature(
            header_cells, lower=self.hash_lower, strip=self.hash_strip
        )

    def signaturize(self) -> int:
        """Compute signatures for every blob that lacks one. Returns the count."""
        blobs = self.storage.list_blobs(without_signature=True)
        for blob in blobs:
            self._signaturize_blob(blob)
        return len(blobs)

    def _signaturize_blob(self, blob: Blob) -> tuple[Signature, bool]:
        header_cells = blob.header(self.header_row)
        sig_hash = self.signature_for(header_cells)
        is_new = self.storage.get_signature(sig_hash) is None

        structured = StructuredBlob(blob, header_row=self.header_row)
        signature = Signature(
            signature_hash=sig_hash,
            header=structured.columns_names,
            n_columns=structured.n_columns,
            columns=structured.schema,
            sample_blob_id=blob.blob_id,
        )
        self.storage.upsert_signature(signature)
        self.storage.set_blob_signature(blob.blob_id, sig_hash)
        blob.signature_hash = sig_hash
        return signature, is_new

    # ------------------------------------------------------------------ #
    # Layer 3: routing + handler registration
    # ------------------------------------------------------------------ #
    def handler(self, name: str):
        """Decorator that registers a handler under ``name``."""
        return self.registry.register(name)

    def register_handler(self, name: str, fn: HandlerFn) -> HandlerFn:
        """Register a handler callable under ``name``."""
        return self.registry.register(name, fn)

    def route(self, signature_hash: str, handler_name: str, *, note: Optional[str] = None) -> None:
        """Map a signature hash to a handler name (the lookup table row)."""
        self.storage.set_route(
            Route(signature_hash=signature_hash, handler_name=handler_name, note=note)
        )

    def route_header(
        self, header_cells, handler_name: str, *, note: Optional[str] = None
    ) -> str:
        """Route by giving the column names directly; returns the signature hash."""
        sig = self.signature_for(header_cells)
        self.route(sig, handler_name, note=note)
        return sig

    def unroute(self, signature_hash: str) -> bool:
        """Remove a route. Returns whether one existed."""
        return self.storage.delete_route(signature_hash)

    def unrouted_signatures(self) -> list[Signature]:
        """Signatures that have been seen but have no route yet.

        Operationally this answers: "what new spreadsheet shapes have shown up
        that I haven't taught the system to handle?"
        """
        return [
            s
            for s in self.storage.list_signatures()
            if self.storage.get_route(s.signature_hash) is None
        ]

    # ------------------------------------------------------------------ #
    # Layer 3 dispatch + Layer 4: process
    # ------------------------------------------------------------------ #
    def process(
        self,
        *,
        reprocess: bool = False,
        blob_id: Optional[str] = None,
    ) -> ProcessResult:
        """Run handlers over blobs.

        By default processes only ``pending`` blobs. Pass ``reprocess=True`` to
        re-run every blob (outputs for a blob are cleared before it re-runs, so
        processing is idempotent), or ``blob_id`` to target a single blob.
        """
        result = ProcessResult()
        if blob_id is not None:
            one = self.storage.get_blob(blob_id)
            blobs = [one] if one is not None else []
        elif reprocess:
            blobs = self.storage.list_blobs()
        else:
            blobs = self.storage.list_blobs(status="pending")

        for blob in blobs:
            if blob.signature_hash is None:
                self._signaturize_blob(blob)
            self._process_blob(blob, result)
        return result

    def _process_blob(self, blob: Blob, result: ProcessResult) -> None:
        route = self.storage.get_route(blob.signature_hash) if blob.signature_hash else None
        if route is None:
            self.storage.mark_blob_processed(blob.blob_id, status="no_route")
            result.no_route.append(blob.blob_id)
            return

        try:
            handler = self.registry.get(route.handler_name)
        except Exception as exc:  # unregistered handler name
            self.storage.mark_blob_processed(
                blob.blob_id, status="error", handler_name=route.handler_name
            )
            result.errors.append((blob.blob_id, str(exc)))
            return

        # Clear any prior output for this blob so (re)processing is idempotent.
        self.storage.delete_outputs_for_blob(blob.blob_id)
        writer = OutputWriter(self.storage, blob_id=blob.blob_id, source_id=blob.source_id)
        structured = StructuredBlob(blob, header_row=self.header_row)
        try:
            handler(structured, writer)
        except Exception as exc:
            self.storage.mark_blob_processed(
                blob.blob_id, status="error", handler_name=route.handler_name
            )
            result.errors.append((blob.blob_id, f"{type(exc).__name__}: {exc}"))
            return

        self.storage.mark_blob_processed(
            blob.blob_id, status="processed", handler_name=route.handler_name
        )
        result.processed += 1
        result.records_emitted += writer.count
        result.by_handler[route.handler_name] = result.by_handler.get(route.handler_name, 0) + 1

    # ------------------------------------------------------------------ #
    # Convenience accessors
    # ------------------------------------------------------------------ #
    def structured(self, blob: Union[Blob, str]) -> StructuredBlob:
        """Return a :class:`StructuredBlob` view of a blob (or blob id)."""
        if isinstance(blob, str):
            found = self.storage.get_blob(blob)
            if found is None:
                raise KeyError(f"no blob with id {blob!r}")
            blob = found
        return StructuredBlob(blob, header_row=self.header_row)

    def output(self, table_name: str, **filters) -> list[dict[str, Any]]:
        """Fetch records from an output table (Layer 4)."""
        return self.storage.fetch_output(table_name, **filters)

    def sources(self) -> list[SourceFile]:
        return self.storage.list_source_files()

    def blobs(self, **filters) -> list[Blob]:
        return self.storage.list_blobs(**filters)

    def signatures(self) -> list[Signature]:
        return self.storage.list_signatures()

    def routes(self) -> list[Route]:
        return self.storage.list_routes()

    def summary(self) -> dict[str, int]:
        """A quick counts snapshot across the layers, handy for dashboards/CLI."""
        blobs = self.storage.list_blobs()
        status_counts: dict[str, int] = {}
        for b in blobs:
            status_counts[b.process_status] = status_counts.get(b.process_status, 0) + 1
        return {
            "sources": len(self.storage.list_source_files()),
            "blobs": len(blobs),
            "signatures": len(self.storage.list_signatures()),
            "routes": len(self.storage.list_routes()),
            "outputs": self.storage.count_output(),
            "pending": status_counts.get("pending", 0),
            "processed": status_counts.get("processed", 0),
            "no_route": status_counts.get("no_route", 0),
            "error": status_counts.get("error", 0),
        }
