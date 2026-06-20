"""strata -- a layered, signature-routed ETL framework for messy spreadsheets.

Build up from the bottom (least structure, most flexible) to the top (strongest
guarantees, least flexible):

    L0 index -> L1 raw blobs -> L2 signature -> L3 routing -> L4 semantic output

Quickstart
----------
>>> from strata import Pipeline, SQLiteStorage
>>> pipe = Pipeline(SQLiteStorage("staging.db"))
>>> pipe.ingest("prices_jan.csv")                      # L0 + L1 + L2
>>> @pipe.handler("prices_v1")                          # L3 (register)
... def prices_v1(blob, out):                           # L4 (extract)
...     for row in blob.records():
...         out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"])})
>>> pipe.route_header(["SKU", "Price"], "prices_v1")    # L3 (lookup row)
>>> pipe.process()                                      # L3 dispatch + L4
>>> pipe.output("prices")
"""

from __future__ import annotations

from .exceptions import (
    MissingDependencyError,
    NoHandlerError,
    NoRouteError,
    ReaderError,
    StorageError,
    StrataError,
    UnsupportedFileError,
)
from .hashing import content_hash, header_signature, new_guid, normalize_header
from .models import (
    Blob,
    ColumnMeta,
    OutputRecord,
    Route,
    Signature,
    SourceFile,
)
from .output import OutputWriter
from .pipeline import IngestResult, Pipeline, ProcessResult
from .readers import Sheet, read_file, register_reader, supported_extensions
from .registry import HandlerRegistry
from .schema import StructuredBlob, infer_column_type, infer_value_type
from .storage import DuckDBStorage, SQLiteStorage, Storage

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # pipeline
    "Pipeline",
    "IngestResult",
    "ProcessResult",
    # storage
    "Storage",
    "SQLiteStorage",
    "DuckDBStorage",
    # registry + output
    "HandlerRegistry",
    "OutputWriter",
    # structured/schema
    "StructuredBlob",
    "infer_value_type",
    "infer_column_type",
    # readers
    "Sheet",
    "read_file",
    "register_reader",
    "supported_extensions",
    # hashing
    "header_signature",
    "normalize_header",
    "content_hash",
    "new_guid",
    # models
    "SourceFile",
    "Blob",
    "Signature",
    "ColumnMeta",
    "Route",
    "OutputRecord",
    # exceptions
    "StrataError",
    "ReaderError",
    "UnsupportedFileError",
    "MissingDependencyError",
    "NoRouteError",
    "NoHandlerError",
    "StorageError",
]
