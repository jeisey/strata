"""Exception hierarchy for strata."""

from __future__ import annotations


class StrataError(Exception):
    """Base class for every error raised by strata."""


class ReaderError(StrataError):
    """Raised when a source file cannot be read into 2D sheet data."""


class UnsupportedFileError(ReaderError):
    """Raised when no registered reader can handle a file."""


class MissingDependencyError(StrataError):
    """Raised when an optional dependency (e.g. openpyxl) is required but absent."""


class NoRouteError(StrataError):
    """Raised when a blob's signature has no route to a handler."""


class NoHandlerError(StrataError):
    """Raised when a route points at a handler that is not registered."""


class StorageError(StrataError):
    """Raised for storage-backend-level problems."""
