"""Reader registry: dispatch a file to the right reader by extension.

Readers are pluggable.  Register your own for new formats::

    from strata.readers import register_reader

    register_reader(".jsonl", read_my_jsonl)
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Callable, Union

from ..exceptions import UnsupportedFileError
from .base import Sheet
from .csv_reader import read_csv
from .excel_reader import read_excel

PathLike = Union[str, Path]
ReaderFn = Callable[[PathLike], list[Sheet]]

_READERS: dict[str, ReaderFn] = {}


def register_reader(extensions: Union[str, Iterable[str]], reader: ReaderFn) -> None:
    """Register ``reader`` for one or more file extensions (with or without dot)."""
    if isinstance(extensions, str):
        extensions = [extensions]
    for ext in extensions:
        key = ext.lower()
        if not key.startswith("."):
            key = "." + key
        _READERS[key] = reader


def get_reader(path: PathLike) -> ReaderFn:
    """Return the reader registered for ``path``'s extension."""
    suffix = Path(path).suffix.lower()
    try:
        return _READERS[suffix]
    except KeyError:
        raise UnsupportedFileError(
            f"no reader registered for '{suffix or path}'. "
            f"Known extensions: {', '.join(sorted(_READERS)) or '(none)'}"
        ) from None


def read_file(path: PathLike) -> list[Sheet]:
    """Read any supported file into a list of :class:`Sheet` objects."""
    return get_reader(path)(path)


def supported_extensions() -> list[str]:
    """Return the sorted list of registered file extensions."""
    return sorted(_READERS)


# Built-in readers.  CSV's sniffer also handles tab/semicolon/pipe delimited text.
register_reader([".csv", ".tsv", ".txt"], read_csv)
register_reader([".xlsx", ".xlsm"], read_excel)

__all__ = [
    "Sheet",
    "read_file",
    "read_csv",
    "read_excel",
    "register_reader",
    "get_reader",
    "supported_extensions",
]
