"""CSV reader built on the standard library.

CSVs are the canonical "data that changes over time, like new columns" case the
first commenter calls out, so they are first-class here right next to Excel.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Union

from ..exceptions import ReaderError
from .base import Sheet

PathLike = Union[str, Path]

# Read a chunk to sniff the dialect; large enough to catch the header + a few rows.
_SNIFF_BYTES = 8192


def read_csv(path: PathLike, *, encoding: str = "utf-8-sig") -> list[Sheet]:
    """Read a delimited text file into a single :class:`Sheet`.

    The delimiter is sniffed from the file's first few KB and falls back to a
    comma.  ``utf-8-sig`` transparently strips a leading BOM if present.
    """
    p = Path(path)
    try:
        with p.open("r", newline="", encoding=encoding) as fh:
            sample = fh.read(_SNIFF_BYTES)
            fh.seek(0)
            dialect: Union[type[csv.Dialect], csv.Dialect]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel  # default comma-separated dialect
            reader = csv.reader(fh, dialect)
            rows: list[list] = [list(row) for row in reader]
    except OSError as exc:
        raise ReaderError(f"could not read CSV {p}: {exc}") from exc

    return [Sheet(name=p.stem, index=0, rows=rows)]
