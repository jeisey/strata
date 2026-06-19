"""Reader protocol and the :class:`Sheet` value object.

A *reader* turns a file on disk into one or more :class:`Sheet` objects -- each
a name plus a 2D grid of string (or ``None``) cells.  This is the only place
file formats are interpreted; everything above the raw layer works on
``Sheet``/``Blob`` data and never touches the original format again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Sheet:
    """One sheet of generic 2D data extracted from a source file."""

    name: str
    index: int
    rows: list[list[Optional[str]]] = field(default_factory=list)

    @property
    def n_rows(self) -> int:
        return len(self.rows)

    @property
    def n_cols(self) -> int:
        return max((len(r) for r in self.rows), default=0)
