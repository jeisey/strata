"""Storage backends for the staging database."""

from __future__ import annotations

from .base import Storage
from .sqlite import SQLiteStorage

__all__ = ["Storage", "SQLiteStorage"]
