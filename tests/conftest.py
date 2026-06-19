"""Shared pytest fixtures and helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from strata import Pipeline, SQLiteStorage


@pytest.fixture
def pipe() -> Pipeline:
    """A pipeline backed by a fresh in-memory SQLite store."""
    return Pipeline(SQLiteStorage(":memory:"))


@pytest.fixture
def write_csv(tmp_path: Path):
    """Factory: write text to a CSV file under tmp_path and return its path."""

    def _write(name: str, text: str) -> Path:
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        return path

    return _write


@pytest.fixture
def prices_jan(write_csv) -> Path:
    return write_csv("prices_jan.csv", "SKU,Price\nA1,9.99\nB2,12.50\n")


@pytest.fixture
def prices_feb(write_csv) -> Path:
    # Same shop, later month, with a brand-new Currency column (schema drift).
    return write_csv(
        "prices_feb.csv", "SKU,Price,Currency\nA1,9.99,USD\nB2,13.00,USD\n"
    )


@pytest.fixture
def excel_workbook(tmp_path: Path):
    """Factory: build a multi-sheet .xlsx with typed cells (needs openpyxl)."""

    def _build(name: str = "book.xlsx") -> Path:
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.Workbook()
        ws1 = wb.active
        ws1.title = "Prices"
        ws1.append(["SKU", "Price", "AsOf"])
        ws1.append(["A1", 9.99, "2024-01-01"])
        ws1.append(["B2", 13, "2024-01-02"])
        ws2 = wb.create_sheet("Notes")
        ws2.append(["key", "value"])
        ws2.append(["author", "jill"])
        path = tmp_path / name
        wb.save(path)
        return path

    return _build
