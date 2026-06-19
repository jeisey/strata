"""Tests for the file readers and the reader registry."""

from __future__ import annotations

import pytest

from strata.exceptions import UnsupportedFileError
from strata.readers import read_file, register_reader, supported_extensions


def test_read_csv_basic(write_csv):
    path = write_csv("a.csv", "SKU,Price\nA1,9.99\n")
    sheets = read_file(path)
    assert len(sheets) == 1
    assert sheets[0].name == "a"
    assert sheets[0].rows == [["SKU", "Price"], ["A1", "9.99"]]


def test_read_csv_sniffs_semicolon(write_csv):
    path = write_csv("b.csv", "SKU;Price\nA1;9.99\n")
    sheets = read_file(path)
    assert sheets[0].rows == [["SKU", "Price"], ["A1", "9.99"]]


def test_read_csv_strips_bom(write_csv):
    path = write_csv("c.csv", "﻿SKU,Price\nA1,9.99\n")
    sheets = read_file(path)
    assert sheets[0].rows[0][0] == "SKU"


def test_tab_separated_via_txt(write_csv):
    path = write_csv("d.tsv", "SKU\tPrice\nA1\t9.99\n")
    sheets = read_file(path)
    assert sheets[0].rows == [["SKU", "Price"], ["A1", "9.99"]]


def test_unsupported_extension_raises(tmp_path):
    path = tmp_path / "thing.parquet"
    path.write_bytes(b"\x00")
    with pytest.raises(UnsupportedFileError):
        read_file(path)


def test_register_custom_reader(tmp_path):
    from strata.readers.base import Sheet

    def fake_reader(path):
        return [Sheet(name="fake", index=0, rows=[["x"], ["1"]])]

    register_reader(".fake", fake_reader)
    assert ".fake" in supported_extensions()
    sheets = read_file(tmp_path / "z.fake")
    assert sheets[0].name == "fake"


def test_read_excel_multiple_sheets_and_types(excel_workbook):
    path = excel_workbook()
    sheets = read_file(path)
    assert [s.name for s in sheets] == ["Prices", "Notes"]
    prices = sheets[0]
    assert prices.rows[0] == ["SKU", "Price", "AsOf"]
    # 13 (an integral float in Excel) must not become "13.0"
    assert prices.rows[2] == ["B2", "13", "2024-01-02"]
