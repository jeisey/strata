"""Tests for type inference and the StructuredBlob view."""

from __future__ import annotations

import pytest

from strata.models import Blob
from strata.schema import StructuredBlob, infer_column_type, infer_value_type


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, "empty"),
        ("", "empty"),
        ("   ", "empty"),
        ("true", "bool"),
        ("FALSE", "bool"),
        ("42", "int"),
        ("-7", "int"),
        ("+7", "int"),
        ("3.14", "float"),
        ("1e5", "float"),
        ("2024-01-15", "date"),
        ("2024-01-15T10:30:00", "datetime"),
        ("hello", "str"),
        ("2024", "int"),  # bare year is just an int
    ],
)
def test_infer_value_type(value, expected):
    assert infer_value_type(value) == expected


def test_infer_column_all_ints():
    assert infer_column_type(["1", "2", "3"]) == ("int", 3, False)


def test_infer_column_int_and_float_promotes_to_float():
    col_type, n_nonnull, nullable = infer_column_type(["1", "2.5", "3"])
    assert col_type == "float"
    assert n_nonnull == 3
    assert nullable is False


def test_infer_column_mixed_falls_back_to_str():
    assert infer_column_type(["1", "x"])[0] == "str"


def test_infer_column_with_blanks_is_nullable():
    col_type, n_nonnull, nullable = infer_column_type(["1", "", "3"])
    assert col_type == "int"
    assert n_nonnull == 2
    assert nullable is True


def test_infer_column_all_empty():
    assert infer_column_type(["", None])[0] == "empty"


def test_infer_column_dates_and_datetimes():
    assert infer_column_type(["2024-01-01", "2024-02-01"])[0] == "date"
    assert infer_column_type(["2024-01-01", "2024-02-01T09:00:00"])[0] == "datetime"


def _blob(data):
    return Blob(
        blob_id="b", source_id="s", sheet_name="x", sheet_index=0,
        n_rows=len(data), n_cols=max((len(r) for r in data), default=0), data=data,
    )


def test_structured_records_map_by_header():
    sb = StructuredBlob(_blob([["SKU", "Price"], ["A1", "9.99"], ["B2", "12.5"]]))
    records = list(sb.records())
    assert records == [{"SKU": "A1", "Price": "9.99"}, {"SKU": "B2", "Price": "12.5"}]
    assert sb.n_rows == 2
    assert sb.n_columns == 2


def test_structured_pads_ragged_rows():
    sb = StructuredBlob(_blob([["a", "b", "c"], ["1"]]))
    assert list(sb.records()) == [{"a": "1", "b": None, "c": None}]


def test_structured_dedupes_and_fills_blank_headers():
    sb = StructuredBlob(_blob([["a", "a", ""], ["1", "2", "3"]]))
    assert sb.columns_names == ["a", "a__1", "column_2"]
    assert list(sb.records()) == [{"a": "1", "a__1": "2", "column_2": "3"}]


def test_structured_schema_infers_types():
    sb = StructuredBlob(
        _blob([["SKU", "Price", "AsOf"], ["A1", "9.99", "2024-01-01"]])
    )
    by_name = {c.name: c.inferred_type for c in sb.schema}
    assert by_name == {"SKU": "str", "Price": "float", "AsOf": "date"}


def test_structured_empty_blob_is_safe():
    sb = StructuredBlob(_blob([]))
    assert sb.n_rows == 0
    assert sb.columns_names == []
    assert list(sb.records()) == []
