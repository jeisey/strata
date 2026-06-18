"""Tests for the signature hashing -- the heart of the framework."""

from __future__ import annotations

import re

from strata.hashing import content_hash, header_signature, new_guid, normalize_header

_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def test_signature_is_32_char_hex_and_deterministic():
    a = header_signature(["SKU", "Price"])
    b = header_signature(["SKU", "Price"])
    assert _HEX32.match(a)
    assert a == b


def test_different_headers_differ():
    assert header_signature(["SKU", "Price"]) != header_signature(["SKU", "Cost"])


def test_new_column_changes_signature():
    """The first commenter's core case: CSVs that gain columns over time."""
    base = header_signature(["SKU", "Price"])
    evolved = header_signature(["SKU", "Price", "Currency"])
    assert base != evolved


def test_delimiter_collision_is_avoided():
    # ["a,b"] must not hash the same as ["a", "b"].
    assert header_signature(["a,b"]) != header_signature(["a", "b"])


def test_strip_and_collapse_whitespace_normalises():
    assert header_signature([" SKU ", "Price"]) == header_signature(["SKU", "Price"])
    assert header_signature(["S K U"]) == header_signature(["S   K   U"])


def test_case_sensitivity_is_opt_in():
    assert header_signature(["SKU"]) != header_signature(["sku"])
    assert header_signature(["SKU"], lower=True) == header_signature(["sku"], lower=True)


def test_normalize_header_handles_none_and_numbers():
    assert normalize_header([None, 1, " x "]) == ["", "1", "x"]


def test_content_hash_is_stable():
    assert content_hash(b"hello") == content_hash(b"hello")
    assert content_hash(b"hello") != content_hash(b"world")


def test_new_guid_is_unique_hex():
    guids = {new_guid() for _ in range(1000)}
    assert len(guids) == 1000
    assert all(len(g) == 32 for g in guids)
