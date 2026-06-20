"""Tests for the DuckDB staging backend.

Mirrors ``test_storage_sqlite.py`` one-for-one: the two backends implement the
same :class:`~strata.storage.base.Storage` contract and must be behaviourally
interchangeable.  Skipped entirely when the optional ``duckdb`` dependency is
not installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("duckdb")

from strata.models import Blob, ColumnMeta, OutputRecord, Route, Signature, SourceFile  # noqa: E402
from strata.storage import DuckDBStorage  # noqa: E402


def _store() -> DuckDBStorage:
    s = DuckDBStorage(":memory:")
    s.initialize()
    return s


def test_initialize_is_idempotent():
    s = _store()
    s.initialize()  # second call must not raise
    assert s.list_source_files() == []


def test_source_roundtrip_and_lookup_by_hash():
    s = _store()
    src = SourceFile("sid", "/p/a.csv", "a.csv", "deadbeef", 123, extra={"team": "fin"})
    s.add_source_file(src)
    got = s.get_source_file("sid")
    assert got is not None and got.original_filename == "a.csv" and got.extra == {"team": "fin"}
    assert [x.source_id for x in s.find_sources_by_content_hash("deadbeef")] == ["sid"]
    assert s.find_sources_by_content_hash("nope") == []


def test_blob_filters():
    s = _store()
    s.add_source_file(SourceFile("sid", "p", "p", "h", 1))
    s.add_blob(Blob("b1", "sid", "s1", 0, 1, 1, [["x"]]))
    s.add_blob(Blob("b2", "sid", "s2", 1, 1, 1, [["y"]], signature_hash="sig"))
    assert {b.blob_id for b in s.list_blobs(source_id="sid")} == {"b1", "b2"}
    assert [b.blob_id for b in s.list_blobs(without_signature=True)] == ["b1"]
    assert [b.blob_id for b in s.list_blobs(signature_hash="sig")] == ["b2"]
    assert [b.blob_id for b in s.list_blobs(status="pending")] == ["b1", "b2"]


def test_set_signature_and_mark_processed():
    s = _store()
    s.add_source_file(SourceFile("sid", "p", "p", "h", 1))
    s.add_blob(Blob("b1", "sid", "s1", 0, 1, 1, [["x"]]))
    s.set_blob_signature("b1", "sig123")
    s.mark_blob_processed("b1", status="processed", handler_name="h1")
    b = s.get_blob("b1")
    assert b.signature_hash == "sig123"
    assert b.process_status == "processed"
    assert b.processed_by == "h1"
    assert b.processed_at is not None


def test_signature_upsert_updates_columns():
    s = _store()
    cols = [ColumnMeta("a", 0, "int", False, 1)]
    s.upsert_signature(Signature("sig", ["a"], 1, [], sample_blob_id="b1"))
    s.upsert_signature(Signature("sig", ["a"], 1, cols, sample_blob_id="b2"))
    got = s.get_signature("sig")
    assert got.sample_blob_id == "b2"
    assert got.columns[0].inferred_type == "int"
    assert len(s.list_signatures()) == 1


def test_route_crud():
    s = _store()
    s.set_route(Route("sig", "handler_a", note="first"))
    assert s.get_route("sig").handler_name == "handler_a"
    s.set_route(Route("sig", "handler_b"))  # upsert
    assert s.get_route("sig").handler_name == "handler_b"
    assert len(s.list_routes()) == 1
    assert s.delete_route("sig") is True
    assert s.delete_route("sig") is False
    assert s.get_route("sig") is None


def test_outputs_and_lineage():
    s = _store()
    s.add_output(OutputRecord("o1", "prices", "b1", "sid", {"sku": "A1", "price": 9.99}))
    s.add_output(OutputRecord("o2", "prices", "b2", "sid", {"sku": "B2", "price": 1.0}))
    s.add_output(OutputRecord("o3", "notes", "b3", "sid", {"k": "v"}))
    assert s.fetch_output("prices") == [
        {"sku": "A1", "price": 9.99},
        {"sku": "B2", "price": 1.0},
    ]
    assert s.fetch_output("prices", blob_id="b1") == [{"sku": "A1", "price": 9.99}]
    assert sorted(s.list_output_tables()) == ["notes", "prices"]
    assert s.count_output() == 3
    assert s.count_output("prices") == 2
    recs = s.fetch_output_records("prices", blob_id="b2")
    assert recs[0].source_id == "sid" and recs[0].blob_id == "b2"


def test_delete_outputs_for_blob():
    s = _store()
    s.add_output(OutputRecord("o1", "prices", "b1", "sid", {"x": 1}))
    s.add_output(OutputRecord("o2", "prices", "b2", "sid", {"x": 2}))
    assert s.delete_outputs_for_blob("b1") == 1
    assert s.count_output() == 1


def test_context_manager_closes():
    with DuckDBStorage(":memory:") as s:
        s.initialize()
        s.add_source_file(SourceFile("sid", "p", "p", "h", 1))
        assert len(s.list_source_files()) == 1


def test_byte_size_handles_large_values():
    """DuckDB's BIGINT byte_size column must round-trip values beyond INT32."""
    s = _store()
    big = 5_000_000_000  # > 2**32, would overflow a 32-bit INTEGER
    s.add_source_file(SourceFile("sid", "p", "big.csv", "h", big))
    assert s.get_source_file("sid").byte_size == big
