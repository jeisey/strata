"""End-to-end tests for the layered pipeline."""

from __future__ import annotations

import pytest


def test_ingest_indexes_source_and_blobs(pipe, prices_jan):
    result = pipe.ingest(prices_jan)
    assert result.skipped_duplicate is False
    assert result.source.original_filename == "prices_jan.csv"
    assert result.source.byte_size > 0
    assert result.n_blobs == 1
    assert len(result.new_signatures) == 1
    # the source is indexed (L0) and the blob carries lineage (L1)
    assert len(pipe.sources()) == 1
    blob = pipe.blobs()[0]
    assert blob.source_id == result.source.source_id
    assert blob.signature_hash is not None  # signature computed (L2)


def test_duplicate_file_is_skipped(pipe, prices_jan):
    pipe.ingest(prices_jan)
    again = pipe.ingest(prices_jan)
    assert again.skipped_duplicate is True
    assert len(pipe.sources()) == 1
    # ...unless explicitly allowed
    forced = pipe.ingest(prices_jan, skip_duplicate=False)
    assert forced.skipped_duplicate is False
    assert len(pipe.sources()) == 2


def test_deferred_signatures(prices_jan):
    from strata import Pipeline, SQLiteStorage

    pipe = Pipeline(SQLiteStorage(":memory:"))
    pipe.ingest(prices_jan, compute_signatures=False)
    assert pipe.blobs()[0].signature_hash is None
    assert len(pipe.signatures()) == 0
    assert pipe.signaturize() == 1
    assert pipe.blobs()[0].signature_hash is not None


def test_routing_and_processing_with_lineage(pipe, prices_jan):
    @pipe.handler("prices_v1")
    def prices_v1(blob, out):
        for row in blob.records():
            out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"])})

    pipe.ingest(prices_jan)
    pipe.route_header(["SKU", "Price"], "prices_v1")

    result = pipe.process()
    assert result.ok
    assert result.processed == 1
    assert result.records_emitted == 2
    assert result.by_handler == {"prices_v1": 1}

    prices = pipe.output("prices")
    assert prices == [{"sku": "A1", "price": 9.99}, {"sku": "B2", "price": 12.5}]

    # gold-layer rows trace back to the blob and source they came from
    records = pipe.storage.fetch_output_records("prices")
    source_id = pipe.sources()[0].source_id
    assert all(r.source_id == source_id for r in records)


def test_schema_evolution_routes_to_different_handlers(pipe, prices_jan, prices_feb):
    """Jill's case: new columns over time -> new signature -> its own handler,
    yet both land in one unified output table."""

    @pipe.handler("prices_v1")
    def prices_v1(blob, out):
        for row in blob.records():
            out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"]), "ccy": "USD"})

    @pipe.handler("prices_v2")
    def prices_v2(blob, out):
        for row in blob.records():
            out.emit(
                "prices",
                {"sku": row["SKU"], "price": float(row["Price"]), "ccy": row["Currency"]},
            )

    pipe.ingest(prices_jan)
    pipe.ingest(prices_feb)
    assert len(pipe.signatures()) == 2

    pipe.route_header(["SKU", "Price"], "prices_v1")
    pipe.route_header(["SKU", "Price", "Currency"], "prices_v2")

    result = pipe.process()
    assert result.processed == 2
    assert set(result.by_handler) == {"prices_v1", "prices_v2"}
    assert pipe.storage.count_output("prices") == 4


def test_unrouted_blob_is_marked_no_route(pipe, prices_jan):
    pipe.ingest(prices_jan)
    assert len(pipe.unrouted_signatures()) == 1

    result = pipe.process()
    assert result.processed == 0
    assert len(result.no_route) == 1
    assert pipe.blobs()[0].process_status == "no_route"


def test_handler_error_is_recorded_not_raised(pipe, prices_jan):
    @pipe.handler("boom")
    def boom(blob, out):
        raise ValueError("kaboom")

    pipe.ingest(prices_jan)
    pipe.route_header(["SKU", "Price"], "boom")

    result = pipe.process()
    assert not result.ok
    assert len(result.errors) == 1
    assert "kaboom" in result.errors[0][1]
    assert pipe.blobs()[0].process_status == "error"


def test_route_to_unregistered_handler_is_error(pipe, prices_jan):
    pipe.ingest(prices_jan)
    pipe.route_header(["SKU", "Price"], "does_not_exist")
    result = pipe.process()
    assert len(result.errors) == 1
    assert "does_not_exist" in result.errors[0][1]


def test_reprocess_is_idempotent(pipe, prices_jan):
    @pipe.handler("prices_v1")
    def prices_v1(blob, out):
        for row in blob.records():
            out.emit("prices", {"sku": row["SKU"]})

    pipe.ingest(prices_jan)
    pipe.route_header(["SKU", "Price"], "prices_v1")
    pipe.process()
    assert pipe.storage.count_output("prices") == 2

    # re-running everything must not duplicate output rows
    pipe.process(reprocess=True)
    assert pipe.storage.count_output("prices") == 2


def test_process_single_blob(pipe, prices_jan, prices_feb):
    @pipe.handler("h")
    def h(blob, out):
        out.emit("t", {"n": blob.n_rows})

    pipe.ingest(prices_jan)
    pipe.ingest(prices_feb)
    pipe.route_header(["SKU", "Price"], "h")
    pipe.route_header(["SKU", "Price", "Currency"], "h")

    target = pipe.blobs()[0]
    result = pipe.process(blob_id=target.blob_id)
    assert result.processed == 1
    assert pipe.storage.count_output("t") == 1


def test_structured_accessor_by_id(pipe, prices_jan):
    pipe.ingest(prices_jan)
    blob = pipe.blobs()[0]
    sb = pipe.structured(blob.blob_id)
    assert sb.columns_names == ["SKU", "Price"]
    with pytest.raises(KeyError):
        pipe.structured("missing-id")


def test_summary_counts(pipe, prices_jan):
    @pipe.handler("h")
    def h(blob, out):
        out.emit("t", {"x": 1})

    pipe.ingest(prices_jan)
    pipe.route_header(["SKU", "Price"], "h")
    pipe.process()
    summary = pipe.summary()
    assert summary["sources"] == 1
    assert summary["blobs"] == 1
    assert summary["routes"] == 1
    assert summary["processed"] == 1
    assert summary["outputs"] == 1


def test_excel_multisheet_becomes_multiple_blobs(pipe, excel_workbook):
    path = excel_workbook()
    result = pipe.ingest(path)
    assert result.n_blobs == 2  # one blob per worksheet
    sheet_names = {b.sheet_name for b in pipe.blobs()}
    assert sheet_names == {"Prices", "Notes"}
    # both sheets got their own signature
    assert len(pipe.signatures()) == 2
