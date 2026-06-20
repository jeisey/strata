"""End-to-end tests for the layered pipeline."""

from __future__ import annotations

import importlib.util
import threading
from pathlib import Path

import pytest

from strata import DuckDBStorage, Pipeline, SQLiteStorage
from strata.storage.base import Storage

_DUCKDB = importlib.util.find_spec("duckdb") is not None

# The concurrency guarantees must hold for *every* backend, so the relevant
# tests run against both.  DuckDB is optional, hence the conditional skip.
BACKENDS = [
    pytest.param(SQLiteStorage, id="sqlite"),
    pytest.param(
        DuckDBStorage,
        id="duckdb",
        marks=pytest.mark.skipif(not _DUCKDB, reason="duckdb not installed"),
    ),
]


def _write_price_files(directory: Path, n_files: int, rows_per_file: int) -> tuple[list[Path], set]:
    """Write ``n_files`` CSVs with globally-unique SKUs; return paths + SKU set."""
    paths: list[Path] = []
    expected_skus: set = set()
    for f in range(n_files):
        lines = ["SKU,Price"]
        for r in range(rows_per_file):
            sku = f"F{f}R{r}"
            expected_skus.add(sku)
            lines.append(f"{sku},{1.0 + r}")
        path = directory / f"prices_{f}.csv"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        paths.append(path)
    return paths, expected_skus


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


# --------------------------------------------------------------------------- #
# Concurrent ingestion (Pipeline.ingest_many)
# --------------------------------------------------------------------------- #
def test_ingest_many_empty_returns_empty():
    pipe = Pipeline(SQLiteStorage(":memory:"))
    assert pipe.ingest_many([]) == []


def test_ingest_many_preserves_input_order(tmp_path):
    pipe = Pipeline(SQLiteStorage(":memory:"))
    paths, _ = _write_price_files(tmp_path, n_files=6, rows_per_file=1)
    results = pipe.ingest_many(paths, max_workers=4)
    # results align with the input order regardless of completion order
    assert [r.source.original_filename for r in results] == [p.name for p in paths]


def test_ingest_many_propagates_worker_errors(tmp_path):
    pipe = Pipeline(SQLiteStorage(":memory:"))
    good = tmp_path / "good.csv"
    good.write_text("SKU,Price\nA1,1.0\n", encoding="utf-8")
    missing = tmp_path / "does_not_exist.csv"  # read_bytes will raise
    with pytest.raises(FileNotFoundError):
        pipe.ingest_many([good, missing])


@pytest.mark.parametrize("storage_cls", BACKENDS)
def test_ingest_many_loads_all_files(storage_cls, tmp_path):
    pipe = Pipeline(storage_cls(":memory:"))
    paths, expected_skus = _write_price_files(tmp_path, n_files=24, rows_per_file=5)

    results = pipe.ingest_many(paths, max_workers=8)

    assert len(results) == len(paths)
    assert all(not r.skipped_duplicate for r in results)
    # every source registered, every sheet exploded into exactly one blob
    assert len(pipe.sources()) == len(paths)
    assert len(pipe.blobs()) == len(paths)
    # all of them share one header shape -> exactly one signature, no dupes
    assert len(pipe.signatures()) == 1


@pytest.mark.parametrize("storage_cls", BACKENDS)
def test_ingest_many_then_process_loses_no_rows(storage_cls, tmp_path):
    """The end-to-end integrity check: concurrent ingest then process must yield
    every input row exactly once -- no loss, no duplication, no corruption."""
    pipe = Pipeline(storage_cls(":memory:"))

    @pipe.handler("prices")
    def prices(blob, out):
        for row in blob.records():
            out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"])})

    n_files, rows_per_file = 30, 8
    paths, expected_skus = _write_price_files(tmp_path, n_files, rows_per_file)

    pipe.ingest_many(paths, max_workers=12)
    pipe.route_header(["SKU", "Price"], "prices")
    result = pipe.process()

    assert result.ok
    assert result.processed == n_files
    emitted = pipe.output("prices")
    assert len(emitted) == n_files * rows_per_file
    # the exact set of SKUs round-trips -- nothing dropped by a race, nothing
    # double-written by a re-run.
    assert {rec["sku"] for rec in emitted} == expected_skus


@pytest.mark.parametrize("storage_cls", BACKENDS)
def test_concurrent_identical_files_dedupe_to_one_source(storage_cls, tmp_path):
    """Byte-identical files ingested concurrently must register exactly one
    source: the dedup check-and-insert is a single critical section."""
    pipe = Pipeline(storage_cls(":memory:"))
    content = "SKU,Price\nA1,9.99\nB2,12.50\n"
    paths = []
    for i in range(20):
        path = tmp_path / f"copy_{i}.csv"
        path.write_text(content, encoding="utf-8")  # identical bytes -> same hash
        paths.append(path)

    results = pipe.ingest_many(paths, max_workers=16)

    assert len(results) == 20
    assert len(pipe.sources()) == 1
    assert len(pipe.blobs()) == 1
    assert sum(1 for r in results if r.skipped_duplicate) == 19
    assert sum(1 for r in results if not r.skipped_duplicate) == 1


# --------------------------------------------------------------------------- #
# Adversarial: maximal-contention stress, refuting the thread-safety assumption
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("storage_cls", BACKENDS)
def test_barrier_synchronized_ingest_storm(storage_cls, tmp_path):
    """Release many threads simultaneously via a barrier so they all slam the
    shared connection at once -- the harshest test of the locking strategy.

    A broken strategy surfaces here as a raised exception ("database is locked"
    / write-write conflict) or as missing/garbled rows.  We assert neither.
    """
    pipe = Pipeline(storage_cls(":memory:"))
    n = 40
    paths, expected_skus = _write_price_files(tmp_path, n_files=n, rows_per_file=4)

    barrier = threading.Barrier(n)
    errors: list[str] = []
    results: list = [None] * n

    def worker(i: int) -> None:
        try:
            barrier.wait()  # all threads proceed together -> maximal contention
            results[i] = pipe.ingest(paths[i])
        except Exception as exc:  # noqa: BLE001 - we want to surface *any* failure
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"concurrency errors: {errors}"
    assert all(r is not None and not r.skipped_duplicate for r in results)
    assert len(pipe.sources()) == n
    assert len(pipe.blobs()) == n
    # blob data survived intact for every file (no torn writes)
    for blob in pipe.blobs():
        assert blob.header() == ["SKU", "Price"]
        assert blob.n_rows == 5  # header + 4 data rows


@pytest.mark.parametrize("storage_cls", BACKENDS)
def test_concurrent_writes_and_reads_stay_consistent(storage_cls, tmp_path):
    """Interleave concurrent ingests with concurrent reads of the store.  A
    backend whose reads aren't serialised against in-flight writes would raise
    or return inconsistent snapshots; we tolerate neither."""
    storage: Storage = storage_cls(":memory:")
    pipe = Pipeline(storage)
    n = 30
    paths, _ = _write_price_files(tmp_path, n_files=n, rows_per_file=3)

    errors: list[str] = []
    stop = threading.Event()

    def reader() -> None:
        try:
            while not stop.is_set():
                # these reads must never see a half-written row or raise
                _ = pipe.summary()
                _ = pipe.blobs()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"reader {type(exc).__name__}: {exc}")

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in readers:
        t.start()
    try:
        results = pipe.ingest_many(paths, max_workers=10)
    finally:
        stop.set()
        for t in readers:
            t.join()

    assert errors == [], f"reader errors: {errors}"
    assert len(results) == n
    assert len(pipe.sources()) == n
    assert len(pipe.blobs()) == n
