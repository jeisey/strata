"""Tests for the strata command-line interface."""

from __future__ import annotations

import json

from strata.cli import main

HANDLERS = '''
def setup(pipeline):
    @pipeline.handler("prices_v1")
    def prices_v1(blob, out):
        for row in blob.records():
            out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"])})
    pipeline.route_header(["SKU", "Price"], "prices_v1")
'''


def _run(capsys, *argv):
    code = main(list(argv))
    out = capsys.readouterr().out
    return code, out


def test_full_cli_workflow(tmp_path, capsys):
    db = tmp_path / "staging.db"
    csv = tmp_path / "prices.csv"
    csv.write_text("SKU,Price\nA1,9.99\nB2,12.50\n")
    handlers = tmp_path / "handlers.py"
    handlers.write_text(HANDLERS)

    dbarg = ["--db", str(db)]

    # ingest (L0/L1/L2)
    code, out = _run(capsys, *dbarg, "ingest", str(csv))
    assert code == 0 and "ingested" in out

    # duplicate ingest is skipped
    code, out = _run(capsys, *dbarg, "ingest", str(csv))
    assert "skipped (duplicate)" in out

    # sources / blobs
    _, out = _run(capsys, *dbarg, "sources")
    assert "prices.csv" in out
    _, out = _run(capsys, *dbarg, "blobs")
    assert "status=pending" in out

    # signatures show as unrouted before any route exists
    _, out = _run(capsys, *dbarg, "signatures")
    assert "UNROUTED" in out

    # process with handlers (L3/L4)
    code, out = _run(capsys, *dbarg, "process", "--handlers", str(handlers))
    assert code == 0
    assert "processed=1" in out and "records=2" in out

    # routes now exist
    _, out = _run(capsys, *dbarg, "routes")
    assert "prices_v1" in out

    # output table prints JSON lines
    _, out = _run(capsys, *dbarg, "output", "prices")
    rows = [json.loads(line) for line in out.splitlines()]
    assert rows == [{"sku": "A1", "price": 9.99}, {"sku": "B2", "price": 12.5}]

    # summary
    _, out = _run(capsys, *dbarg, "summary")
    assert "outputs" in out and "processed" in out


def test_process_without_handlers_hints(tmp_path, capsys):
    db = tmp_path / "s.db"
    csv = tmp_path / "p.csv"
    csv.write_text("A,B\n1,2\n")
    main(["--db", str(db), "ingest", str(csv)])
    capsys.readouterr()  # clear the ingest output
    code = main(["--db", str(db), "process"])
    captured = capsys.readouterr()
    assert code == 0  # no errors, just nothing routed
    assert "no_route=1" in captured.out
    assert "hint" in captured.err
