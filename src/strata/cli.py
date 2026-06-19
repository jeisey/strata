"""Command-line interface for strata.

    strata ingest data/*.xlsx          # L0 + L1 + L2
    strata signatures                  # inspect discovered header shapes
    strata process --handlers h.py     # L3 dispatch + L4 extract
    strata output prices               # read a gold-layer table

The staging database defaults to ``strata.db`` in the working directory and can
be overridden with ``--db`` on any command.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Optional

from . import __version__
from .hashing import short_guid
from .pipeline import Pipeline
from .storage.sqlite import SQLiteStorage


def _build_pipeline(args: argparse.Namespace) -> Pipeline:
    return Pipeline(SQLiteStorage(args.db))


def _load_handlers(pipeline: Pipeline, path: str) -> None:
    """Import a handlers file and call its ``setup(pipeline)`` function."""
    module_path = Path(path)
    if not module_path.exists():
        raise SystemExit(f"handlers file not found: {path}")
    spec = importlib.util.spec_from_file_location("strata_handlers", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise SystemExit(f"could not load handlers from: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    setup = getattr(module, "setup", None)
    if not callable(setup):
        raise SystemExit(f"handlers file {path} must define a setup(pipeline) function")
    setup(pipeline)


# --------------------------------------------------------------------------- #
# command implementations
# --------------------------------------------------------------------------- #
def _cmd_ingest(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    for raw_path in args.files:
        result = pipe.ingest(
            raw_path,
            skip_duplicate=not args.allow_duplicates,
            compute_signatures=not args.no_signatures,
        )
        if result.skipped_duplicate:
            print(f"skipped (duplicate)   {raw_path}")
            continue
        src = result.source
        print(
            f"ingested {raw_path}  source={short_guid(src.source_id if src else None)}  "
            f"blobs={result.n_blobs}  new_signatures={len(result.new_signatures)}"
        )
    return 0


def _cmd_sources(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    for s in pipe.sources():
        print(
            f"{short_guid(s.source_id)}  {s.registered_at}  "
            f"{s.byte_size:>10} bytes  {s.original_filename}"
        )
    return 0


def _cmd_blobs(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    for b in pipe.blobs(source_id=args.source):
        sig = short_guid(b.signature_hash) if b.signature_hash else "(none)"
        print(
            f"{short_guid(b.blob_id)}  sheet={b.sheet_name!r:<20}  "
            f"rows={b.n_rows:>6}  cols={b.n_cols:>4}  "
            f"sig={sig:<10}  status={b.process_status}"
        )
    return 0


def _cmd_signatures(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    routes = {r.signature_hash: r.handler_name for r in pipe.routes()}
    for sig in pipe.signatures():
        handler = routes.get(sig.signature_hash, "-- UNROUTED --")
        header = ", ".join(sig.header)
        print(f"{short_guid(sig.signature_hash)}  cols={sig.n_columns:<3}  -> {handler}")
        print(f"    header: {header}")
    return 0


def _cmd_routes(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    for r in pipe.routes():
        note = f"  ({r.note})" if r.note else ""
        print(f"{short_guid(r.signature_hash)}  -> {r.handler_name}{note}")
    return 0


def _cmd_process(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    if args.handlers:
        _load_handlers(pipe, args.handlers)
    result = pipe.process(reprocess=args.reprocess)
    print(
        f"processed={result.processed}  records={result.records_emitted}  "
        f"no_route={len(result.no_route)}  errors={len(result.errors)}"
    )
    for blob_id, message in result.errors:
        print(f"  error {short_guid(blob_id)}: {message}", file=sys.stderr)
    if result.no_route and not args.handlers:
        print(
            "  hint: pass --handlers to register handlers and routes before processing",
            file=sys.stderr,
        )
    return 1 if result.errors else 0


def _cmd_output(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    records = pipe.output(args.table)
    if args.limit is not None:
        records = records[: args.limit]
    for rec in records:
        print(json.dumps(rec, ensure_ascii=False))
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    pipe = _build_pipeline(args)
    summary = pipe.summary()
    width = max(len(k) for k in summary)
    for key, value in summary.items():
        print(f"{key:<{width}} : {value}")
    return 0


# --------------------------------------------------------------------------- #
# argument parser
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="strata", description=__doc__.splitlines()[0])
    parser.add_argument("--version", action="version", version=f"strata {__version__}")
    parser.add_argument(
        "--db", default="strata.db", help="path to the staging database (default: strata.db)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="index files and extract raw blobs (L0/L1/L2)")
    p_ingest.add_argument("files", nargs="+", help="spreadsheet/CSV files to ingest")
    p_ingest.add_argument(
        "--no-signatures", action="store_true", help="skip signature computation (L2)"
    )
    p_ingest.add_argument(
        "--allow-duplicates", action="store_true", help="ingest even if bytes already seen"
    )
    p_ingest.set_defaults(func=_cmd_ingest)

    p_sources = sub.add_parser("sources", help="list indexed source files (L0)")
    p_sources.set_defaults(func=_cmd_sources)

    p_blobs = sub.add_parser("blobs", help="list raw blobs (L1)")
    p_blobs.add_argument("--source", default=None, help="filter by source id")
    p_blobs.set_defaults(func=_cmd_blobs)

    p_sigs = sub.add_parser("signatures", help="list header signatures and their routes (L2/L3)")
    p_sigs.set_defaults(func=_cmd_signatures)

    p_routes = sub.add_parser("routes", help="list signature -> handler routes (L3)")
    p_routes.set_defaults(func=_cmd_routes)

    p_proc = sub.add_parser("process", help="dispatch handlers over blobs (L3/L4)")
    p_proc.add_argument(
        "--handlers", default=None, help="path to a .py file defining setup(pipeline)"
    )
    p_proc.add_argument(
        "--reprocess", action="store_true", help="re-run all blobs, not just pending ones"
    )
    p_proc.set_defaults(func=_cmd_process)

    p_out = sub.add_parser("output", help="print records from an output table (L4)")
    p_out.add_argument("table", help="output table name")
    p_out.add_argument("--limit", type=int, default=None, help="max records to print")
    p_out.set_defaults(func=_cmd_output)

    p_sum = sub.add_parser("summary", help="counts across all layers")
    p_sum.set_defaults(func=_cmd_summary)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
