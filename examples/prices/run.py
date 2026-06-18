"""End-to-end demo of the strata framework on messy price sheets.

Run from the repository root::

    python examples/prices/run.py

It ingests every CSV in this folder (three different header shapes), shows the
header signatures it discovered, routes each shape to a handler, processes them,
and prints one unified ``prices`` table plus a lineage trace.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `import handlers` work regardless of where the script is launched from.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from handlers import setup  # noqa: E402  (import after sys.path tweak)

from strata import Pipeline, SQLiteStorage  # noqa: E402


def main() -> None:
    pipe = Pipeline(SQLiteStorage(":memory:"))

    # L0 + L1 + L2: index files, explode sheets into blobs, fingerprint headers.
    print("== Ingesting (L0 index -> L1 raw blobs -> L2 signatures) ==")
    for csv in sorted(HERE.glob("*.csv")):
        result = pipe.ingest(csv)
        print(
            f"  {csv.name:<22} blobs={result.n_blobs}  "
            f"new_signatures={len(result.new_signatures)}"
        )

    print("\n== Discovered header signatures (L2) ==")
    for sig in pipe.signatures():
        types = ", ".join(f"{c.name}:{c.inferred_type}" for c in sig.columns)
        print(f"  {sig.signature_hash[:8]}  [{types}]")

    # L3: register handlers + signature->handler routes.
    setup(pipe)

    unrouted = pipe.unrouted_signatures()
    print(f"\n== Routing (L3): {len(pipe.routes())} routes, {len(unrouted)} unrouted ==")

    # L3 dispatch + L4 extract.
    result = pipe.process()
    print(
        f"\n== Processing (L3 dispatch -> L4 output): "
        f"processed={result.processed}, records={result.records_emitted} =="
    )

    print("\n== Unified prices (gold layer, L4) ==")
    for row in pipe.output("prices"):
        print(f"  {row['sku']:<12} {row['price']:>7.2f} {row['currency']}")

    print("\n== Lineage: trace a gold row back to its source file ==")
    rec = pipe.storage.fetch_output_records("prices")[0]
    src = pipe.storage.get_source_file(rec.source_id)
    print(f"  {rec.record}")
    print(f"    <- blob {rec.blob_id[:8]} <- file '{src.original_filename}'")

    print("\n== Summary ==")
    for key, value in pipe.summary().items():
        print(f"  {key:<11} {value}")


if __name__ == "__main__":
    main()
