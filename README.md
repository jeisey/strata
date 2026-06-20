# strata

**A layered, signature-routed ETL framework for spreadsheets and CSVs that
change shape over time.**

`strata` ingests messy, heterogeneous, drifting spreadsheets by building data up
**from the bottom to the top** — from a loose index of files, through raw 2D
blobs, to header *signatures*, to dynamic routing, and finally to clean typed
output tables. Each layer up adds structure and guarantees while the layer below
stays maximally flexible.

It is a faithful, batteries-included implementation of a battle-tested pattern
described in two data-engineering write-ups (reproduced and credited
[below](#where-this-comes-from)): **hash the header row to fingerprint each
sheet's shape, then look that fingerprint up in a table to decide which
extractor to run.** As one of the authors put it: *"Above logic hasn't failed me
once in over 15 years of ETL/ELT."*

```python
from strata import Pipeline, SQLiteStorage

pipe = Pipeline(SQLiteStorage("staging.db"))

@pipe.handler("prices_v1")                       # an extractor ("stored proc")
def prices_v1(blob, out):
    for row in blob.records():
        out.emit("prices", {"sku": row["SKU"], "price": float(row["Price"])})

pipe.ingest("prices_january.csv")                # L0 index + L1 blob + L2 signature
pipe.route_header(["SKU", "Price"], "prices_v1") # L3 signature -> handler lookup row
pipe.process()                                   # L3 dispatch + L4 extract

pipe.output("prices")
# [{'sku': 'A1', 'price': 9.99}, {'sku': 'B2', 'price': 12.5}]
```

---

## The idea: build from the bottom to the top

> At the bottom you've applied the least structure to the data, but you've also
> kept it the most flexible. At the top you've added stronger format and semantic
> guarantees on the output data but made it less flexible.

```
   guarantees ↑                                                  flexibility ↓
 ┌───────────────────────────────────────────────────────────────────────────┐
 │  L4  SEMANTIC   typed records in output tables  (e.g. one "prices" table)   │  most structure
 │  L3  ROUTING    signature -> handler lookup + dynamic dispatch              │
 │  L2  SIGNATURE  MD5(header row) + inferred schema (columns, types, counts)  │
 │  L1  RAW        each sheet as a generic 2D string blob (per-blob GUID)      │
 │  L0  INDEX      a registry of source files (path, name, timestamp, GUID)    │  most flexible
 └───────────────────────────────────────────────────────────────────────────┘
```

| Layer | What it produces | What you can rely on |
|------:|------------------|----------------------|
| **L0 Index** | a row per file: GUID, path, original filename, content hash, byte size, timestamp | the file existed and was registered |
| **L1 Raw** | one blob per sheet: a 2D grid of strings, its own GUID, lineage to L0 | you can re-read the bytes/grid later |
| **L2 Signature** | an `MD5` of the header row + a schema snapshot (column names, inferred types, row counts) | sheets with the same header share one signature |
| **L3 Routing** | a `signature → handler` lookup table, resolved against a handler registry | each known shape has a designated extractor |
| **L4 Semantic** | typed records in named output tables, each carrying lineage back to its blob | "I collected all the prices from these sheets" |

The further up you go, the more you must know about the data — but the stronger
the promises you can make about the output.

---

## Why signatures? Handling spreadsheets that drift

Real spreadsheets gain columns, get reordered, and arrive from different vendors
in different shapes. `strata` fingerprints each sheet by hashing **line #1 — the
column headers** — with `MD5`. Two sheets with the same header get the same
signature; **add a column and you get a brand-new signature** that you can route
to its own handler, without touching the old one.

```python
pipe.ingest("prices_2023.csv")        # header: SKU,Price          -> signature A
pipe.ingest("prices_2024.csv")        # header: SKU,Price,Currency -> signature B (new!)

pipe.route_header(["SKU", "Price"],             "prices_v1")
pipe.route_header(["SKU", "Price", "Currency"], "prices_v2")

pipe.process()   # each shape dispatched to its own extractor, both feed "prices"
```

`pipe.unrouted_signatures()` tells you which shapes have shown up that you
haven't taught the system to handle yet — your work queue when a file drifts.

---

## Install

```bash
pip install strata-etl            # CSV support, zero dependencies
pip install "strata-etl[excel]"   # adds .xlsx / .xlsm via openpyxl
```

From source:

```bash
git clone https://github.com/jeisey/strata
cd strata
pip install -e ".[dev]"
```

Requires Python 3.9+.

---

## Quickstart (Python API)

```python
from strata import Pipeline, SQLiteStorage

# The "staging database". Use a path for persistence or ":memory:" for tests.
pipe = Pipeline(SQLiteStorage("staging.db"))

# --- L0 + L1 + L2: index the file, explode sheets to blobs, fingerprint headers
result = pipe.ingest("data/prices_january.xlsx")
print(result.n_blobs, "blobs;", len(result.new_signatures), "new signatures")

# --- L3: write an extractor and map a header shape to it
@pipe.handler("acme_prices_v1")
def acme_prices_v1(blob, out):
    # `blob` is a StructuredBlob: a table-like view over the raw 2D data.
    for row in blob.records():            # row is a dict keyed by column name
        out.emit("prices", {
            "sku": row["SKU"],
            "price": float(row["Price"]),
            "currency": row.get("Currency", "USD"),
        })

pipe.route_header(["SKU", "Price", "Currency"], "acme_prices_v1")

# --- L3 dispatch + L4 extract
report = pipe.process()
print(report)   # ProcessResult(processed=…, records_emitted=…, errors=[…], …)

# --- read the gold layer
for row in pipe.output("prices"):
    print(row)
```

### Handlers

A handler is any callable `handler(blob, out)`:

- `blob` is a [`StructuredBlob`](src/strata/schema.py): `blob.records()` yields
  each data row as a dict keyed by column name; `blob.schema` gives inferred
  column types; `blob.rows`, `blob.n_rows`, `blob.columns_names` are also there.
- `out` is an [`OutputWriter`](src/strata/output.py): `out.emit(table, record)`
  writes one record into a named output table and automatically stamps it with
  lineage (`blob_id`, `source_id`).

Errors raised inside a handler are caught, recorded against the blob
(`process_status = "error"`), and surfaced in `ProcessResult.errors` — one bad
sheet never sinks the batch.

### Lineage

Every gold-layer record can be traced all the way back down:

```python
rec = pipe.storage.fetch_output_records("prices")[0]
src = pipe.storage.get_source_file(rec.source_id)
print(rec.record, "<- blob", rec.blob_id[:8], "<- file", src.original_filename)
```

---

## Command-line interface

```bash
# L0 + L1 + L2 — index files, extract blobs, fingerprint headers
strata --db staging.db ingest data/*.csv data/*.xlsx

# Inspect what shapes showed up (and which are not routed yet)
strata --db staging.db signatures

# L3 + L4 — register handlers/routes from a Python file and process
strata --db staging.db process --handlers handlers.py

# Read a gold-layer table (JSON lines)
strata --db staging.db output prices --limit 5

# Counts across every layer
strata --db staging.db summary
```

The `--handlers` file just needs a `setup(pipeline)` function that registers
handlers and routes — see [`examples/prices/handlers.py`](examples/prices/handlers.py).
`python -m strata …` works too.

---

## Worked example

[`examples/prices/`](examples/prices) contains three deliberately
**differently-shaped** price sheets (an old `SKU,Price` file, a newer one with a
`Currency` column, and a different vendor's `item_code,unit_cost_eur,as_of`
format) plus handlers that normalise all three into one clean `prices` table:

```bash
python examples/prices/run.py
```

It prints the discovered signatures, the routing, the unified output, and a
lineage trace — the second author's *"collect all the prices from these
differently formatted spreadsheets"* goal, realised.

---

## Architecture

```
            ┌──────────────┐
  files ───▶│   readers    │  csv (stdlib) · xlsx/xlsm (openpyxl) · pluggable
            └──────┬───────┘
                   │ Sheet[]  (2D string grids)
            ┌──────▼───────┐
            │   Pipeline   │  orchestrates the five layers
            └──────┬───────┘
                   │ reads/writes everything through…
            ┌──────▼───────┐
            │   Storage    │  abstract backend  ──▶  SQLiteStorage (bundled)
            └──────────────┘
```

The staging database holds five tables, one per concept:
`source_files` (L0), `blobs` (L1), `signatures` (L2), `routes` (L3), and
`outputs` (L4, with `blob_id`/`source_id` lineage columns).

### Swap the storage backend

`SQLiteStorage` is the default, but the entire framework talks to the abstract
[`Storage`](src/strata/storage/base.py) interface. Implement it against
Postgres, DuckDB, a cloud object store, etc., and pass it to `Pipeline` — the
"dump blobs into a staging database" idea is backend-agnostic by design.

### Add a reader

```python
from strata.readers import register_reader
from strata.readers.base import Sheet

def read_jsonl(path):
    ...
    return [Sheet(name="data", index=0, rows=rows)]

register_reader(".jsonl", read_jsonl)
```

---

## Where this comes from

This package implements, faithfully, the layered approach laid out in two
data-engineering comments. The mapping is one-to-one:

| From the write-ups | In `strata` |
|--------------------|-------------|
| "dumps as generic 2D data each sheet into multiple blobs in a staging database with original file name, time stamp, etc." | **L0 `source_files` + L1 `blobs`** |
| "a guid generated on insert of the blob in the staging table" | every `SourceFile`/`Blob`/`OutputRecord` gets a GUID |
| "the 'signature' of each blob. Usually Line #1. Then MD5() … a hash of line #1 (just column headers)" | **L2** [`header_signature`](src/strata/hashing.py) |
| "I apply this technique to CSV files also, that also change over time, like new columns." | new header ⇒ new signature ⇒ its own route |
| "a lookup into a table, hashed signature, what SP to run … with a dynamic sql statement and passed the blob info reference #" | **L3** `routes` table + handler registry dynamic dispatch |
| "index the spreadsheets: output looks like a table which says there is a excel file at this path and it was registered on this date" | **L0** `source_files` |
| "upload a binary blob of the spreadsheet data into the DB … reader to fetch and interpret" | **L1** `blobs` (generic 2D data) |
| "extract the data into a known file format … interpret the metadata … column x is a integer column y is a datetime … x total rows" | **L2** schema inference ([`StructuredBlob.schema`](src/strata/schema.py)) |
| "a second process which searches … for the information you want to interpret as a price and then load that into a price output table" | **L4** handlers → `outputs` |
| "work up from the bottom to the top" | the whole pipeline |

> The comments are paraphrased and anonymized; this repository is an original
> implementation of the pattern they describe, not their code.

---

## Development

```bash
pip install -e ".[dev]"
pytest          # run the test suite
ruff check .    # lint
```

## License

[MIT](LICENSE).
