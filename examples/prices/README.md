# Example: normalising differently-shaped price sheets

Three CSVs, three different header shapes, one clean output table.

| File | Header | Notes |
|------|--------|-------|
| `prices_2023.csv` | `SKU,Price` | the original shape |
| `prices_2024_h1.csv` | `SKU,Price,Currency` | a **new column** appeared (schema drift) |
| `vendor_b.csv` | `item_code,unit_cost_eur,as_of` | a different vendor, prices in EUR |

Each header hashes to its own signature, each signature routes to its own
handler, and all three handlers emit into a single unified `prices` table.

## Run it

End-to-end story (in-memory database):

```bash
python examples/prices/run.py
```

Or via the CLI with a persistent database:

```bash
strata --db prices.db ingest examples/prices/*.csv
strata --db prices.db signatures                       # see the 3 shapes
strata --db prices.db process --handlers examples/prices/handlers.py
strata --db prices.db output prices
```

[`handlers.py`](handlers.py) defines the extractors and the
`signature → handler` routes via a `setup(pipeline)` function.
