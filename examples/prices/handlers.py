"""Example handlers: normalise three differently-shaped price sheets into one
clean ``prices`` table -- the second commenter's "collect all the prices from
these differently formatted spreadsheets" goal.

Each ``@pipeline.handler`` is the analogue of a stored proc; each
``route_header`` call is a row in the signature->handler lookup table.

Use from the CLI::

    strata --db prices.db ingest examples/prices/*.csv
    strata --db prices.db process --handlers examples/prices/handlers.py
    strata --db prices.db output prices

or run ``python examples/prices/run.py`` for the whole story end to end.
"""

from __future__ import annotations


def setup(pipeline):
    """Register handlers (L4 code) and routes (L3 lookup rows) on a pipeline."""

    @pipeline.handler("prices_2023")
    def prices_2023(blob, out):
        # Oldest sheet: just SKU + Price, currency implied USD.
        for row in blob.records():
            out.emit(
                "prices",
                {"sku": row["SKU"], "price": float(row["Price"]), "currency": "USD"},
            )

    @pipeline.handler("prices_2024")
    def prices_2024(blob, out):
        # Same shop a year later -- a new Currency column appeared (schema drift).
        for row in blob.records():
            out.emit(
                "prices",
                {"sku": row["SKU"], "price": float(row["Price"]), "currency": row["Currency"]},
            )

    @pipeline.handler("vendor_b")
    def vendor_b(blob, out):
        # A different vendor entirely: different column names, prices in EUR.
        for row in blob.records():
            out.emit(
                "prices",
                {
                    "sku": row["item_code"],
                    "price": float(row["unit_cost_eur"]),
                    "currency": "EUR",
                },
            )

    # The lookup table: each header shape (its MD5 signature) -> the handler to run.
    pipeline.route_header(["SKU", "Price"], "prices_2023")
    pipeline.route_header(["SKU", "Price", "Currency"], "prices_2024")
    pipeline.route_header(["item_code", "unit_cost_eur", "as_of"], "vendor_b")
