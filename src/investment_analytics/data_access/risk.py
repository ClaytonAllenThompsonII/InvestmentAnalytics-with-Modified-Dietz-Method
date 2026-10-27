
"""Database access for security-level risk analytics."""

from datetime import date, timedelta

import pandas as pd

from investment_analytics.data_access.database import get_connection


def get_latest_benchmark_date(benchmark: str = "SPY") -> date:
    """Find the latest available benchmark trading date."""
    query = """
        SELECT MAX(price_date)
        FROM source.market_data_daily_adjusted
        WHERE instrument = %s
          AND adjusted_close > 0;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (benchmark,))
            result = cur.fetchone()[0]

    if result is None:
        raise ValueError(f"No adjusted prices found for {benchmark}.")

    return result


def get_equity_exposures(as_of_date: date) -> pd.DataFrame:
    """Retrieve current long equity positions and market values.

    Equity market value excludes dividend receivables.
    """

    query = """
        WITH latest_positions AS (
            SELECT DISTINCT ON (instrument)
                instrument,
                date,
                eod_qty
            FROM core.position_qty_eod_v
            WHERE date <= %s
            ORDER BY instrument, date DESC
        )
        SELECT
            p.instrument,
            p.eod_qty AS shares,
            m.price_date,
            m.close_price
        FROM latest_positions p
        LEFT JOIN LATERAL (
            SELECT price_date, close_price
            FROM source.market_data_daily_adjusted m
            WHERE m.instrument = p.instrument
              AND m.price_date <= %s
            ORDER BY m.price_date DESC
            LIMIT 1
        ) m ON TRUE
        WHERE p.eod_qty > 0
        ORDER BY p.instrument;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (as_of_date, as_of_date))
            rows = cur.fetchall()

    exposures = pd.DataFrame(
        rows,
        columns=["instrument", "shares", "price_date", "close_price"],
    )

    if exposures.empty:
        raise ValueError("No open equity positions found.")

    exposures["shares"] = pd.to_numeric(exposures["shares"])
    exposures["close_price"] = pd.to_numeric(
        exposures["close_price"], errors="coerce"
    )

    missing = exposures[
        exposures["close_price"].isna()
        | exposures["price_date"].ne(as_of_date)
    ]

    if not missing.empty:
        raise ValueError(
            "Missing or stale closing prices for: "
            f"{missing['instrument'].tolist()}"
        )

    exposures["market_value"] = (
        exposures["shares"] * exposures["close_price"]
    )

    return exposures


def get_daily_return_panel(
    symbols: list[str],
    start_date: date,
    end_date: date,
    benchmark: str = "SPY",
) -> pd.DataFrame:
    """Return daily adjusted returns aligned to benchmark trading dates.

    Missing security prices are not forward-filled.
    A prior-price buffer preserves the first return in the window.
    """

    universe = sorted(set(symbols) | {benchmark})
    query_start = start_date - timedelta(days=15)

    query = """
        SELECT instrument, price_date, adjusted_close
        FROM source.market_data_daily_adjusted
        WHERE instrument = ANY(%s)
          AND price_date BETWEEN %s AND %s
        ORDER BY price_date, instrument;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (universe, query_start, end_date))
            rows = cur.fetchall()

    if not rows:
        raise ValueError("No adjusted market prices found.")

    prices = pd.DataFrame(
        rows,
        columns=["instrument", "price_date", "adjusted_close"],
    )

    prices["price_date"] = pd.to_datetime(prices["price_date"])
    prices["adjusted_close"] = pd.to_numeric(
        prices["adjusted_close"], errors="coerce"
    )

    panel = (
        prices.pivot(
            index="price_date",
            columns="instrument",
            values="adjusted_close",
        )
        .sort_index()
    )

    if benchmark not in panel.columns:
        raise ValueError(f"Missing benchmark: {benchmark}")

    for symbol in universe:
        if symbol not in panel.columns:
            panel[symbol] = float("nan")

    benchmark_calendar = panel[benchmark].dropna().index
    aligned_prices = panel.reindex(benchmark_calendar)

    returns = aligned_prices.pct_change(fill_method=None)

    return returns.loc[
        (returns.index >= pd.Timestamp(start_date))
        & (returns.index <= pd.Timestamp(end_date))
    ]
