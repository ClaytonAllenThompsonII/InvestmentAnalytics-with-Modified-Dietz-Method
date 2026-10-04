#!/usr/bin/env python3
"""
Ingest Alpha Vantage daily adjusted market data into
source.market_data_daily_adjusted.

Routine refreshes derive their equity universe from currently open portfolio
positions and configured benchmarks. Historical transaction-derived symbols
remain available for backfills and reconciliation. Market data is upserted
without truncating existing history.
"""

import os
import time
import logging
from datetime import date
from typing import Optional, Dict, List, Tuple

import requests
import pandas as pd
import psycopg
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

load_dotenv()

ALPHAVANTAGE_API_KEY = (
    os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")
    or os.getenv("ALPHAVANTAGE_API_KEY")
)

if not ALPHAVANTAGE_API_KEY:
    raise ValueError(
        "Missing Alpha Vantage API key. "
        "Set ALPHAVANTAGE_API_KEY or ALPHAVANTAGE_PREMIUM_API_KEY in .env"
    )

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Portfolio inception date floor
INCEPTION_DATE = date(2020, 7, 6)

# If your last stored price_date is older than this, COMPACT may not cover the gap -> use FULL.
# COMPACT is ~100 trading days, so 120 calendar days is a safe heuristic.
COMPACT_COVERAGE_DAYS = 120

# Throttle to respect AV rate limits. (Free is typically 5 calls/min; premium higher.)
SLEEP_BETWEEN_CALLS_SECONDS = 15

# Symbol remap (keep your canonical tickers)
SYMBOL_MAP = {"FB": "META"}

# Benchmarks you want always
BENCHMARKS = ["SPY", "QQQ", "XLK", "XLY", "EEM", "IXUS"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# ------------------------------------------------------------------------------
# DB HELPERS
# ------------------------------------------------------------------------------

def get_connection() -> psycopg.Connection:
    """Create a Psycopg connection to the PostgreSQL database."""
    return psycopg.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

# Routine refresh universe
def get_active_symbols() -> List[str]:
    """
    Return symbols requiring routine market-data refresh:
    currently open equity positions plus configured benchmarks.
    """
    query = """
    WITH latest_position AS (
        SELECT DISTINCT ON (instrument)
            instrument,
            date,
            eod_qty
        FROM core.position_qty_eod_v
        ORDER BY instrument, date DESC
    )
    SELECT instrument
    FROM latest_position
    WHERE eod_qty <> 0;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    symbols = [r[0] for r in rows]
    all_syms = sorted(set(symbols + BENCHMARKS))

    return sorted(set(SYMBOL_MAP.get(s, s) for s in all_syms))

# Historical/backfill universe
def get_historical_symbols() -> List[str]:
    """
    Return the full historical portfolio universe plus benchmarks.

    Used for backfills, reconciliation, and repairing incomplete
    historical market-data coverage.
    """
    query = """
    SELECT DISTINCT normalized_instrument
    FROM stage.txn_classified_v
    WHERE txn_bucket = 'equity_trade'
      AND normalized_instrument IS NOT NULL
      AND normalized_instrument <> 'CASH';
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    symbols = [r[0] for r in rows]
    all_syms = sorted(set(symbols + BENCHMARKS))

    return sorted(set(SYMBOL_MAP.get(s, s) for s in all_syms))

def get_symbol_state(symbol: str) -> Dict[str, Optional[date]]:
    """
    Return latest stored dates for this symbol.
    """
    query = """
    SELECT
      MAX(price_date) AS max_price_date,
      MAX(last_refreshed_date) AS max_last_refreshed
    FROM source.market_data_daily_adjusted
    WHERE instrument = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            row = cur.fetchone()

    return {
        "max_price_date": row[0],
        "max_last_refreshed_date": row[1],
    }


def upsert_market_data(records: List[Tuple]):
    """
    Upsert daily market-data records into source.market_data_daily_adjusted.

    Existing instrument/date rows are updated; new rows are inserted.
    """
    if not records:
        return
  
    sql = """
        INSERT INTO source.market_data_daily_adjusted (
            instrument,
            price_date,
            open_price,
            high_price,
            low_price,
            close_price,
            adjusted_close,
            volume,
            dividend_amount,
            split_coefficient,
            last_refreshed_date
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (instrument, price_date)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume,
            dividend_amount = EXCLUDED.dividend_amount,
            split_coefficient = EXCLUDED.split_coefficient,
            last_refreshed_date = EXCLUDED.last_refreshed_date,
            updated_at = now();
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)


# ------------------------------------------------------------------------------
# ALPHA VANTAGE FETCH
# ------------------------------------------------------------------------------

def choose_outputsize(symbol_state: Dict[str, Optional[date]]) -> str:
    """Choose a full or compact Alpha Vantage refresh."""
    max_date = symbol_state["max_price_date"]

    if max_date is None:
        return "full"

    gap_days = (date.today() - max_date).days

    if gap_days > COMPACT_COVERAGE_DAYS:
        return "full"
    
    return "compact"


def fetch_daily_adjusted(symbol: str, outputsize: str) -> Optional[pd.DataFrame]:
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={symbol}"
        f"&outputsize={outputsize}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
    )

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        logging.error("Request error for %s: %s", symbol, e)
        return None

    if "Error Message" in payload:
        logging.error("Alpha Vantage error for %s: %s", symbol, payload["Error Message"])
        return None

    if "Note" in payload:
        # Rate limit / throttling note
        raise RuntimeError(f"Alpha Vantage throttling: {payload['Note']}")

    meta = payload.get("Meta Data", {})
    ts_daily = payload.get("Time Series (Daily)", {})
    if not ts_daily:
        logging.warning("No Time Series (Daily) returned for %s", symbol)
        return None

    last_refreshed = meta.get("3. Last Refreshed")
    last_refreshed_date = pd.to_datetime(last_refreshed).date() if last_refreshed else None

    df = pd.DataFrame.from_dict(ts_daily, orient="index")
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    rename_map = {
        "1. open": "open_price",
        "2. high": "high_price",
        "3. low": "low_price",
        "4. close": "close_price",
        "5. adjusted close": "adjusted_close",
        "6. volume": "volume",
        "7. dividend amount": "dividend_amount",
        "8. split coefficient": "split_coefficient",
    }
    df.rename(columns=rename_map, inplace=True)

    # Ensure all expected cols exist
    for col in rename_map.values():
        if col not in df.columns:
            df[col] = None

    numeric_cols = [
        "open_price", "high_price", "low_price",
        "close_price", "adjusted_close",
        "dividend_amount", "split_coefficient",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    df["price_date"] = df.index.date
    df["last_refreshed_date"] = last_refreshed_date

    return df[
        [
            "price_date",
            "open_price", "high_price", "low_price", "close_price",
            "adjusted_close",
            "volume",
            "dividend_amount",
            "split_coefficient",
            "last_refreshed_date",
        ]
    ]


def filter_df_for_load(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the portfolio inception-date floor before loading market data.
    """
    df = df.copy()

    return df[df["price_date"] >= INCEPTION_DATE]


def df_to_records(symbol: str, df: pd.DataFrame) -> List[Tuple]:
    records: List[Tuple] = []
    for _, row in df.iterrows():
        # require key fields for valuation
        if pd.isna(row.get("price_date")) or pd.isna(row.get("adjusted_close")):
            continue

        volume_val = int(row["volume"]) if pd.notna(row["volume"]) else None

        records.append((
            symbol,
            row["price_date"],
            row.get("open_price"),
            row.get("high_price"),
            row.get("low_price"),
            row.get("close_price"),
            row.get("adjusted_close"),
            volume_val,
            row.get("dividend_amount"),
            row.get("split_coefficient"),
            row.get("last_refreshed_date"),
        ))
    return records


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

def main():
    logging.info("Starting market data ELT (v2). No truncation; upsert-only.")
    logging.info("Inception date floor: %s", INCEPTION_DATE.isoformat())

    symbols = get_active_symbols()
    logging.info("Active symbols to process: %d", len(symbols))

    for i, symbol in enumerate(symbols, start=1):
        try:
            state = get_symbol_state(symbol)

            outputsize = choose_outputsize(state)
            logging.info("[%d/%d] %s: fetching (%s). Current max price_date=%s",
                         i, len(symbols), symbol, outputsize, state["max_price_date"])

            df = fetch_daily_adjusted(symbol, outputsize=outputsize)
            if df is None or df.empty:
                logging.info("%s: no data returned; skipping", symbol)
                time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)
                continue

            df = filter_df_for_load(df)
            if df.empty:
                logging.info("%s: no market data on or after the inception date; skipping", symbol)
                time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)
                continue

            records = df_to_records(symbol, df)
            if not records:
                logging.info("%s: no valid records after parsing; skipping", symbol)
                time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)
                continue

            upsert_market_data(records)
            logging.info("%s: upserted %d rows (from %s to %s)",
                         symbol, len(records), df["price_date"].min(), df["price_date"].max())

            time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)

        except RuntimeError as e:
            # throttling note -> back off harder
            logging.error("Throttle hit: %s", e)
            logging.info("Sleeping 60 seconds then continuing...")
            time.sleep(60)

        except Exception as e:
            logging.error("Error processing %s: %s", symbol, e)
            # small sleep to be kind to AV even on errors
            time.sleep(SLEEP_BETWEEN_CALLS_SECONDS)

    logging.info("Done.")


if __name__ == "__main__":
    main()