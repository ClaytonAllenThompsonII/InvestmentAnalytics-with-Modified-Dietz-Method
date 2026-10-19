#!/usr/bin/env python3
"""Ingest EOD option quotes from Alpha Vantage for positions held in the portfolio.

Use --limit 0 to fetch all missing prices, or --refresh to refetch stored prices.
Routine runs register new standard contracts; unusual adjustments require review.
See USAGE below. Never truncates source or core data.
"""

import argparse
import logging
import os
import re
import time
from datetime import date
from decimal import Decimal, InvalidOperation

import requests
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from investment_analytics.data_access.database import get_connection


load_dotenv()
LOG = logging.getLogger(__name__)
ENDPOINT = "https://www.alphavantage.co/query"
PROVIDER = "alpha_vantage"
CONTRACT_PATTERN = re.compile(r"^([A-Z0-9.]+)(\d{6})([CP])(\d{8})$")

# Run once at startup. Materializing the required EOD holdings in this query
# avoids recomputing the view chain separately for each API request.
WORKLIST_SQL = """
WITH required AS MATERIALIZED (
    SELECT DISTINCT
        option_contract_id,
        date AS price_date
    FROM core.option_position_qty_calendar_v
    WHERE eod_qty <> 0
), resolved AS (
    SELECT
        r.option_contract_id,
        r.price_date,
        COUNT(s.option_contract_symbol_id) AS mapping_count,
        MIN(s.contract_symbol) AS contract_symbol
    FROM required r
    LEFT JOIN core.option_contract_symbols s
      ON s.option_contract_id = r.option_contract_id
     AND s.source_system = 'alpha_vantage'
     AND (s.valid_from IS NULL OR r.price_date >= s.valid_from)
     AND (s.valid_to IS NULL OR r.price_date <= s.valid_to)
    GROUP BY r.option_contract_id, r.price_date
)
SELECT
    r.option_contract_id,
    r.price_date,
    r.contract_symbol,
    r.mapping_count,
    (m.option_market_data_id IS NOT NULL) AS already_stored
FROM resolved r
LEFT JOIN source.option_market_data_daily m
  ON m.data_source = 'alpha_vantage'
 AND m.contract_symbol = r.contract_symbol
 AND m.price_date = r.price_date
ORDER BY r.price_date, r.option_contract_id;
"""

# Bootstrap only *standard* new option contracts. Canonical normalization is
# defined by core.option_transaction_events_v (currently includes FB -> META).
# Adjusted contracts require human review rather than guessed identifiers.
REGISTER_CONTRACTS_SQL = """
INSERT INTO core.option_contracts (
    underlying_symbol, expiration_date, option_type, strike_price
)
SELECT DISTINCT
    e.canonical_underlying_symbol,
    e.expiration_date,
    e.option_type,
    e.strike_price
FROM core.option_transaction_events_v e
WHERE e.option_contract_id IS NULL
  AND e.canonical_underlying_symbol IS NOT NULL
  AND e.expiration_date IS NOT NULL
  AND e.option_type IN ('call', 'put')
  AND e.strike_price > 0
ON CONFLICT DO NOTHING;
"""

# Generate candidate OCC-style IDs for required EOD dates, retaining historical
# FB/META symbol boundaries. They remain unverified until the API confirms them.
REGISTER_SYMBOLS_SQL = """
WITH required AS MATERIALIZED (
    SELECT DISTINCT
        p.option_contract_id,
        p.date AS price_date,
        c.underlying_symbol,
        c.expiration_date,
        c.option_type,
        c.strike_price
    FROM core.option_position_qty_calendar_v p
    JOIN core.option_contracts c
      ON c.option_contract_id = p.option_contract_id
    WHERE p.eod_qty <> 0
      AND c.is_adjusted = FALSE
), symbol_candidates AS (
    SELECT DISTINCT
        option_contract_id,
        CASE
            WHEN underlying_symbol = 'META' AND price_date < DATE '2022-06-09'
                THEN 'FB'
            ELSE underlying_symbol
        END AS historical_symbol,
        expiration_date,
        option_type,
        strike_price,
        CASE
            WHEN underlying_symbol = 'META' AND price_date >= DATE '2022-06-09'
                THEN DATE '2022-06-09'
            ELSE NULL::date
        END AS valid_from,
        CASE
            WHEN underlying_symbol = 'META' AND price_date < DATE '2022-06-09'
                THEN DATE '2022-06-08'
            ELSE NULL::date
        END AS valid_to
    FROM required
)
INSERT INTO core.option_contract_symbols (
    option_contract_id, contract_symbol, source_system,
    valid_from, valid_to, is_verified, verified_price_date
)
SELECT
    option_contract_id,
    historical_symbol
        || TO_CHAR(expiration_date, 'YYMMDD')
        || CASE WHEN option_type = 'call' THEN 'C' ELSE 'P' END
        || LPAD(((strike_price * 1000)::BIGINT)::TEXT, 8, '0'),
    'alpha_vantage',
    valid_from,
    valid_to,
    FALSE,
    NULL::date
FROM symbol_candidates
WHERE historical_symbol ~ '^[A-Z0-9.]+$'
  AND strike_price * 1000 = TRUNC(strike_price * 1000)
  AND strike_price * 1000 BETWEEN 1 AND 99999999
ON CONFLICT (source_system, contract_symbol) DO NOTHING;
"""

# The known 2022 FB -> META conversion was manually verified against OCC and
# the original Robinhood records. New OCA events must be examined separately.
UNREVIEWED_ADJUSTMENTS_SQL = """
SELECT COUNT(*)
FROM stage.option_transaction_events_v e
WHERE e.raw_trans_code = 'OCA'
  AND NOT (
      e.event_date = DATE '2022-06-09'
      AND e.instrument IN ('FB', 'META')
      AND e.expiration_date = DATE '2022-07-08'
      AND e.option_type = 'put'
      AND e.strike_price IN (190, 200)
  );
"""

UPSERT_SQL = """
INSERT INTO source.option_market_data_daily (
    data_source, contract_symbol, underlying_symbol, price_date,
    expiration_date, option_type, strike_price,
    last_price, mark_price, bid_price, ask_price,
    bid_size, ask_size, volume, open_interest,
    implied_volatility, delta, gamma, theta, vega, rho,
    raw_payload
) VALUES (
    'alpha_vantage', %(contract_symbol)s, %(underlying_symbol)s, %(price_date)s,
    %(expiration_date)s, %(option_type)s, %(strike_price)s,
    %(last_price)s, %(mark_price)s, %(bid_price)s, %(ask_price)s,
    %(bid_size)s, %(ask_size)s, %(volume)s, %(open_interest)s,
    %(implied_volatility)s, %(delta)s, %(gamma)s, %(theta)s, %(vega)s, %(rho)s,
    %(raw_payload)s
)
ON CONFLICT (data_source, contract_symbol, price_date)
DO UPDATE SET
    underlying_symbol = EXCLUDED.underlying_symbol,
    expiration_date = EXCLUDED.expiration_date,
    option_type = EXCLUDED.option_type,
    strike_price = EXCLUDED.strike_price,
    last_price = EXCLUDED.last_price,
    mark_price = EXCLUDED.mark_price,
    bid_price = EXCLUDED.bid_price,
    ask_price = EXCLUDED.ask_price,
    bid_size = EXCLUDED.bid_size,
    ask_size = EXCLUDED.ask_size,
    volume = EXCLUDED.volume,
    open_interest = EXCLUDED.open_interest,
    implied_volatility = EXCLUDED.implied_volatility,
    delta = EXCLUDED.delta,
    gamma = EXCLUDED.gamma,
    theta = EXCLUDED.theta,
    vega = EXCLUDED.vega,
    rho = EXCLUDED.rho,
    raw_payload = EXCLUDED.raw_payload,
    retrieved_at = NOW();
"""


def optional_decimal(value):
    """Keep valid provider numbers, and map '-', blanks and NaN to NULL."""
    if value is None or str(value).strip().lower() in {"", "-", "none", "null", "n/a"}:
        return None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def optional_integer(value):
    value = optional_decimal(value)
    if value is None or value < 0 or value != value.to_integral_value():
        return None
    return int(value)


def sync_contract_catalog(*, dry_run=False):
    """Register ordinary new contracts and dated symbol candidates, idempotently.

    A dry run only reports unresolved activity and does not modify PostgreSQL.
    New corporate-action adjustments cannot be inferred safely from basic terms.
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(UNREVIEWED_ADJUSTMENTS_SQL)
        unreviewed = cur.fetchone()[0]
        if unreviewed:
            raise RuntimeError(
                f"Aborting: {unreviewed} unreviewed OCA adjustment event(s). "
                "Review contract identity and deliverables before ingesting."
            )

        cur.execute("SELECT MAX(market_date) FROM core.market_calendar_v")
        latest_market_date = cur.fetchone()[0]
        cur.execute("SELECT MAX(event_date) FROM stage.option_transaction_events_v")
        latest_option_event = cur.fetchone()[0]
        if latest_option_event and (
            latest_market_date is None or latest_option_event > latest_market_date
        ):
            raise RuntimeError(
                f"Options transactions extend through {latest_option_event}, "
                f"but the market calendar ends {latest_market_date}. "
                "Refresh equity market data before options ingestion."
            )
        if latest_market_date and (date.today() - latest_market_date).days > 5:
            LOG.warning(
                "Market calendar last date is %s; refresh equity market data "
                "before relying on current option coverage.", latest_market_date,
            )

        cur.execute("""
            SELECT COUNT(*) FROM core.option_transaction_events_v
            WHERE option_contract_id IS NULL
        """)
        unresolved_before = cur.fetchone()[0]

        if dry_run:
            if unresolved_before:
                LOG.warning(
                    "Dry run: %d unresolved option event(s); standard contract "
                    "registration is skipped in dry-run mode. Run without "
                    "--dry-run before previewing new prices.", unresolved_before,
                )
            return unresolved_before

        cur.execute(REGISTER_CONTRACTS_SQL)
        new_contracts = cur.rowcount
        cur.execute("""
            SELECT COUNT(*) FROM core.option_transaction_events_v
            WHERE option_contract_id IS NULL
        """)
        unresolved_after = cur.fetchone()[0]
        if unresolved_after:
            raise RuntimeError(
                f"Aborting: {unresolved_after} option events still have no "
                "canonical contract after standard registration. "
                "Review adjusted/nonstandard contract terms."
            )

        cur.execute(REGISTER_SYMBOLS_SQL)
        new_symbols = cur.rowcount
        if new_contracts or new_symbols:
            LOG.info(
                "Contract catalog: registered %d new contract(s) and %d "
                "candidate Alpha Vantage symbol(s).",
                new_contracts, new_symbols,
            )

        # The psycopg connection context commits on a clean exit.
        return 0


def load_worklist():
    started = time.monotonic()
    with get_connection() as conn, conn.cursor() as cur:
        # A new Robinhood contract must be registered before prices can be
        # scheduled. Otherwise unresolved events disappear from the holdings
        # worklist, and a refresh could incorrectly report zero pending rows.
        cur.execute("""
            SELECT COUNT(*)
            FROM core.option_transaction_events_v
            WHERE option_contract_id IS NULL
        """)
        unresolved = cur.fetchone()[0]
        if unresolved:
            raise RuntimeError(
                f"Aborting: {unresolved} option events have no canonical contract. "
                "Populate core.option_contracts before options ingestion."
            )
        cur.execute(WORKLIST_SQL)
        rows = cur.fetchall()
    LOG.info("Loaded %d required contract-date rows in %.1fs", len(rows), time.monotonic() - started)
    invalid = [r for r in rows if r[3] != 1]
    if invalid:
        raise RuntimeError(
            f"Aborting: {len(invalid)} contract-date rows have missing/ambiguous symbol mappings. "
            f"First: contract_id={invalid[0][0]}, date={invalid[0][1]}, matches={invalid[0][3]}"
        )
    return rows


def fetch_one(session, contract_symbol, price_date, api_key):
    match = CONTRACT_PATTERN.fullmatch(contract_symbol)
    if not match:
        raise ValueError(f"Unsupported contract symbol: {contract_symbol}")
    api_underlying = match.group(1)  # FB before 2022-06-09; META afterward
    response = session.get(
        ENDPOINT,
        params={
            "function": "HISTORICAL_OPTIONS",
            "symbol": api_underlying,
            "date": price_date.isoformat(),
            "contract": contract_symbol,
            "apikey": api_key,
        },
        timeout=60,
    )
    # Do not print response.url or the raw request exception: both can expose a key.
    if response.status_code == 429:
        raise RuntimeError("RATE_LIMIT")
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("Invalid JSON response") from exc
    for key in ("Information", "Note"):
        if key in payload:
            # Provider informational messages can indicate exhausted API limits;
            # halt rather than wasting requests on the remaining worklist.
            raise RuntimeError("RATE_LIMIT")
    if "Error Message" in payload:
        raise RuntimeError("Alpha Vantage returned Error Message")
    records = payload.get("data")
    if not isinstance(records, list):
        raise RuntimeError("Response has no data array")
    matches = [
        row for row in records
        if row.get("contractID") == contract_symbol
        and row.get("date") == price_date.isoformat()
    ]
    if len(matches) > 1:
        raise RuntimeError("Duplicate matching records in API response")
    return matches[0] if matches else None


def normalize_record(raw, expected_symbol, expected_date):
    if raw.get("contractID") != expected_symbol or raw.get("date") != expected_date.isoformat():
        raise ValueError("API record identity does not match request")
    option_type = str(raw.get("type", "")).lower()
    strike = optional_decimal(raw.get("strike"))
    if option_type not in {"put", "call"} or strike is None or strike <= 0:
        raise ValueError("Invalid option type or strike")
    match = CONTRACT_PATTERN.fullmatch(expected_symbol)
    expiration = date.fromisoformat(raw["expiration"])
    symbol_expiration = date.fromisoformat("20" + match.group(2)[:2] + "-" + match.group(2)[2:4] + "-" + match.group(2)[4:])
    symbol_strike = Decimal(match.group(4)) / 1000
    if (
        raw.get("symbol") != match.group(1)
        or expiration != symbol_expiration
        or option_type != {"C": "call", "P": "put"}[match.group(3)]
        or strike != symbol_strike
    ):
        raise ValueError("Provider contract terms disagree with requested identifier")
    return {
        "contract_symbol": expected_symbol,
        "underlying_symbol": raw["symbol"],
        "price_date": expected_date,
        "expiration_date": expiration,
        "option_type": option_type,
        "strike_price": strike,
        "last_price": optional_decimal(raw.get("last")),
        "mark_price": optional_decimal(raw.get("mark")),
        "bid_price": optional_decimal(raw.get("bid")),
        "ask_price": optional_decimal(raw.get("ask")),
        "bid_size": optional_integer(raw.get("bid_size")),
        "ask_size": optional_integer(raw.get("ask_size")),
        "volume": optional_integer(raw.get("volume")),
        "open_interest": optional_integer(raw.get("open_interest")),
        "implied_volatility": optional_decimal(raw.get("implied_volatility")),
        "delta": optional_decimal(raw.get("delta")),
        "gamma": optional_decimal(raw.get("gamma")),
        "theta": optional_decimal(raw.get("theta")),
        "vega": optional_decimal(raw.get("vega")),
        "rho": optional_decimal(raw.get("rho")),
        "raw_payload": Jsonb(raw),
    }


def upsert_one(row):
    # Each successful observation commits independently so interruptions are safe.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=5, help="Max requests (default 5; 0 for all)")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Pause between API requests (default 1 second)")
    parser.add_argument("--contract", help="Optional Alpha Vantage contract identifier")
    parser.add_argument("--date-from", type=date.fromisoformat)
    parser.add_argument("--date-to", type=date.fromisoformat)
    parser.add_argument("--refresh", action="store_true", help="Refetch existing observations")
    parser.add_argument("--dry-run", action="store_true", help="Preview worklist without calling API")
    args = parser.parse_args()
    if args.limit < 0 or args.sleep_seconds < 0:
        parser.error("--limit and --sleep-seconds must be nonnegative")
    api_key = os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")
    if not api_key and not args.dry_run:
        parser.error("ALPHAVANTAGE_PREMIUM_API_KEY is missing")

    pending_registrations = sync_contract_catalog(dry_run=args.dry_run)
    if args.dry_run and pending_registrations:
        LOG.warning("Worklist preview omitted: register new contracts in a normal run first.")
        return

    worklist = load_worklist()
    pending = [
        (contract_id, price_date, symbol)
        for contract_id, price_date, symbol, mapping_count, stored in worklist
        if (args.refresh or not stored)
        and (args.contract is None or symbol == args.contract)
        and (args.date_from is None or price_date >= args.date_from)
        and (args.date_to is None or price_date <= args.date_to)
    ]
    total_pending = len(pending)
    batch = pending[:args.limit] if args.limit else pending
    LOG.info("Pending: %d; selected this run: %d", total_pending, len(batch))
    if args.dry_run:
        for contract_id, price_date, symbol in batch:
            print(f"contract_id={contract_id} date={price_date} symbol={symbol}")
        return

    success = no_data = failed = missing_mark = 0
    with requests.Session() as session:
        for idx, (contract_id, price_date, symbol) in enumerate(batch, start=1):
            try:
                raw = fetch_one(session, symbol, price_date, api_key)
                if raw is None:
                    no_data += 1
                    LOG.warning("[%d/%d] NOT FOUND %s %s", idx, len(batch), symbol, price_date)
                else:
                    row = normalize_record(raw, symbol, price_date)
                    upsert_one(row)
                    success += 1
                    if row["mark_price"] is None:
                        missing_mark += 1
                    LOG.info(
                        "[%d/%d] STORED %s %s mark=%s",
                        idx, len(batch), symbol, price_date, row["mark_price"],
                    )
            except requests.RequestException as exc:
                failed += 1
                LOG.error("[%d/%d] REQUEST ERROR %s %s (%s)", idx, len(batch), symbol, price_date, type(exc).__name__)
            except Exception as exc:
                failed += 1
                if str(exc) == "RATE_LIMIT":
                    LOG.error("Rate limit reached. Stop now; retry later with slower pacing.")
                    break
                LOG.error("[%d/%d] ERROR %s %s (%s)", idx, len(batch), symbol, price_date, str(exc))
            if idx < len(batch):
                time.sleep(args.sleep_seconds)

    LOG.info(
        "Finished: stored=%d not_found=%d failed=%d stored_without_mark=%d",
        success, no_data, failed, missing_mark,
    )
    if no_data or failed:
        LOG.warning("Missing/error observations remain pending and will be retried on a later run.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()



# ------------------------------------------------------------------------------
# USAGE
# ------------------------------------------------------------------------------
# Run from repository root with virtual environment active.
# Requires ALPHAVANTAGE_PREMIUM_API_KEY in .env.
#
# Prerequisites:
#   Import latest Robinhood transactions and refresh equity market data.
#   New standard option contracts and symbols are registered automatically.
#
# 1. Dry Run — Preview pending observations without API calls or database writes.
#    PYTHONPATH=src python -m investment_analytics.ingestion.market_data.ingest_option_market_data_alpha --dry-run --limit 0
#
# 2. Refresh (Routine) — Ingest missing historical and current option prices.
#    PYTHONPATH=src python -m investment_analytics.ingestion.market_data.ingest_option_market_data_alpha --limit 0
#
# 3. Rebuild — Re-fetch and update all required historical option prices.
#    PYTHONPATH=src python -m investment_analytics.ingestion.market_data.ingest_option_market_data_alpha --refresh --limit 0
#
# Options:
#   --limit 5          Limit requests to 5 (default); 0 processes all.
#   --sleep-seconds 1  Delay between API calls (default: 1 second).
#   --contract         Filter by specific option contract.
#   --date-from        Filter starting date (YYYY-MM-DD).
#   --date-to          Filter ending date (YYYY-MM-DD).
#
# Notes:
#   - Existing observations are skipped unless --refresh is specified.
#   - Missing observations remain pending for future runs.
#   - Unusual corporate actions require manual review.
#   - No database tables are truncated.
