#!/usr/bin/env python3

"""
Ingest Alpha Vantage income-statement data into source.income_statements.

Routine refreshes derive their universe from currently open portfolio positions.
Historical rebuilds derive their universe from transaction history.

Income-statement data is persisted through upserts. This module does not
truncate existing history.
"""

import os
import time
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

import psycopg
import requests
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


SLEEP_BETWEEN_CALLS_SECONDS = 15

SYMBOL_MAP = {
    "FB": "META",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# ------------------------------------------------------------------------------
# ALPHA VANTAGE FIELD MAPPING
# ------------------------------------------------------------------------------

ALPHA_FIELDS = {
    "fiscalDateEnding": "fiscal_date",
    "reportedCurrency": "reported_currency",

    "totalRevenue": "total_revenue",
    "costOfRevenue": "cost_of_revenue",
    "costofGoodsAndServicesSold": "cost_of_goods_and_services_sold",
    "grossProfit": "gross_profit",

    "operatingIncome": "operating_income",
    "operatingExpenses": "operating_expenses",
    "researchAndDevelopment": "research_and_development",
    "sellingGeneralAndAdministrative": "selling_general_and_administrative",

    "depreciation": "depreciation",
    "depreciationAndAmortization": "depreciation_and_amortization",
    "ebit": "ebit",
    "ebitda": "ebitda",

    "incomeBeforeTax": "income_before_tax",
    "incomeTaxExpense": "income_tax_expense",
    "netIncome": "net_income",
    "netIncomeFromContinuingOperations": "net_income_from_continuing_operations",
    "comprehensiveIncomeNetOfTax": "comprehensive_income_net_of_tax",

    "interestExpense": "interest_expense",
    "interestIncome": "interest_income",
    "interestAndDebtExpense": "interest_and_debt_expense",
    "netInterestIncome": "net_interest_income",
    "investmentIncomeNet": "investment_income_net",
    "nonInterestIncome": "non_interest_income",
    "otherNonOperatingIncome": "other_non_operating_income",
}


# ------------------------------------------------------------------------------
# DATABASE
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


def get_active_symbols() -> list[str]:
    """
    Return the current portfolio-security universe.

    A security is active when its latest end-of-day portfolio quantity
    is non-zero.
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

    symbols = [row[0] for row in rows]

    return sorted(
        set(SYMBOL_MAP.get(symbol, symbol) for symbol in symbols)
    )


def get_historical_symbols() -> list[str]:
    """
    Return securities that have appeared in historical portfolio activity.

    Used for historical reconciliation and backfills.
    """

    query = """
        SELECT DISTINCT normalized_instrument
        FROM stage.txn_classified_v
        WHERE raw_trans_code IN ('Buy', 'Sell', 'REC')
          AND normalized_instrument IS NOT NULL
          AND normalized_instrument <> 'CASH';
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    symbols = [row[0] for row in rows]

    return sorted(
        set(SYMBOL_MAP.get(symbol, symbol) for symbol in symbols)
    )


# ------------------------------------------------------------------------------
# ALPHA VANTAGE FETCH
# ------------------------------------------------------------------------------

def fetch_income_data(symbol: str) -> dict | None:
    """Fetch annual and quarterly income statements from Alpha Vantage."""

    url = "https://www.alphavantage.co/query"

    params = {
        "function": "INCOME_STATEMENT",
        "symbol": symbol,
        "apikey": ALPHAVANTAGE_API_KEY,
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

    except Exception as exc:
        logging.error(
            "Request error for %s: %s",
            symbol,
            exc,
        )
        return None

    if "Error Message" in payload:
        logging.error(
            "Alpha Vantage error for %s: %s",
            symbol,
            payload["Error Message"],
        )
        return None

    if "Note" in payload:
        raise RuntimeError(
            f"Alpha Vantage throttling: {payload['Note']}"
        )

    if "Information" in payload:
        logging.warning(
            "Alpha Vantage information for %s: %s",
            symbol,
            payload["Information"],
        )
        return None

    return payload


# ------------------------------------------------------------------------------
# NORMALIZATION
# ------------------------------------------------------------------------------

def parse_numeric(value) -> Decimal | None:
    """Convert an Alpha Vantage numeric value to Decimal when possible."""

    if value in (None, "", "None"):
        return None

    try:
        parsed = Decimal(str(value))

        if not parsed.is_finite():
            return None

        return parsed

    except (InvalidOperation, ValueError):
        return None


def normalize_reports(
    symbol: str,
    reports: list[dict],
    frequency: str,
) -> list[dict]:
    """
    Normalize Alpha Vantage income-statement reports for database loading.
    """

    normalized = []

    for report in reports:
        fiscal_date_raw = report.get("fiscalDateEnding")

        if not fiscal_date_raw:
            logging.warning(
                "%s: skipping %s report with no fiscalDateEnding",
                symbol,
                frequency,
            )
            continue

        try:
            fiscal_date = date.fromisoformat(fiscal_date_raw)
        except ValueError:
            logging.warning(
                "%s: invalid fiscal date %s; skipping report",
                symbol,
                fiscal_date_raw,
            )
            continue

        reported_currency_raw = report.get("reportedCurrency")

        reported_currency = (
            None
            if reported_currency_raw in (None, "", "None")
            else reported_currency_raw
        )

        row = {
            "instrument": symbol,
            "frequency": frequency,
            "fiscal_date": fiscal_date,
            "reported_currency": reported_currency,
        }

        for api_key, db_key in ALPHA_FIELDS.items():

            if db_key in ("fiscal_date", "reported_currency"):
                continue

            row[db_key] = parse_numeric(
                report.get(api_key)
            )

        normalized.append(row)

    return normalized


# ------------------------------------------------------------------------------
# LOAD
# ------------------------------------------------------------------------------

def upsert_income_statements(records: list[dict]) -> None:
    """
    Upsert normalized income statements into source.income_statements.

    Existing statement periods are refreshed in place while new periods
    are inserted.
    """

    if not records:
        return

    columns = list(records[0].keys())

    column_sql = ", ".join(columns)

    placeholders = ", ".join(
        f"%({column})s"
        for column in columns
    )

    update_columns = [
        column
        for column in columns
        if column not in (
            "instrument",
            "fiscal_date",
            "frequency",
        )
    ]

    update_sql = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in update_columns
    )

    sql = f"""
        INSERT INTO source.income_statements (
            {column_sql}
        )
        VALUES (
            {placeholders}
        )
        ON CONFLICT (
            instrument,
            fiscal_date,
            frequency
        )
        DO UPDATE SET
            {update_sql},
            updated_at = now();
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                sql,
                records,
            )

    logging.info(
        "Upserted %d income-statement records.",
        len(records),
    )


# ------------------------------------------------------------------------------
# SYMBOL PROCESSING
# ------------------------------------------------------------------------------

def process_symbol(symbol: str) -> int:
    """Fetch, normalize, and persist income statements for one symbol."""

    payload = fetch_income_data(symbol)

    if not payload:
        return 0

    annual = normalize_reports(
        symbol,
        payload.get("annualReports", []),
        "annual",
    )

    quarterly = normalize_reports(
        symbol,
        payload.get("quarterlyReports", []),
        "quarterly",
    )

    records = annual + quarterly

    if not records:
        logging.warning(
            "%s: no income-statement records returned",
            symbol,
        )
        return 0

    upsert_income_statements(records)

    return len(records)


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

def main(mode: str = "refresh") -> None:
    """
    Run the Alpha Vantage income-statement ingestion pipeline.

    refresh:
        Update income statements for currently held portfolio securities.

    rebuild:
        Update income statements for the full historical portfolio universe.
    """

    logging.info(
        "Starting Alpha Vantage income-statement ingestion. Mode=%s",
        mode,
    )

    if mode == "refresh":
        symbols = get_active_symbols()

    elif mode == "rebuild":
        symbols = get_historical_symbols()

    else:
        raise ValueError(
            "mode must be either 'refresh' or 'rebuild'"
        )

    logging.info(
        "%s symbols to process: %d",
        mode.capitalize(),
        len(symbols),
    )

    total_records = 0

    for i, symbol in enumerate(
        symbols,
        start=1,
    ):
        try:
            logging.info(
                "[%d/%d] Processing %s",
                i,
                len(symbols),
                symbol,
            )

            total_records += process_symbol(symbol)

            time.sleep(
                SLEEP_BETWEEN_CALLS_SECONDS
            )

        except RuntimeError as exc:
            logging.error(
                "Throttle hit while processing %s: %s",
                symbol,
                exc,
            )

            logging.info(
                "Sleeping 60 seconds then continuing..."
            )

            time.sleep(60)

        except Exception as exc:
            logging.error(
                "Error processing %s: %s",
                symbol,
                exc,
            )

            time.sleep(
                SLEEP_BETWEEN_CALLS_SECONDS
            )

    logging.info(
        "Done. Upserted %d total income-statement records.",
        total_records,
    )


# ------------------------------------------------------------------------------
# USAGE
# ------------------------------------------------------------------------------

# Routine refresh:
#   python -m investment_analytics.ingestion.ingest_income_statements_alpha
#
# Historical rebuild / reconciliation:
#   python -c "from investment_analytics.ingestion.ingest_income_statements_alpha import main; main('rebuild')"
#
# refresh:
#   Current holdings only.
#
# rebuild:
#   Full historical portfolio universe.


if __name__ == "__main__":
    main()