import os
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import psycopg2

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Alpha Vantage API key
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing Alpha Vantage API key")

# DB connection params
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_holdings():
    query = """
        SELECT DISTINCT instrument
        FROM fifo_equity_lots
        WHERE open_quantity > 0;
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]


ALPHA_FIELDS = {
    "fiscalDateEnding": "fiscal_date",
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
    "otherNonOperatingIncome": "other_non_operating_income"
}

def truncate_income_statements():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE income_statements RESTART IDENTITY;")
        conn.commit()
    logging.info("Truncated income_statements table. Starting fresh load...")


def fetch_income_data(symbol):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "INCOME_STATEMENT",
        "symbol": symbol,
        "apikey": ALPHAVANTAGE_API_KEY
    }

    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None


def normalize_reports(symbol, reports, frequency):
    normalized = []
    for report in reports:
        row = {"instrument": symbol, "frequency": frequency}
        for api_key, db_key in ALPHA_FIELDS.items():
            value = report.get(api_key)
            if db_key == "fiscal_date":
                row[db_key] = pd.to_datetime(value) if value else None
            else:
                row[db_key] = float(value) if value not in (None, '', 'None') else None
        normalized.append(row)
    return normalized


def upsert_income_statements(records):
    if not records:
        return

    keys = records[0].keys()
    cols = ", ".join(keys)
    placeholders = ", ".join([f"%({k})s" for k in keys])
    update_clause = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in ('instrument', 'fiscal_date', 'frequency')])

    sql = f"""
        INSERT INTO income_statements ({cols})
        VALUES ({placeholders})
        ON CONFLICT (instrument, fiscal_date, frequency)
        DO UPDATE SET {update_clause};
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in records:
                cur.execute(sql, row)
        conn.commit()
    logging.info(f"Upserted {len(records)} income statement records.")


def process_symbol(symbol):
    data = fetch_income_data(symbol)
    if not data:
        return

    annual = normalize_reports(symbol, data.get("annualReports", []), "annual")
    quarterly = normalize_reports(symbol, data.get("quarterlyReports", []), "quarterly")
    upsert_income_statements(annual + quarterly)


def main():
    logging.info("Starting Alpha Vantage Income Statement ETL")
    truncate_income_statements()
    symbols = get_holdings()
    for symbol in symbols:
        logging.info(f"Processing {symbol}")
        process_symbol(symbol)


if __name__ == "__main__":
    main()