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

def truncate_treasury_yields():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE treasury_yields RESTART IDENTITY;")
        conn.commit()
    logging.info("Truncated treasury_yields table.")

def fetch_treasury_yield(maturity: str = '10year', interval: str = 'monthly') -> pd.DataFrame:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TREASURY_YIELD",
        "maturity": maturity,
        "interval": interval,
        "apikey": ALPHAVANTAGE_API_KEY,
        "datatype": "json"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "data" not in data:
            raise ValueError(f"No yield data returned for {maturity}")

        df = pd.DataFrame(data["data"])
        df["yield_date"] = pd.to_datetime(df["date"])
        df["maturity"] = maturity
        df["yield_percent"] = pd.to_numeric(df["value"], errors="coerce")
        df["interval"] = interval
        df["source"] = "AlphaVantage"
        df = df[["yield_date", "maturity", "yield_percent", "interval", "source"]]

        return df

    except Exception as e:
        logging.error(f"Failed to fetch {maturity} yield: {e}")
        return pd.DataFrame()


def upsert_yields(df: pd.DataFrame):
    if df.empty:
        logging.warning("No data to insert.")
        return

    # Convert yield_date to native Python date
    df["yield_date"] = df["yield_date"].dt.date

    insert_query = """
        INSERT INTO treasury_yields (yield_date, maturity, yield_percent, interval, source)
        VALUES %s
        ON CONFLICT (yield_date, maturity)
        DO UPDATE SET
            yield_percent = EXCLUDED.yield_percent,
            interval = EXCLUDED.interval,
            source = EXCLUDED.source;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            records = df.to_records(index=False)
            execute_values(cur, insert_query, records)
        conn.commit()

    logging.info(f"Upserted {len(df)} rows into treasury_yields.")

def main():
    logging.info("Starting Treasury Yield ETL")
    truncate_treasury_yields()

    maturities = ['3month', '2year', '5year', '10year']
    all_data = []

    for maturity in maturities:
        logging.info(f"Fetching data for: {maturity}")
        df = fetch_treasury_yield(maturity, interval='monthly')
        if not df.empty:
            all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data)
        upsert_yields(combined_df)
    else:
        logging.warning("No yield data retrieved for any maturity.")


if __name__ == "__main__":
    main()