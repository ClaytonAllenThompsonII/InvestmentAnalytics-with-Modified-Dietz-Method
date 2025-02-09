import os
import math
from datetime import datetime
import psycopg2
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    """Create a psycopg2 connection to the Postgres database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def truncate_market_data():
    """
    Optional step: Truncate the market_data table to clear existing records
    and reset its primary key before loading new data.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE market_data RESTART IDENTITY;")
    conn.commit()
    cursor.close()
    conn.close()
    print("Truncated market_data table. Starting fresh...")


def get_distinct_instruments():
    """
    Query the transactions table to get distinct instruments (tickers).
    Exclude placeholders like 'CASH', 'DFEE', 'DTAX', 'GOLD', etc., if needed.
    Return a list of valid tickers (raw_instrument).
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    exclude_list = ["CASH", "DFEE", "DTAX", "GOLD", "OCA", ""]
    exclude_str  = "', '".join(exclude_list)

    query = f"""
        SELECT DISTINCT instrument
        FROM transactions
        WHERE instrument NOT IN ('{exclude_str}')
        ORDER BY instrument;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    instruments = [row[0] for row in results if row[0] is not None]
    return instruments


def get_min_date_for_instrument(raw_instrument):
    """
    Find the earliest activity_date in the transactions table for this raw_instrument.
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT MIN(activity_date)
        FROM transactions
        WHERE instrument = %s
    """
    cursor.execute(query, (raw_instrument,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result and result[0]:
        return result[0]
    return None


def upsert_market_data(records):
    """
    Insert or upsert the fetched Yahoo data into the market_data table.
    We'll do: 
      INSERT ... ON CONFLICT (instrument, price_date) DO UPDATE ...
    """
    upsert_sql = """
        INSERT INTO market_data (
            instrument, price_date,
            open_price, high_price, low_price, close_price, volume,
            currency, exchange
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument, price_date)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume,
            currency    = EXCLUDED.currency,
            exchange    = EXCLUDED.exchange;
    """

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(upsert_sql, records)
    conn.commit()
    cursor.close()
    conn.close()


def fetch_yahoo_data_for_instrument(mapped_instrument, start_date, end_date=None, interval='1d'):
    """
    Fetch daily data from Yahoo, then reduce it to a single row per month (last trading day).
    """
    if not end_date:
        end_date = datetime.today().strftime('%Y-%m-%d')

    ticker = yf.Ticker(mapped_instrument)
    df = ticker.history(start=start_date, end=end_date, interval=interval)

    if df.empty:
        print(f"No data returned for {mapped_instrument} from {start_date} to {end_date}.")
        return None

    # Reset index so 'Date' becomes a normal column.
    df.reset_index(inplace=True)
    # Rename columns to match our DB schema
    df.rename(columns={
        'Date': 'price_date',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Volume': 'volume'
    }, inplace=True, errors='ignore')

    # Convert to datetime
    df['price_date'] = pd.to_datetime(df['price_date'])

    # Group by year-month, pick the last row (represents last trading day each month)
    df['year_month'] = df['price_date'].dt.to_period('M')
    df_monthly = df.groupby('year_month').tail(1).copy()

    # Convert back to date for price_date
    df_monthly['price_date'] = df_monthly['price_date'].dt.date

    # Attach currency/exchange info from the Ticker object (if available)
    info = getattr(ticker, 'info', {})
    df_monthly['currency'] = info.get('currency')
    df_monthly['exchange'] = info.get('exchange')

    return df_monthly


def main():
    # Step #8 (Optional): Clean slate if you want a brand-new table each time.
    truncate_market_data()

    raw_instruments = get_distinct_instruments()
    symbol_map = {'FB': 'META'}

    for raw_instrument in raw_instruments:
        mapped_instrument = symbol_map.get(raw_instrument, raw_instrument)

        min_date = get_min_date_for_instrument(raw_instrument)
        if not min_date:
            continue

        df_yahoo = fetch_yahoo_data_for_instrument(
            mapped_instrument, 
            start_date=min_date,
            end_date=None,
            interval='1d'
        )

        if df_yahoo is None or df_yahoo.empty:
            continue

        # Build record tuples
        records = []
        for _, row in df_yahoo.iterrows():
            records.append((
                mapped_instrument,
                row['price_date'],
                row['open_price'],
                row['high_price'],
                row['low_price'],
                row['close_price'],
                None if pd.isna(row['volume']) else int(row['volume']),
                row['currency'],
                row['exchange']
            ))

        # Insert/update monthly rows
        upsert_market_data(records)
        print(f"Upserted {len(records)} monthly rows for {raw_instrument} -> {mapped_instrument}.")


if __name__ == "__main__":
    main()