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

def get_distinct_instruments():
    """
    Query the transactions table to get distinct instruments (tickers).
    Exclude placeholders like 'CASH', 'DFEE', 'DTAX', 'GOLD', etc., if needed.
    Return a list of valid tickers (raw_instrument).
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Remove 'None' from this list to avoid join() TypeError,
    # and only exclude actual placeholders or empty strings.
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

    # Flatten list of tuples
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
            open_price, high_price, low_price, close_price, adj_close, volume,
            currency, exchange
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument, price_date)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            adj_close   = EXCLUDED.adj_close,
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

def fetch_yahoo_data_for_instrument(mapped_instrument, start_date, end_date=None):
    """
    Fetch daily data from yfinance using the *mapped_instrument*.
    Return a DataFrame with columns: price_date, open_price, high_price, low_price, 
    close_price, adj_close, volume, plus currency & exchange from ticker.info.
    """
    if not end_date:
        end_date = datetime.today().strftime('%Y-%m-%d')

    ticker = yf.Ticker(mapped_instrument)
    df = ticker.history(start=start_date, end=end_date, interval='1d')

    if df.empty:
        print(f"No data returned for {mapped_instrument} from {start_date} to {end_date}.")
        return None

    # Reset index so 'Date' becomes a normal column
    df.reset_index(inplace=True)

    # Rename columns (some tickers might not have 'Adj Close', so we handle that)
    df.rename(columns={
        'Date': 'price_date',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Adj Close': 'adj_close',
        'Volume': 'volume'
    }, inplace=True, errors='ignore')

    # Ensure these columns exist, even if Yahoo didn't return them
    expected_cols = ['price_date', 'open_price', 'high_price',
                     'low_price', 'close_price', 'adj_close', 'volume']
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Retrieve extra info like currency and exchange
    info = getattr(ticker, 'info', {})
    currency = info.get('currency')
    exchange = info.get('exchange')

    # Convert 'price_date' to a pure date
    df['price_date'] = pd.to_datetime(df['price_date']).dt.date
    df['currency']   = currency if currency else None
    df['exchange']   = exchange if exchange else None

    return df

def main():
    # 1) Distinct raw_instruments from transactions
    raw_instruments = get_distinct_instruments()
    print("Raw instruments found:", raw_instruments)

    # A small dictionary mapping old/invalid symbols to current Yahoo symbols
    symbol_map = {
        'FB': 'META',
        # Add more as needed
    }

    for raw_instrument in raw_instruments:
        # Map from raw_instrument => valid Yahoo symbol
        mapped_instrument = symbol_map.get(raw_instrument, raw_instrument)

        # 2) Earliest date for *raw_instrument*
        min_date = get_min_date_for_instrument(raw_instrument)
        if not min_date:
            print(f"No min_date found for {raw_instrument}, skipping.")
            continue

        # 3) Fetch data using the mapped_instrument
        df_yahoo = fetch_yahoo_data_for_instrument(mapped_instrument, start_date=min_date)
        if df_yahoo is None or df_yahoo.empty:
            print(f"No Yahoo data for {mapped_instrument}, skipping.")
            continue

        # 4) Build record tuples for the upsert
        records = []
        for _, row in df_yahoo.iterrows():
            records.append((
                mapped_instrument,          # store the final mapped symbol in DB
                row['price_date'],
                row['open_price'],
                row['high_price'],
                row['low_price'],
                row['close_price'],
                row['adj_close'],
                None if pd.isna(row['volume']) else int(row['volume']),
                row['currency'],
                row['exchange']
            ))

        upsert_market_data(records)
        print(f"Upserted {len(records)} rows for {raw_instrument} (fetched as {mapped_instrument}).")

if __name__ == "__main__":
    main()