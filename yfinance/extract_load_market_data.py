import os
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
import logging
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

load_dotenv()

# Database credentials from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    """Create a psycopg2 connection to the Postgres database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise


def truncate_market_data():
    """
    Truncate the market_data table to clear existing records and reset its primary key.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE market_data RESTART IDENTITY;")
            conn.commit()
        logging.info("Truncated market_data table. Starting fresh...")
    except Exception as e:
        logging.error("Error truncating market_data table: %s", e)
        raise


def get_instruments_and_min_dates():
    """
    Query the transactions table to get distinct instruments (tickers) along with their earliest activity date.
    The query orders instruments alphabetically.
    
    Returns:
        list of tuples: [(instrument, min_date), ...]
    """
    query = """
        SELECT instrument, MIN(activity_date) AS min_date
        FROM transactions
        WHERE raw_trans_code IN ('Buy', 'Sell', 'REC')
          AND instrument IS NOT NULL
        GROUP BY instrument
        ORDER BY instrument;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        logging.info("Fetched %d instruments with min dates.", len(results))
        return results
    except Exception as e:
        logging.error("Error fetching instruments and min dates: %s", e)
        return []


def get_market_data_end_date():
    """
    Determine the appropriate end date for market data.
    If the current time is after 4PM, use today's date;
    otherwise, use yesterday's date.
    
    Returns:
        str: Date in 'YYYY-MM-DD' format.
    """
    now = datetime.now()
    cutoff_hour = 16  # 4PM cutoff
    if now.hour >= cutoff_hour:
        return now.strftime('%Y-%m-%d')
    else:
        return (now - timedelta(days=1)).strftime('%Y-%m-%d')


def fetch_yahoo_data_for_instrument(symbol, start_date, end_date=None, interval='1d'):
    """
    Fetch daily data from Yahoo Finance for the given symbol and date range,
    then aggregate it to one row per month (using the last trading day of each month).
    
    If end_date is None, it is computed using the 4PM cutoff logic.
    """
    if not end_date:
        end_date = get_market_data_end_date()
        logging.info("Computed end_date for %s: %s", symbol, end_date)
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, interval=interval)
    except Exception as e:
        logging.error("Error fetching data from Yahoo for %s: %s", symbol, e)
        return None

    if df.empty:
        logging.warning("No data returned for %s from %s to %s.", symbol, start_date, end_date)
        return None

    # Process the DataFrame: reset index and rename columns to match our schema.
    df.reset_index(inplace=True)
    df.rename(columns={
        'Date': 'price_date',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Volume': 'volume'
    }, inplace=True, errors='ignore')

    if 'price_date' not in df.columns:
        logging.error("price_date column missing for %s; skipping.", symbol)
        return None

    df['price_date'] = pd.to_datetime(df['price_date'])
    # Group by year-month and take the last trading day.
    df['year_month'] = df['price_date'].dt.to_period('M')
    df_monthly = df.groupby('year_month').tail(1).copy()
    df_monthly['price_date'] = df_monthly['price_date'].dt.date

    # Attach additional info (if available) from the ticker.
    info = getattr(ticker, 'info', {})
    df_monthly['currency'] = info.get('currency')
    df_monthly['exchange'] = info.get('exchange')

    return df_monthly


def upsert_market_data(records):
    """
    Insert or upsert the fetched Yahoo data into the market_data table.
    Uses PostgreSQL's ON CONFLICT clause (based on instrument and price_date).
    """
    upsert_sql = """
        INSERT INTO market_data (
            instrument, price_date, open_price, high_price, low_price,
            close_price, volume, currency, exchange
        )
        VALUES %s
        ON CONFLICT (instrument, price_date)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            currency = EXCLUDED.currency,
            exchange = EXCLUDED.exchange;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, upsert_sql, records)
            conn.commit()
        logging.info("Upserted %d rows into market_data.", len(records))
    except Exception as e:
        logging.error("Error upserting market data: %s", e)
        raise


def main():
    try:
        # Step 1: Clear the target table.
        truncate_market_data()

        # Step 2: Retrieve instruments (with their earliest transaction dates).
        instruments_data = get_instruments_and_min_dates()
        if not instruments_data:
            logging.warning("No instruments found to process.")
            return

        # Log the list of instruments (which will be in ascending order).
        instrument_list = [instrument for instrument, _ in instruments_data]
        logging.info("Instruments to process: %s", instrument_list)

        # Optional: Map raw symbols if necessary (e.g., map 'FB' to 'META').
        symbol_map = {'FB': 'META'}

        # Step 3: Process each ticker independently.
        for instrument, min_date in instruments_data:
            try:
                symbol = symbol_map.get(instrument, instrument)
                logging.info("Processing ticker %s (min_date: %s)", symbol, min_date)
                
                df_yahoo = fetch_yahoo_data_for_instrument(
                    symbol,
                    start_date=min_date,
                    end_date=None,  # Computed inside the function
                    interval='1d'
                )

                if df_yahoo is None or df_yahoo.empty:
                    logging.info("No Yahoo data for %s; skipping.", symbol)
                    continue

                # (Optional) Log the DataFrame structure for debugging.
                logging.info("Fetched data for %s with columns: %s", symbol, df_yahoo.columns.tolist())
                # Uncomment the next line if you want to log a sample row:
                # logging.info("Sample row for %s: %s", symbol, df_yahoo.head(1).to_dict(orient='records'))

                # Build record tuples for this ticker.
                records = []
                required_cols = ['price_date', 'open_price', 'high_price', 'low_price', 'close_price']
                for idx, row in df_yahoo.iterrows():
                    try:
                        missing = [col for col in required_cols if col not in row or pd.isna(row[col])]
                        if missing:
                            raise ValueError(f"Missing required columns {missing} in row {idx}")
                        
                        volume_val = None
                        if 'volume' in row and pd.notna(row.get('volume')):
                            try:
                                volume_val = int(row['volume'])
                            except Exception as e:
                                logging.warning("Volume conversion error for %s on row %s: %s", symbol, idx, e)
                        
                        record = (
                            symbol,
                            row['price_date'],
                            row.get('open_price'),
                            row.get('high_price'),
                            row.get('low_price'),
                            row.get('close_price'),
                            volume_val,
                            row.get('currency'),
                            row.get('exchange')
                        )
                        records.append(record)
                    except Exception as rec_e:
                        logging.error("Error processing row %s for %s: %s", idx, symbol, rec_e)
                        continue

                if records:
                    upsert_market_data(records)
                    logging.info("Upserted %d rows for %s.", len(records), symbol)
                else:
                    logging.info("No valid records to upsert for %s.", symbol)
            except Exception as ticker_e:
                logging.error("Error processing ticker %s: %s", instrument, ticker_e)
                continue

    except Exception as e:
        logging.error("Error in main processing: %s", e)


if __name__ == "__main__":
    main()