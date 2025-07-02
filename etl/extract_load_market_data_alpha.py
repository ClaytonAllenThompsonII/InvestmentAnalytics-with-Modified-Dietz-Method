import os
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
from psycopg2.extras import execute_values
from alpha_vantage.timeseries import TimeSeries
import requests
from collections import defaultdict

# Load environment variables
load_dotenv()

# Required API key
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing Alpha Vantage API key. Set ALPHAVANTAGE_API_KEY in .env")

# Alpha Vantage timeseries object
ts = TimeSeries(key=ALPHAVANTAGE_API_KEY, output_format='json')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Environment-based DB credentials
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Symbol remapping and validation constants
SYMBOL_MAP = {'FB': 'META'}
REQUIRED_EQUITY_COLS = ['price_date', 'open_price', 'high_price', 'low_price', 'close_price']


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def truncate_market_data():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE market_data RESTART IDENTITY;")
        conn.commit()
    logging.info("Truncated market_data table. Starting fresh...")


def truncate_options_market_data():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE options_market_data RESTART IDENTITY;")
        conn.commit()
    logging.info("Truncated options_market_data table. Starting fresh...")


def get_equity_symbols_to_pull():
    query = """
    SELECT DISTINCT instrument
    FROM fifo_equity_lots
    WHERE open_quantity > 0;
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

    # Flatten results and add benchmarks
    symbols = [row[0] for row in results]
    
    # Explicitly add benchmarks
    benchmarks = ['SPY', 'QQQ', 'XLK', 'XLY', 'EEM', 'IXUS']
    
    # Combine and remove duplicates
    all_symbols = list(set(symbols + benchmarks))
    
    return all_symbols

def get_active_option_positions():
    query = """
        SELECT instrument, description, expiration_date, option_type, strike_price
        FROM fifo_option_lots
        WHERE open_contracts > 0;
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()



def fetch_alpha_data_for_instrument(symbol):
    try:
        data, _ = ts.get_daily(symbol=symbol, outputsize='full')
    except Exception as e:
        logging.error("Alpha Vantage error for %s: %s", symbol, e)
        return None

    if not data:
        logging.warning("No data returned for %s", symbol)
        return None

    try:
        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index)
        df.columns = [
            'open_price', 'high_price', 'low_price',
            'close_price', 'volume'
        ]
        df.sort_index(inplace=True)

        if df.empty:
            logging.info("No data available for %s after retrieval.", symbol)
            return None

        # Just add the date from the index directly
        df['price_date'] = df.index.date

        # Keep daily frequency by removing the monthly grouping entirely
        df['currency'] = None
        df['exchange'] = None

        return df[[
            'price_date', 'open_price', 'high_price',
            'low_price', 'close_price', 'volume', 'currency', 'exchange'
        ]]
    except Exception as e:
        logging.error("Processing error for %s: %s", symbol, e)
        return None
    
def upsert_market_data(records):
    sql = """
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
    with get_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, records)
        conn.commit()
    logging.info("Upserted %d equity records.", len(records))



def fetch_alpha_options_chain(symbol, date):
    url = (
        f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS"
        f"&symbol={symbol}&date={date}&apikey={ALPHAVANTAGE_API_KEY}"
    )
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("message") == "success":
            return data["data"]
        else:
            logging.warning("No options returned for %s on %s", symbol, date)
            return []
    except Exception as e:
        logging.error("Fetch error for %s: %s", symbol, e)
        return []


def upsert_options_market_data(records):
    sql = """
        INSERT INTO options_market_data (
            symbol, contract_id, expiration, strike, type, date,
            last, mark, bid, ask, volume, open_interest,
            implied_volatility, delta, gamma, theta, vega, rho
        )
        VALUES %s
        ON CONFLICT (contract_id, date)
        DO UPDATE SET
            last = EXCLUDED.last,
            mark = EXCLUDED.mark,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            implied_volatility = EXCLUDED.implied_volatility,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            rho = EXCLUDED.rho;
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, records)
        conn.commit()
    logging.info("Upserted %d options records.", len(records))


def process_equity(symbol):
    df = fetch_alpha_data_for_instrument(symbol)
    if df is None or df.empty:
        logging.info("No equity data for %s; skipping.", symbol)
        return

    records = []
    for _, row in df.iterrows():
        try:
            missing = [col for col in REQUIRED_EQUITY_COLS if col not in row or pd.isna(row[col])]
            if missing:
                raise ValueError(f"Missing columns: {missing}")

            volume_val = int(row['volume']) if pd.notna(row['volume']) else None

            records.append((
                symbol,
                row['price_date'],
                row['open_price'],
                row['high_price'],
                row['low_price'],
                row['close_price'],
                volume_val,
                row['currency'],
                row['exchange']
            ))
        except Exception as e:
            logging.warning("Skipping row for %s due to error: %s", symbol, e)

    if records:
        upsert_market_data(records)


def process_options(symbol, positions, date):
    chain = fetch_alpha_options_chain(symbol, date)
    if not chain:
        return

    df = pd.DataFrame(chain)
    if df.empty:
        return

    matched = []
    for _, opt in df.iterrows():
        for owned in positions:
            _, _, exp, typ, strike = owned
            if (
                opt['expiration'] == exp.strftime('%Y-%m-%d') and
                opt['type'].lower() == typ.lower() and
                float(opt['strike']) == float(strike)
            ):
                matched.append((
                    symbol,
                    opt['contractID'],
                    opt['expiration'],
                    opt['strike'],
                    opt['type'],
                    opt['date'],
                    opt.get('last'),
                    opt.get('mark'),
                    opt.get('bid'),
                    opt.get('ask'),
                    opt.get('volume'),
                    opt.get('open_interest'),
                    opt.get('implied_volatility'),
                    opt.get('delta'),
                    opt.get('gamma'),
                    opt.get('theta'),
                    opt.get('vega'),
                    opt.get('rho')
                ))

    if matched:
        upsert_options_market_data(matched)


def main():
    logging.info("Starting ETL: Alpha Vantage equity and options load")

    truncate_market_data()
    truncate_options_market_data()

    # Equity processing
    instruments = get_equity_symbols_to_pull()
    for instrument in instruments:
        symbol = SYMBOL_MAP.get(instrument, instrument)
        logging.info("Processing equity symbol: %s", symbol)
        process_equity(symbol)

    # Options processing remains unchanged
    option_positions = get_active_option_positions()
    if not option_positions:
        logging.info("No active option positions.")
        return

    options_by_symbol = defaultdict(list)
    for row in option_positions:
        options_by_symbol[row[0]].append(row)

    options_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # yesterday's date
    for symbol, positions in options_by_symbol.items():
        logging.info("Processing options for: %s", symbol)
        process_options(symbol, positions, options_date)

if __name__ == "__main__":
    main()