import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_connection():
    """Establish a connection to the database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def fetch_transactions():
    """Fetch relevant transactions for equity-related events."""
    query = """
        SELECT 
            activity_date,
            instrument,
            raw_trans_code AS event_type,
            quantity,
            amount
        FROM transactions
        WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL')
        ORDER BY activity_date;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df['activity_date'] = pd.to_datetime(df['activity_date']).dt.tz_localize(None)  # Remove timezone
    return df

def fetch_market_data():
    """Fetch market data for selected instruments and fill missing prices."""
    query = """
    WITH monthly_data AS (
        SELECT 
            instrument,
            DATE_TRUNC('month', price_date) AS month_start,
            MAX(price_date) AS eom_date
        FROM market_data
        WHERE instrument IN ('AAPL', 'CPNG', 'TSM', 'TOST', 'NU', 'NVO', 'MSFT', 'INTC', 'AMRC')
        GROUP BY instrument, DATE_TRUNC('month', price_date)
    ),
    eom_prices AS (
        SELECT 
            md.instrument,
            md.month_start,
            md.eom_date,
            md2.close_price AS eom_price
        FROM monthly_data md
        JOIN market_data md2
        ON md.instrument = md2.instrument AND md.eom_date = md2.price_date
    )
    SELECT 
        e1.instrument,
        e1.month_start,
        e1.eom_price AS bom_price,  -- Use previous EOM as BOM for the current month
        e2.eom_price AS eom_price
    FROM eom_prices e1
    LEFT JOIN eom_prices e2
    ON e1.instrument = e2.instrument AND e1.month_start = e2.month_start - INTERVAL '1 month'
    ORDER BY e1.instrument, e1.month_start;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    # Print column types for debugging
    print("Column types before processing:", df.dtypes)

    # Ensure `month_start` is datetime and timezone-naive
    if 'month_start' in df.columns:
        df['month_start'] = pd.to_datetime(df['month_start'], errors='coerce')
        if df['month_start'].dt.tz is not None:  # If timezone-aware
            df['month_start'] = df['month_start'].dt.tz_convert('UTC').dt.tz_localize(None)

    # Sort by instrument and month_start to ensure forward and backward fill work correctly
    df = df.sort_values(by=['instrument', 'month_start'])

    # Fill missing BOM and EOM prices with the last available values
    df['bom_price'] = df.groupby('instrument')['bom_price'].ffill()
    df['eom_price'] = df.groupby('instrument')['eom_price'].ffill()

    # Print dataframe head for debugging
    print("Market data after processing:", df.head())

    return df

def calculate_asset_values(transactions, market_data):
    """
    Calculate asset values for each instrument and month.
    """
    transactions['activity_date'] = pd.to_datetime(transactions['activity_date'])
    transactions['as_of_date'] = transactions['activity_date'].apply(
        lambda x: x.replace(day=monthrange(x.year, x.month)[1])
    )
    market_data['month_start'] = pd.to_datetime(market_data['month_start'])

    asset_values = []
    for instrument, group in transactions.groupby('instrument'):
        instrument_data = market_data[market_data['instrument'] == instrument]

        for as_of_date, monthly_txns in group.groupby('as_of_date'):
            # Match the month for BOM and EOM prices
            month_data = instrument_data[instrument_data['month_start'] == as_of_date.replace(day=1)]

            if month_data.empty:
                print(f"Warning: Still no market data available for {instrument} on {as_of_date}. Skipping.")
                continue

            bom_price = month_data['bom_price'].iloc[0]
            eom_price = month_data['eom_price'].iloc[0]

            shares_bom = monthly_txns['quantity'].cumsum().iloc[0]
            shares_eom = monthly_txns['quantity'].cumsum().iloc[-1]

            T = (as_of_date - as_of_date.replace(day=1)).days + 1
            monthly_txns['t_i'] = (monthly_txns['activity_date'] - as_of_date.replace(day=1)).dt.days + 1
            monthly_txns['W_i'] = (T - monthly_txns['t_i'] + 1) / T
            monthly_txns['WCF_i'] = monthly_txns['W_i'] * monthly_txns['amount']
            wcf = monthly_txns['WCF_i'].sum()

            realized_pnl = monthly_txns[monthly_txns['event_type'] == 'CDIV']['amount'].sum()
            net_cf = monthly_txns['amount'].sum()
            unrealized_pnl = shares_eom * eom_price - shares_bom * bom_price - net_cf
            bom_nav = shares_bom * bom_price
            eom_nav = shares_eom * eom_price
            average_capital = bom_nav + wcf

            asset_values.append({
                'as_of_date': as_of_date,
                'instrument': instrument,
                'shares_bom': shares_bom,
                'shares_eom': shares_eom,
                'price_bom': bom_price,
                'price_eom': eom_price,
                'bom_nav': bom_nav,
                'eom_nav': eom_nav,
                'net_cf': net_cf,
                'wcf': wcf,
                'realized_pnl': realized_pnl,
                'unrealized_pnl': unrealized_pnl,
                'average_capital': average_capital,
            })

    # Return as a DataFrame
    asset_values_df = pd.DataFrame(asset_values)
    return asset_values_df


def upsert_asset_values(asset_values):
    """Insert or update the calculated asset values into the database."""
    conn = get_connection()
    cursor = conn.cursor()

    upsert_sql = """
    INSERT INTO asset_value (
        as_of_date, instrument, shares_bom, shares_eom, price_bom, price_eom,
        bom_nav, eom_nav, net_cf, wcf, realized_pnl, unrealized_pnl, average_capital
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (as_of_date, instrument) DO UPDATE SET
        shares_bom = EXCLUDED.shares_bom,
        shares_eom = EXCLUDED.shares_eom,
        price_bom = EXCLUDED.price_bom,
        price_eom = EXCLUDED.price_eom,
        bom_nav = EXCLUDED.bom_nav,
        eom_nav = EXCLUDED.eom_nav,
        net_cf = EXCLUDED.net_cf,
        wcf = EXCLUDED.wcf,
        realized_pnl = EXCLUDED.realized_pnl,
        unrealized_pnl = EXCLUDED.unrealized_pnl,
        average_capital = EXCLUDED.average_capital;
    """

    asset_values['as_of_date'] = asset_values['as_of_date'].dt.date

    records = asset_values.to_records(index=False)
    cursor.executemany(upsert_sql, records)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    print("Fetching transactions...")
    transactions = fetch_transactions()

    # Portfolio instruments
    instruments = ['AAPL', 'CPNG', 'TSM', 'TOST', 'NU', 'NVO', 'MSFT', 'INTC', 'AMRC']
    transactions = transactions[transactions['instrument'].isin(instruments)]

    print("Fetching market data...")
    market_data = fetch_market_data()

    print("Calculating asset values...")
    asset_values = calculate_asset_values(transactions, market_data)

    # Debugging: Ensure asset_values has expected columns
    print(asset_values.head())
    print(asset_values.columns)

    print("Upserting asset values...")
    upsert_asset_values(asset_values)

    print("Done.")

if __name__ == "__main__":
    main()