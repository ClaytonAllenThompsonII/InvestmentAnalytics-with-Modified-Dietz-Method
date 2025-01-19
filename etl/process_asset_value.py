import os
import psycopg2
import pandas as pd
from datetime import datetime
from calendar import monthrange
from dotenv import load_dotenv

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

# 1) Fetch Transactions
def fetch_transactions():
    """
    Retrieve relevant transactions (Buy, Sell, Dividend, Split).
    We only gather columns needed for monthly P&L and share changes.
    """
    query = """
        SELECT
            activity_date,
            instrument,
            raw_trans_code AS event_type,
            quantity,
            amount
        FROM transactions
        WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL')
        ORDER BY activity_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    # Ensure datetime is timezone-naive
    df['activity_date'] = pd.to_datetime(df['activity_date']).dt.tz_localize(None)

    # Create a year-month Period from the activity_date
    df['txn_month'] = df['activity_date'].dt.to_period('M')

    return df

# 2) Fetch Monthly Market Data
def fetch_monthly_market_data():
    """
    We assume each row in market_data is already monthly,
    e.g., recorded on the 15th or some consistent day each month.
    We'll simply convert price_date => year_month, 
    and treat close_price as 'this month's price'.
    """
    query = """
        SELECT
            instrument,
            price_date,
            close_price
        FROM market_data
        ORDER BY instrument, price_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    df['price_date'] = pd.to_datetime(df['price_date']).dt.tz_localize(None)
    # Convert to Period
    df['year_month'] = df['price_date'].dt.to_period('M')

    # Potentially rename 'close_price' to 'monthly_price' for clarity
    df.rename(columns={'close_price': 'monthly_price'}, inplace=True)

    # We'll keep columns: [instrument, year_month, monthly_price]
    # Possibly discard 'price_date' if unneeded:
    return df[['instrument','year_month','monthly_price']]

# 3) Summarize Transactions by Month/Instrument
def summarize_transactions(transactions):
    """
    For each (instrument, txn_month):
      - sum 'amount' => net_cf
      - sum 'quantity' => net_shares_change
      - sum dividends if event_type=='CDIV' => realized_pnl
      - Weighted CF is optional. We'll do a simple half-month approach or skip.
    """
    trans_agg = []
    for (instr, ymonth), grp in transactions.groupby(['instrument','txn_month']):
        net_cf = grp['amount'].sum()
        net_shares_change = grp['quantity'].sum()
        realized_pnl = grp.loc[grp['event_type'] == 'CDIV','amount'].sum()

        # Simple weighting approach => assume mid-month => WCF = net_cf*0.5
        # or do a daily-based approach if you want
        wcf = net_cf * 0.5

        trans_agg.append({
            'instrument': instr,
            'year_month': ymonth,
            'net_cf': net_cf,
            'net_shares_change': net_shares_change,
            'realized_pnl': realized_pnl,
            'wcf': wcf
        })

    trans_df = pd.DataFrame(trans_agg)
    return trans_df

# 4) Calculate Monthly Asset Values
def calculate_asset_values(transactions, market_data):
    """
    Combine monthly transaction summaries with monthly market prices,
    then compute:
       - shares_bom / shares_eom
       - price (we'll just have 1 monthly_price)
       - net_cf, wcf, realized_pnl
       - bom_nav, eom_nav, unrealized_pnl, average_capital
       - as_of_date => last day of that month
    """
    # 4.1 Summarize transactions by month
    trans_df = summarize_transactions(transactions)

    # 4.2 Merge with monthly market data
    merged = pd.merge(
        trans_df,
        market_data,   # columns: [instrument, year_month, monthly_price]
        on=['instrument','year_month'],
        how='inner'    # only keep months that exist in both sets
    )

    # 4.3 Sort by instrument, year_month so we can do a cumulative share approach
    merged = merged.sort_values(['instrument','year_month'])

    # We'll track cumulative shares across months
    merged['shares_bom'] = 0.0
    merged['shares_eom'] = 0.0

    def per_instrument_calc(group):
        current_shares = 0.0
        out_rows = []
        for idx, row in group.iterrows():
            row['shares_bom'] = current_shares
            row['shares_eom'] = current_shares + row['net_shares_change']
            current_shares = row['shares_eom']
            out_rows.append(row)
        return pd.DataFrame(out_rows)

    merged = merged.groupby('instrument', group_keys=False).apply(per_instrument_calc).reset_index(drop=True)

    # 4.4 Compute nav, P&L, etc.
    # We only have one monthly_price => treat that as eom_price
    # For BOM price, either store the prior month’s price,
    # or just use the same monthly_price for BOM & EOM. 
    # Alternatively, keep a "rolling" approach so that "BOM_price" = last month's price. 
    # We'll do a simple approach => BOM = prior month's monthly_price.

    # SHIFT monthly_price down 1 month to get BOM price
    merged['bom_price'] = merged.groupby('instrument')['monthly_price'].shift(1)
    # if no prior month => fill first with the same as monthly_price or 0.0
    merged['bom_price'] = merged.groupby('instrument')['bom_price'].fillna(merged['monthly_price'])

    # eom_price is the current row's monthly_price
    merged['eom_price'] = merged['monthly_price']

    merged['bom_nav'] = merged['shares_bom'] * merged['bom_price']
    merged['eom_nav'] = merged['shares_eom'] * merged['eom_price']

    # net_cf, wcf, realized_pnl already exist
    merged['unrealized_pnl'] = merged['eom_nav'] - merged['bom_nav'] - merged['net_cf']
    merged['average_capital'] = merged['bom_nav'] + merged['wcf']

    # 4.5 as_of_date => last day of year_month
    def last_day_of_period(period):
        start_dt = period.to_timestamp(how='start')  # e.g. '2025-01-01'
        days_in_month = monthrange(start_dt.year, start_dt.month)[1]
        return start_dt.replace(day=days_in_month).date()

    merged['as_of_date'] = merged['year_month'].apply(last_day_of_period)

    # 4.6 Build final DataFrame
    keep_cols = [
        'as_of_date','instrument',
        'shares_bom','shares_eom',
        'bom_price','eom_price',
        'bom_nav','eom_nav',
        'net_cf','wcf',
        'realized_pnl','unrealized_pnl','average_capital'
    ]
    final_df = merged[keep_cols].reset_index(drop=True)
    return final_df

# 5) Upsert logic
def upsert_asset_values(df):
    """
    Insert or update rows in the 'asset_value' table
    using (as_of_date, instrument) as the unique key.
    """
    if df.empty:
        print("No asset values to upsert.")
        return

    conn = get_connection()
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO asset_value (
            as_of_date, instrument,
            shares_bom, shares_eom,
            price_bom, price_eom,
            bom_nav, eom_nav,
            net_cf, wcf,
            realized_pnl, unrealized_pnl,
            average_capital
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (as_of_date, instrument)
        DO UPDATE SET
            shares_bom = EXCLUDED.shares_bom,
            shares_eom = EXCLUDED.shares_eom,
            price_bom  = EXCLUDED.price_bom,
            price_eom  = EXCLUDED.price_eom,
            bom_nav    = EXCLUDED.bom_nav,
            eom_nav    = EXCLUDED.eom_nav,
            net_cf     = EXCLUDED.net_cf,
            wcf        = EXCLUDED.wcf,
            realized_pnl   = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            average_capital = EXCLUDED.average_capital
    """

    # as_of_date => convert to date object (no time)
    df['as_of_date'] = pd.to_datetime(df['as_of_date']).dt.date

    records = df.to_records(index=False)
    cur.executemany(upsert_sql, records)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Upserted {len(records)} asset_value rows.")

# 6) Main entrypoint
def main():
    print("Fetching transactions...")
    txns = fetch_transactions()
    print(f"  Fetched {len(txns)} transaction rows.")

    print("Fetching monthly market data (already monthly in DB)...")
    market_data = fetch_monthly_market_data()
    print(f"  Fetched {len(market_data)} monthly market rows.")

    print("Calculating monthly asset values...")
    df_asset = calculate_asset_values(txns, market_data)
    print(f"  Computed {len(df_asset)} asset_value records.")

    # Quick peek
    print(df_asset.head(10))

    print("Upserting into asset_value table...")
    upsert_asset_values(df_asset)

    print("Done.")

if __name__ == "__main__":
    main()