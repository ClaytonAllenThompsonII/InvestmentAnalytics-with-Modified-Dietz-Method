import os
import math
from datetime import datetime
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Loads DB credentials from .env

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


def parse_parentheses(value_str):
    """
    Convert strings like '($58.19)' to float -58.19.
    Remove '$', commas. Return None if invalid or empty.
    """
    if pd.isna(value_str) or value_str.strip() == "":
        return None
    
    clean_str = value_str.replace('$', '').replace(',', '').strip()
    
    # Parentheses => negative
    if clean_str.startswith('(') and clean_str.endswith(')'):
        clean_str = clean_str.replace('(', '-').replace(')', '')
    
    try:
        return float(clean_str)
    except ValueError:
        return None


def parse_date(date_str):
    """
    Parse dates in 'MM/DD/YY' or 'MM/DD/YYYY' format. Return None if invalid/empty.
    """
    if pd.isna(date_str) or str(date_str).strip() == "":
        return None
    
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def standardize_trans_code(code):
    """
    Convert raw trans codes to a descriptive string if you want.
    Example: 'ACH' => 'Automated Clearing House', 'OCA' => 'One-Cancel-All Order', etc.
    """
    code_map = {
        'ACH': 'Automated Clearing House',
        'BTC': 'Buy to Close',
        'BTO': 'Buy to Open',
        'Buy': 'Buy',
        'CDIV': 'Dividend',
        'DFEE': 'Fee',
        'DTAX': 'Tax',
        'GOLD': 'Gold Fee',
        'OCA': 'One-Cancel-All Order',
        'OEXP': 'Option Expiration',
        'REC': 'Received Something',
        'Sell': 'Sell',
        'SPL': 'Split',
        'STC': 'Sell to Close',
        'STO': 'Sell to Open'
    }
    return code_map.get(code, code)  # fallback if not in dict


def none_if_nan(x):
    """
    Convert float('nan') to None, and empty strings to None.
    """
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    if isinstance(x, str) and not x.strip():
        return None
    return x


def transaction_priority(code):
    """
    Priority so that for the same date:
      1) 'Buy', 'BTO', 'STO'  -> Opens (equity or options)
      2) 'Sell', 'STC', 'BTC' -> Closes
      3) 'OEXP'               -> Expiration
      99) # Everything else (e.g. 'REC', 'ACH', 'OCA', etc.)
    """
    if code in ('Buy', 'BTO', 'STO'):
        return 1
    elif code in ('Sell', 'STC', 'BTC'):
        return 2
    elif code == 'OEXP':
        return 3
    else:
        return 99


def ingest_transactions(csv_file_path):
    """
    Reads the CSV, cleans data, and inserts into 'transactions' table,
    truncating the table each time for a fresh load.
    """
    # 1) Load CSV into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Rename columns to match our table's naming
    df.rename(columns={
        'Activity Date': 'activity_date',
        'Process Date':  'process_date',
        'Settle Date':   'settle_date',
        'Instrument':    'instrument',
        'Description':   'description',
        
        'Trans Code':    'raw_trans_code',  # original, raw code
        'Quantity':      'raw_quantity',
        'Price':         'raw_price',
        'Amount':        'raw_amount'
    }, inplace=True, errors='ignore')
    
    # 2) Clean / transform columns
    df['activity_date'] = df['activity_date'].apply(parse_date)
    df['process_date']  = df['process_date'].apply(parse_date)
    df['settle_date']   = df['settle_date'].apply(parse_date)
    
    # Convert numeric columns
    df['quantity'] = pd.to_numeric(df['raw_quantity'], errors='coerce')
    df['price']    = df['raw_price'].apply(parse_parentheses)
    df['amount']   = df['raw_amount'].apply(parse_parentheses)
    
    # Create standardized code from the raw code
    df['trans_code'] = df['raw_trans_code'].apply(standardize_trans_code)

    # Example special handling: ACH => instrument = 'CASH', quantity=0, price=None
    ach_mask = (df['trans_code'] == 'Automated Clearing House')
    df.loc[ach_mask, 'instrument'] = 'CASH'
    df.loc[ach_mask, 'quantity']   = 0
    df.loc[ach_mask, 'price']      = None

    # If "REC" transaction has missing price, assume 0
    rec_mask = (df['raw_trans_code'] == 'REC') & (df['price'].isnull())
    df.loc[rec_mask, 'price']  = 0.0
    df.loc[rec_mask, 'amount'] = 0.0

    # 2B) Determine a custom priority so that on the same day,
    # "Buy" rows appear before "Sell" rows
    df['trans_priority'] = df['trans_code'].apply(transaction_priority)

    # Keep track of original order to break ties beyond date + trans_priority
    df['original_idx'] = df.index

    # 2C) Sort the DataFrame
    df.sort_values(
        by=['activity_date', 'trans_priority', 'original_idx'],
        ascending=True,
        inplace=True
    )

    # 3) Truncate table, then insert row by row
    truncate_sql = "TRUNCATE TABLE transactions RESTART IDENTITY;"
    insert_sql = """
        INSERT INTO transactions (
            activity_date,
            process_date,
            settle_date,
            raw_trans_code,
            trans_code,
            instrument,
            description,
            quantity,
            price,
            amount,
            raw_quantity,
            raw_price,
            raw_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(truncate_sql)

    # Convert NaN or empty strings => None in the insertion loop
    for _, row in df.iterrows():
        record = (
            none_if_nan(row['activity_date']),
            none_if_nan(row['process_date']),
            none_if_nan(row['settle_date']),
            none_if_nan(row['raw_trans_code']),  # e.g. "ACH"
            none_if_nan(row['trans_code']),      # e.g. "Automated Clearing House"
            none_if_nan(row['instrument']),      # e.g. "CASH"
            none_if_nan(row['description']),
            none_if_nan(row['quantity']),
            none_if_nan(row['price']),
            none_if_nan(row['amount']),
            none_if_nan(row.get('raw_quantity')),
            none_if_nan(row.get('raw_price')),
            none_if_nan(row.get('raw_amount'))
        )
        cursor.execute(insert_sql, record)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Truncated 'transactions' and inserted {len(df)} rows from {csv_file_path}.")


if __name__ == "__main__":
    csv_path = "/Users/claytonthompson/Desktop/portfolio_tx.csv"  # Update as needed
    ingest_transactions(csv_path)