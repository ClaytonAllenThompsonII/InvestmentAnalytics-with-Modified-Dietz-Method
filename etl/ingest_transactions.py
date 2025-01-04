import os
from datetime import datetime
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # This will load DB credentials from your .env

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    """
    Create a psycopg2 connection to the Postgres database.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn


def parse_parentheses(value_str):
    """
    Convert strings like '($58.19)' to numeric -58.19.
    Also remove '$', commas. Return None if invalid or empty.
    """
    if pd.isna(value_str) or value_str.strip() == "":
        return None
    clean_str = value_str.replace('$', '').replace(',', '').strip()
    # Handle parentheses => negative
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
    # Try multiple formats if needed:
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None  # If it didn't match any known format


def standardize_trans_code(code):
    """
    Map transaction codes to a standardized descriptive string.
    NOTE: If you see a new code in future CSVs, add it here.
    """
    code_map = {
        'ACH': 'ACH',
        'BTC': 'Buy to Close',
        'BTO': 'Buy to Open',
        'Buy': 'Buy',
        'CDIV': 'Dividend',
        'DFEE': 'Fee',
        'DTAX': 'Tax',
        'GOLD': 'Gold Fee',
        'OCA': 'OCA',  # if you encounter this code
        'OEXP': 'Option Expiration',
        'REC': 'Received Something',  # if you see 'REC' often
        'Sell': 'Sell',
        'SPL': 'Split',
        'STC': 'Sell to Close',
        'STO': 'Sell to Open'
    }
    return code_map.get(code, code)  # fallback to original if not in map


def ingest_transactions(csv_file_path):
    """
    Reads portfolio_tx.csv, cleans data, and inserts into 'transactions' table.
    """
    # 1) Load CSV into DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Rename columns to something more standard (if needed)
    # Adjust these if your CSV column headers differ
    df.rename(columns={
        'Activity Date': 'activity_date',
        'Process Date': 'process_date',
        'Settle Date': 'settle_date',
        'Instrument': 'instrument',
        'Description': 'description',
        'Trans Code': 'trans_code',
        'Quantity': 'raw_quantity',
        'Price': 'raw_price',
        'Amount': 'raw_amount'
    }, inplace=True, errors='ignore')
    
    # 2) Clean / transform columns
    df['activity_date'] = df['activity_date'].apply(parse_date)
    df['process_date'] = df['process_date'].apply(parse_date)
    df['settle_date'] = df['settle_date'].apply(parse_date)
    
    # Convert raw_quantity, raw_price, raw_amount
    df['quantity'] = pd.to_numeric(df['raw_quantity'], errors='coerce')
    df['price'] = df['raw_price'].apply(parse_parentheses)
    df['amount'] = df['raw_amount'].apply(parse_parentheses)

    # Standardize transaction codes
    df['trans_code'] = df['trans_code'].apply(standardize_trans_code)

    # ---------------------------------------------------------
    # FIX ACH ROWS
    ach_mask = (df['trans_code'] == 'ACH')
    df.loc[ach_mask, 'instrument'] = 'CASH'
    df.loc[ach_mask, 'quantity']   = 0
    df.loc[ach_mask, 'price']      = None
    # amount already parsed (positive deposit / negative withdrawal)
    # ---------------------------------------------------------

    # **Convert all pandas NaN/NA/NaT to None** 
    df = df.where(pd.notnull(df), None)

    # 3) Insert into Postgres
    insert_sql = """
        INSERT INTO transactions (
            activity_date,
            process_date,
            settle_date,
            instrument,
            description,
            trans_code,
            quantity,
            price,
            amount,
            raw_quantity,
            raw_price,
            raw_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = get_connection()
    cursor = conn.cursor()

    # Insert row by row (simple approach)
    for _, row in df.iterrows():
        record = (
            row['activity_date'],
            row['process_date'],
            row['settle_date'],
            row['instrument'],
            row['description'],
            row['trans_code'],
            row['quantity'],
            row['price'],
            row['amount'],
            row.get('raw_quantity', None),
            row.get('raw_price', None),
            row.get('raw_amount', None)
        )
        cursor.execute(insert_sql, record)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted {len(df)} rows from {csv_file_path} into transactions table.")


if __name__ == "__main__":
    # Example usage
    csv_path = "/Users/claytonthompson/Desktop/portfolio_tx.csv"  # adjust path as needed
    ingest_transactions(csv_path)