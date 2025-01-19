import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
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

# Fetch Transactions with Adjusted Signs
def fetch_transactions():
    query = """
        SELECT
            activity_date,
            instrument,
            raw_trans_code AS event_type,
            quantity,
            CASE
                WHEN raw_trans_code = 'Buy' THEN ABS(amount) -- Treat buys as positive
                WHEN raw_trans_code = 'Sell' THEN -ABS(amount) -- Treat sells as negative
                ELSE amount -- Dividends and other flows retain their original sign
            END AS amount
        FROM transactions
        WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL')
        ORDER BY activity_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    df['activity_date'] = pd.to_datetime(df['activity_date']).dt.tz_localize(None)
    df['txn_month'] = df['activity_date'].dt.to_period('M')

    # Debug print
    print("Transactions fetched and transformed:")
    print(df.head())

    return df[['activity_date', 'instrument', 'event_type', 'quantity', 'amount', 'txn_month']]
# Fetch Monthly Market Data
def fetch_monthly_market_data():
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
    df['year_month'] = df['price_date'].dt.to_period('M')
    df.rename(columns={'close_price': 'monthly_price'}, inplace=True)

    # Debug print
    print("Market data fetched and transformed:")
    print(df.head())

    return df[['instrument', 'year_month', 'monthly_price']]
# Weighted Cash Flow (WCF) Calculation with Expanded Components
def calculate_wcf_components(txn_group, start_date, end_date):
    """
    Calculate Weighted Cash Flow (WCF) along with its components, including cash flow type.
    Returns WCF and a DataFrame with the components for debugging.
    """
    total_days = (end_date - start_date).days + 1
    wcf = 0.0
    components = []  # Store components for debugging

    for _, txn in txn_group.iterrows():
        t_i = (txn['activity_date'] - start_date.to_timestamp()).days
        weight = (total_days - t_i + 1) / total_days

        # Handle NaN cash flows explicitly
        cash_flow = txn['amount'] if pd.notna(txn['amount']) else 0.0
        cash_flow_contribution = cash_flow * weight
        wcf += cash_flow_contribution

        # Classify the cash flow type
        cash_flow_type = "Unknown"
        if txn['event_type'] == 'Buy':
            cash_flow_type = "Purchase"
        elif txn['event_type'] == 'Sell':
            cash_flow_type = "Sale"
        elif txn['event_type'] == 'CDIV':
            cash_flow_type = "Dividend"
        elif txn['event_type'] == 'SPL':
            cash_flow_type = "Stock Split"

        # Append the components for debugging
        components.append({
            'activity_date': txn['activity_date'],
            'cash_flow': cash_flow,
            't_i': t_i,
            'T': total_days,
            'weight': weight,
            'cash_flow_contribution': cash_flow_contribution,
            'cash_flow_type': cash_flow_type
        })

    return wcf, pd.DataFrame(components)

# Ensure this function is included in the script before `calculate_returns`
def summarize_transactions(transactions):
    transactions['year_month'] = transactions['txn_month']  # Align txn_month with year_month

    trans_agg = []
    for (instr, ymonth), grp in transactions.groupby(['instrument', 'year_month']):
        net_cf = grp['amount'].sum()
        net_shares_change = grp['quantity'].sum()
        realized_pnl = grp.loc[grp['event_type'] == 'CDIV', 'amount'].sum()
        trans_agg.append({
            'instrument': instr,
            'year_month': ymonth,
            'net_cf': net_cf,
            'net_shares_change': net_shares_change,
            'realized_pnl': realized_pnl
        })

    trans_df = pd.DataFrame(trans_agg)

    # Debug print
    print("Transaction summary:")
    print(trans_df.head())

    return trans_df

def generate_time_series(transactions, market_data):
    instruments = transactions['instrument'].unique()
    all_records = []

    for instrument in instruments:
        instrument_txns = transactions[transactions['instrument'] == instrument]
        instrument_market_data = market_data[market_data['instrument'] == instrument]

        start_month = instrument_txns['year_month'].min()
        end_month = max(
            instrument_txns['year_month'].max(),
            instrument_market_data['year_month'].max()
        )

        time_index = pd.period_range(start=start_month.start_time, end=end_month.end_time, freq='M')

        for month in time_index:
            all_records.append({
                'instrument': instrument,
                'year_month': month,
                'bom_nav': 0.0,
                'eom_nav': 0.0,
                'net_cf': 0.0,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0
            })

    time_series = pd.DataFrame(all_records)

    # Debug print
    print("Generated time series:")
    print(time_series.head())

    return time_series

# Modified Dietz Return Calculation with Expanded Components
# Modified Dietz Return Calculation with Expanded Components
def calculate_returns(transactions, market_data):
    """
    Calculate returns using the Modified Dietz Method with additional components.
    """
    print("Summarizing transactions...")
    transactions_summary = summarize_transactions(transactions)
    print("Transaction summary:")
    print(transactions_summary.head())

    # Merge transactions summary with market data
    print("Merging transactions summary with market data...")
    merged = pd.merge(
        transactions_summary,
        market_data,
        on=['instrument', 'year_month'],
        how='outer'  # Outer join ensures all months are included
    )
    print("Merged transactions and market data:")
    print(merged.head())

    # Generate a complete time series
    print("Generating a complete time series...")
    time_series = generate_time_series(transactions, market_data)
    print("Generated time series:")
    print(time_series.head())

    # Merge time series with transactions and market data
    print("Merging time series with transactions and market data...")
    merged = pd.merge(time_series, merged, on=['instrument', 'year_month'], how='left')
    print("Merged time series and transactions:")
    print(merged.head())

    # Fill missing values
    print("Filling missing values...")
    merged.fillna({'net_cf': 0.0, 'realized_pnl': 0.0, 'bom_nav': 0.0, 'eom_nav': 0.0, 'wcf': 0.0}, inplace=True)
    print("Merged data after filling missing values:")
    print(merged.head())

    # Validate year_month data type before processing
    print("Validating year_month data types...")
    print("Sample year_month values:")
    print(merged['year_month'].head())
    print("Data type of year_month:", type(merged['year_month'].iloc[0]))

    # Calculate WCF
    print("Calculating WCF...")
    merged['wcf'] = merged.apply(
        lambda row: calculate_wcf_components(
            transactions[
                (transactions['instrument'] == row['instrument']) &
                (transactions['txn_month'] == row['year_month'])
            ],
            pd.Period(row['year_month'], freq='M').start_time,
            pd.Period(row['year_month'], freq='M').end_time
        )[0],
        axis=1
    )
    print("WCF calculated. Sample values:")
    print(merged[['instrument', 'year_month', 'wcf']].head())

    # Calculate PnL
    print("Calculating PnL...")
    merged['pnl'] = merged['eom_nav'] - merged['bom_nav'] - merged['net_cf']
    print("PnL calculated. Sample values:")
    print(merged[['instrument', 'year_month', 'pnl']].head())

    # Calculate Average Capital
    print("Calculating Average Capital...")
    merged['average_capital'] = merged['bom_nav'] + merged['wcf']
    print("Average Capital calculated. Sample values:")
    print(merged[['instrument', 'year_month', 'average_capital']].head())

    # Calculate Modified Dietz Return
    print("Calculating Modified Dietz Return...")
    merged['return'] = merged['pnl'] / merged['average_capital']
    print("Returns calculated. Sample values:")
    print(merged[['instrument', 'year_month', 'return']].head())

    return merged[['instrument', 'year_month', 'bom_nav', 'eom_nav', 'net_cf', 'wcf', 'pnl', 'average_capital', 'return']]

def calculate_realized_unrealized_pnl(merged, transactions):
    for _, row in transactions.iterrows():
        if row['event_type'] in ['Sell', 'CDIV']:
            merged.loc[
                (merged['instrument'] == row['instrument']) &
                (merged['year_month'] == row['txn_month']),
                'realized_pnl'
            ] += row['amount']

    merged['unrealized_pnl'] = merged['pnl'] - merged['realized_pnl']

    # Debug print
    print("Merged data with realized and unrealized PnL:")
    print(merged.head())

    return merged
# Main
def main():
    print("Fetching transactions...")
    transactions = fetch_transactions()
    print(f"Fetched {len(transactions)} transaction rows.")

    print("Fetching market data...")
    market_data = fetch_monthly_market_data()
    print(f"Fetched {len(market_data)} market data rows.")

    print("Generating time series and calculating returns...")
    returns = calculate_returns(transactions, market_data)

    print("Calculating realized and unrealized PnL...")
    returns = calculate_realized_unrealized_pnl(returns, transactions)

    # Final debug print
    print("Final Returns Table:")
    print(returns.head(57))

if __name__ == "__main__":
    main()