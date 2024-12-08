"""
Equity NAV Analysis Module for Robinhood Transaction Data

This module processes equity transaction data from a simple `.csv` file downloaded 
from Robinhood (or similar platforms). It calculates various financial metrics 
such as Weighted Cash Flow (WCF), Net Asset Value (NAV), Modified Dietz Return, 
and Geometric Linked Returns, and displays the results in tabular format.

Users can simply download their transaction history as a `.csv` file from Robinhood, 
rename the file, place it on their desktop, and specify the file path and stock 
symbol to analyze.

Assumptions:
1. The input `.csv` file contains transaction data with columns such as 
   'Instrument', 'Activity Date', 'Amount', 'Quantity', 'Price', and 'Trans Code'.
2. The file is renamed and accessible via a user-specified file path.
3. The module uses yFinance to fetch historical stock prices for accurate calculations.

"""
from datetime import datetime
import pandas as pd
import yfinance as yf

def load_and_prepare_data(file_path, stock_symbol):
    """
    Loads and processes transaction data from a Robinhood `.csv` file for a given stock symbol.

    Args:
        file_path (str): Path to the `.csv` file containing transaction data.
        stock_symbol (str): Stock ticker symbol to filter relevant transactions.

    Returns:
        pd.DataFrame: A processed DataFrame containing transactions for the specified stock.
    
    Notes:
        - The input file is assumed to have columns like 'Instrument', 'Activity Date', 
          'Amount', 'Quantity', 'Price', and 'Trans Code'.
        - The function processes dates, amounts, and quantities, and categorizes transactions.
    """
    
    df = pd.read_csv(file_path)
    df_stock = df[df['Instrument'].str.contains(stock_symbol, na=False)].copy()
    
    df_stock['Activity_Date'] = pd.to_datetime(df_stock['Activity Date'], errors='coerce')
    df_stock['Amount'] = pd.to_numeric(
        df_stock['Amount'].replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True), errors='coerce'
    ).fillna(0)
    df_stock['Quantity'] = pd.to_numeric(df_stock['Quantity'], errors='coerce').fillna(0)
    df_stock['Price'] = pd.to_numeric(
        df_stock['Price'].replace({'\$': '', ',': ''}, regex=True), errors='coerce'
    ).fillna(0)
    
    df_stock['Event_Type'] = df_stock.apply(categorize_transaction, axis=1)
    df_stock['Adjusted_Amount'] = df_stock.apply(adjust_cash_flow_sign, axis=1)
    df_stock['Period'] = df_stock['Activity_Date'].dt.to_period('M')
    
    return df_stock

def categorize_transaction(row):
    """
    Categorizes transactions based on Robinhood's 'Trans Code' column.

    Args:
        row (pd.Series): A row from the DataFrame.

    Returns:
        str: The categorized transaction type (e.g., 'Buy', 'Sell', 'Dividend', or 'Split').
    
    Notes:
        - Other transaction types not explicitly mapped are categorized as 'Other'.
    """
    transaction_map = {'Buy': 'Buy', 'Sell': 'Sell', 'CDIV': 'Dividend', 'SPL': 'Split'}
    return transaction_map.get(row['Trans Code'], 'Other')

def adjust_cash_flow_sign(row):
    """ Adjust Cash Flow sign to reorient fund, portfolio, investment perspective..?"""
    
    if row['Event_Type'] == 'Buy':
        return abs(row['Amount'])
    elif row['Event_Type'] == 'Sell':
        return -abs(row['Amount'])
    return row['Amount']

def calculate_cumulative_shares(data):
    """  Calculates the cumulative number of shares held over time. """
    total_shares = 0
    cumulative_shares = []
    for _, row in data.iterrows():
        if row['Event_Type'] == 'Buy':
            total_shares += row['Quantity']
        elif row['Event_Type'] == 'Sell':
            total_shares -= row['Quantity']
        elif row['Event_Type'] == 'Split' and row['Quantity'] > 0:
            total_shares += row['Quantity']
        cumulative_shares.append(total_shares)
    data['Cumulative_Shares'] = cumulative_shares
    return data

def fetch_historical_prices(stock_symbol, start_date):
    ticker = yf.Ticker(stock_symbol)
    hist = ticker.history(start=start_date, end=datetime.today())
    hist['As_of_Date'] = hist.index.to_period('M')
    return hist

def calculate_wcf_table(transactions, periods):
    wcf_rows = []
    for period in periods:
        period_start = pd.Timestamp(period.start_time)
        period_end = pd.Timestamp(period.end_time)
        T = (period_end - period_start).days + 1
        period_transactions = transactions[transactions['Period'] == period]

        for _, row in period_transactions.iterrows():
            t_i = (row['Activity_Date'] - period_start).days + 1
            W_i = (T - t_i + 1) / T
            weighted_contribution = W_i * row['Adjusted_Amount']
            wcf_rows.append({
                'As_of_Date': period,
                'Activity_Date': row['Activity_Date'],
                'Event_Type': row['Event_Type'],
                'Amount': row['Adjusted_Amount'],
                'T (Days in Period)': T,
                't_i (Days Before Transaction)': t_i,
                'W_i (Weight)': W_i,
                'W_i * F_i (Weighted Contribution)': weighted_contribution
            })
    return pd.DataFrame(wcf_rows)

def calculate_equity_nav(df_equities, hist, all_periods):
    ncf = df_equities[df_equities['Event_Type'].isin(['Buy', 'Sell'])].groupby('Period').agg(
        Net_CF=('Adjusted_Amount', 'sum')
    ).reset_index()

    equities_nav = pd.merge(all_periods, df_equities.groupby('Period').agg(
        Shares_EOM=('Cumulative_Shares', 'last')
    ).reset_index(), left_on='As_of_Date', right_on='Period', how='left')

    equities_nav['Shares_EOM'] = equities_nav['Shares_EOM'].fillna(method='ffill').fillna(0)
    equities_nav['Shares_BOM'] = equities_nav['Shares_EOM'].shift(1, fill_value=0)
    equities_nav['Shares'] = (equities_nav['Shares_BOM'] + equities_nav['Shares_EOM']) / 2

    equities_nav = pd.merge(equities_nav, hist.groupby('As_of_Date').agg(
        BOM_Price=('Close', 'first'),
        EOM_Price=('Close', 'last')
    ).reset_index(), on='As_of_Date', how='left')

    equities_nav['Equity_NAV_BOM'] = equities_nav['BOM_Price'] * equities_nav['Shares_BOM']
    equities_nav['Equity_NAV_EOM'] = equities_nav['EOM_Price'] * equities_nav['Shares_EOM']

    equities_nav = pd.merge(equities_nav, ncf, left_on='As_of_Date', right_on='Period', how='left')
    equities_nav['Net_CF'] = equities_nav['Net_CF'].fillna(0)
    return equities_nav

def calculate_metrics(equities_nav, wcf_table):
    """
    Calculates financial metrics including P&L, Average Capital, and Modified Dietz Return.

    Args:
        equities_nav (pd.DataFrame): The Equity NAV table.
        wcf_table (pd.DataFrame): The Weighted Cash Flow table.

    Returns:
        pd.DataFrame: The Equity NAV table with additional metrics.
    """
    wcf_aggregated = wcf_table.groupby('As_of_Date').agg(
        WCF=('W_i * F_i (Weighted Contribution)', 'sum')
    ).reset_index()

    equities_nav = pd.merge(equities_nav, wcf_aggregated, on='As_of_Date', how='left')
    equities_nav['WCF'] = equities_nav['WCF'].fillna(0)
    equities_nav['P&L'] = equities_nav['Equity_NAV_EOM'] - equities_nav['Equity_NAV_BOM'] - equities_nav['Net_CF']
    equities_nav['Average_Capital'] = equities_nav['Equity_NAV_BOM'] + equities_nav['WCF']
    equities_nav['Modified_Dietz_Return'] = (
        equities_nav['P&L'] / equities_nav['Average_Capital']
    ).replace([float('inf'), -float('inf')], None)
    return equities_nav

def calculate_geometric_returns(equities_nav):
    """
    Adds a column for geometrically linked returns to the Asset Value Table.
    """
    geometric_returns = []
    cumulative_return = 1  # Start with 1 for multiplication
    
    for md_return in equities_nav['Modified_Dietz_Return']:
        if pd.notnull(md_return):  # Ensure valid return
            cumulative_return *= (1 + md_return)  # Chain the returns
        geometric_returns.append(cumulative_return - 1)  # Subtract 1 for percentage format

    equities_nav['Geometric_Linked_Return'] = geometric_returns
    return equities_nav

def main(file_path, stock_symbol):
    """
    Main function to process data and display results.

    Args:
        file_path (str): Path to the CSV file with transaction data.
        stock_symbol (str): Stock ticker symbol to analyze.
    """
    df_stock = load_and_prepare_data(file_path, stock_symbol)
    df_equities = calculate_cumulative_shares(df_stock[df_stock['Event_Type'].isin(['Buy', 'Sell', 'Split'])])
    hist = fetch_historical_prices(stock_symbol, df_stock['Activity_Date'].min())
    all_periods = pd.DataFrame({'As_of_Date': hist['As_of_Date'].unique()})
    wcf_table = calculate_wcf_table(df_equities, all_periods['As_of_Date'])
    equities_nav = calculate_equity_nav(df_equities, hist, all_periods)
    equities_nav = calculate_metrics(equities_nav, wcf_table)
    equities_nav = calculate_geometric_returns(equities_nav)

    # Output results
    print("\nWeighted Cash Flow Table:")
    print(wcf_table)
    print(f"\nTransactions Table for {stock_symbol} (Equities):")
    print(df_equities[['Activity_Date', 'Event_Type', 'Quantity', 'Adjusted_Amount', 'Cumulative_Shares']])
    print(f"\nAsset Value Table for {stock_symbol} (Equities):")
    print(equities_nav[['As_of_Date', 'BOM_Price', 'EOM_Price', 'Shares_BOM', 'Shares_EOM', 
                        'Equity_NAV_BOM', 'Equity_NAV_EOM', 'Net_CF', 'WCF', 
                        'P&L', 'Average_Capital', 'Modified_Dietz_Return', 
                        'Geometric_Linked_Return']])

# Run the script
main('/Users/claytonthompson/Desktop/portfolio_tx.csv', 'AAPL')