import yfinance as yf
import pandas as pd
from datetime import datetime

def download_option_chain_data(ticker_symbol):
    """
    Download the entire option chain (all expirations) for a ticker
    and return as one combined DataFrame (calls + puts).
    """
    ticker = yf.Ticker(ticker_symbol)
    all_expirations = ticker.options
    
    records = []

    for exp in all_expirations:
        # For each expiration date, fetch calls + puts
        opt_chain = ticker.option_chain(exp)
        
        calls = opt_chain.calls.copy()
        calls['optionType'] = 'call'
        calls['expirationDate'] = exp
        
        puts = opt_chain.puts.copy()
        puts['optionType'] = 'put'
        puts['expirationDate'] = exp
        
        records.append(calls)
        records.append(puts)

    # Combine everything
    full_chain = pd.concat(records, ignore_index=True)
    return full_chain

if __name__ == "__main__":
    ticker_symbol = "NU"
    
    # 1. Fetch the entire chain
    chain_df = download_option_chain_data(ticker_symbol)
    
    # 2. Optionally filter only the contract you want (NU250321C00011000).
    #    But let's store everything for historical tracking:
    # filtered = chain_df[chain_df['contractSymbol'] == 'NU250321C00011000']
    
    # 3. Save to CSV with a timestamp or load into a DB
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{ticker_symbol}_option_chain_{timestamp_str}.csv"
    
    chain_df.to_csv(file_name, index=False)
    print(f"Saved option chain to {file_name}")