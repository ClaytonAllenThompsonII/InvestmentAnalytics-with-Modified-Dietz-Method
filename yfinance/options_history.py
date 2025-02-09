import yfinance as yf

# The specific contract symbol for NU Mar 21 2025 11.0 Call
contract_symbol = "NU250321C00011000"

# Create a Ticker object for the option contract
option_ticker = yf.Ticker(contract_symbol)

# Attempt to fetch the maximum available history
history_df = option_ticker.history(period="max")

print(history_df)