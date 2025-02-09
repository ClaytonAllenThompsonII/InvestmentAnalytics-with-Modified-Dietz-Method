import yfinance as yf

# 1. Create a Ticker object for the underlying
ticker = yf.Ticker("NU")

# 2. Check available options expiration dates
expirations = ticker.options
print("Available Expiration Dates:", expirations)

# We expect "2025-03-21" to be one of them, but let's just store it
desired_expiration = "2025-03-21"
if desired_expiration not in expirations:
    raise ValueError(f"Desired expiration {desired_expiration} not found in available dates.")

# 3. Get the entire option chain for that date
option_chain = ticker.option_chain(desired_expiration)

calls = option_chain.calls
puts = option_chain.puts  # Just for reference if needed

# 4. Filter calls for strike == 11.0
target_call = calls[calls['strike'] == 11.0]

print("Filtered Option Chain Data for NU Mar 21, 2025 $11 Calls:")
print(target_call)