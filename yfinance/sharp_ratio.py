import pandas as pd
import numpy as np

csv_file = "/Users/claytonthompson/Desktop/Source/InvestmentAnalytics with Modified Dietz Method/NU_option_chain_20250130_175831.csv"
df = pd.read_csv(csv_file)

# Ensure Date is datetime
df['Date'] = pd.to_datetime(df['Date'])
df.sort_values(by='Date', inplace=True)

purchase_date = pd.to_datetime("2024-12-18")
df = df[df['Date'] >= purchase_date].copy()

# If you need to force that day's price to 1.09:
df.loc[df['Date'] == purchase_date, 'Close'] = 1.09

df['Close'] = df['Close'].astype(float)

# Compute daily returns
df['Daily_Return'] = df['Close'].pct_change()
df.dropna(subset=['Daily_Return'], inplace=True)

mean_daily_return = df['Daily_Return'].mean()
std_daily_return = df['Daily_Return'].std()

# Annualization factor for daily data
days_per_year = 252
annualized_return = mean_daily_return * days_per_year
annualized_vol = std_daily_return * np.sqrt(days_per_year)

sharpe_ratio = annualized_return / annualized_vol

print(f"Annualized Return: {annualized_return:.4f}")
print(f"Annualized Volatility: {annualized_vol:.4f}")
print(f"Sharpe Ratio: {sharpe_ratio:.4f}")