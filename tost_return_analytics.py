import pandas as pd
from datetime import datetime
import yfinance as yf

# Load the CSV file
file_path = '/Users/claytonthompson/Desktop/portfolio_tx.csv'
df = pd.read_csv(file_path)

# Filter for TOST transactions
df_tost = df[df['Instrument'].str.contains('TOST', na=False)]

# Drop unnecessary columns
df_tost = df_tost.loc[:, ~df_tost.columns.str.contains('^Unnamed')]

# Convert necessary fields
df_tost['Activity Date'] = pd.to_datetime(df_tost['Activity Date'])
df_tost['Amount'] = df_tost['Amount'].replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True).astype(float)
df_tost['Quantity'] = pd.to_numeric(df_tost['Quantity'], errors='coerce')

# Adjust Amount for fund perspective (positive for buys, negative for sells)
def adjust_amount(row):
    if row['Trans Code'] in ['Buy', 'BTO']:  # Buy or Buy to Open
        return -row['Amount']
    elif row['Trans Code'] in ['Sell', 'STC']:  # Sell or Sell to Close
        return row['Amount']
    elif row['Trans Code'] in ['STO']:  # Sell to Open (short position)
        return row['Amount']
    elif row['Trans Code'] in ['BTC']:  # Buy to Close (close short position)
        return -row['Amount']
    return 0

df_tost['Amount'] = df_tost.apply(adjust_amount, axis=1)

# Add Period column (rename to As_of_Date for clarity)
df_tost['As_of_Date'] = df_tost['Activity Date'].dt.to_period('M')

# Add days in period and days before the transaction
df_tost['Period_Start'] = df_tost['As_of_Date'].apply(lambda p: p.start_time)
df_tost['Period_End'] = df_tost['As_of_Date'].apply(lambda p: p.end_time)
df_tost['Days_in_Period'] = (df_tost['Period_End'] - df_tost['Period_Start']).dt.days + 1
df_tost['Days_Before_Tx'] = (df_tost['Activity Date'] - df_tost['Period_Start']).dt.days + 1

# Calculate Weight components
df_tost['Weight_Numerator'] = df_tost['Days_in_Period'] - df_tost['Days_Before_Tx'] + 1
df_tost['Weight'] = df_tost['Weight_Numerator'] / df_tost['Days_in_Period']

# Calculate individual Weighted CF
df_tost['Weighted_CF'] = df_tost['Weight'] * df_tost['Amount']

# Fetch Yahoo Finance Data for TOST equity prices
ticker = yf.Ticker('TOST')
hist = ticker.history(start=df_tost['Activity Date'].min(), end="2024-12-31")
hist['Date'] = hist.index
hist['As_of_Date'] = hist['Date'].dt.to_period('M')

# Period-Level Table from Yahoo Finance
period_data = hist.groupby('As_of_Date').agg(
    BOM_Price=('Close', 'first'),
    EOM_Price=('Close', 'last')
).reset_index()

# Ensure complete records for each month starting from the first transaction
all_periods = pd.period_range(start=df_tost['As_of_Date'].min(), end="2024-12", freq='M')
period_data = period_data.set_index('As_of_Date').reindex(all_periods).reset_index()
period_data.rename(columns={'index': 'As_of_Date'}, inplace=True)

# Fix Net_Tx to represent net quantity change in each period
def calculate_net_tx(row):
    if row['Trans Code'] in ['Buy', 'BTO']:
        return row['Quantity']
    elif row['Trans Code'] in ['Sell', 'STC']:
        return -row['Quantity']
    elif row['Trans Code'] in ['STO']:
        return -row['Quantity']
    elif row['Trans Code'] in ['BTC']:
        return row['Quantity']
    return 0

df_tost['Net_Tx'] = df_tost.apply(calculate_net_tx, axis=1)

# Group transactions by Period and calculate Total_CF properly
tx_grouped = df_tost.groupby('As_of_Date').agg(
    Total_CF=('Amount', 'sum'),  # Sum of inflows and outflows
    WCF=('Weighted_CF', 'sum'),  # Sum of weighted cash flows
    Net_Tx=('Net_Tx', 'sum')     # Net quantity change
).reset_index()

# Merge transaction data with period table
period_data = pd.merge(period_data, tx_grouped, on='As_of_Date', how='left').fillna(0)

# Correct Cumulative_Net_Tx calculation
period_data['Cumulative_Net_Tx'] = period_data['Net_Tx'].cumsum()

# Update NAV_BOM and NAV_EOM calculations
period_data['NAV_BOM'] = period_data['BOM_Price'] * period_data['Cumulative_Net_Tx'].shift(fill_value=0)
period_data['NAV_EOM'] = period_data['EOM_Price'] * period_data['Cumulative_Net_Tx']

# Calculate P&L and Average Capital for sanity checks
period_data['P&L'] = period_data['NAV_EOM'] - period_data['NAV_BOM'] - period_data['Total_CF']
period_data['Average_Capital'] = period_data['NAV_BOM'] + period_data['WCF']

# Calculate Modified Dietz Return
period_data['Modified_Dietz_Return'] = (
    period_data['P&L'] / period_data['Average_Capital']
).replace([float('inf'), -float('inf')], None)

# Add Geometrically Linked Return (No Reset)
period_data['Geometrically_Linked_Return'] = (
    (1 + period_data['Modified_Dietz_Return'] / 100).cumprod() - 1
) * 100

# Formatting for output
period_data['BOM_Price'] = period_data['BOM_Price'].round(2)
period_data['EOM_Price'] = period_data['EOM_Price'].round(2)
period_data['NAV_BOM'] = period_data['NAV_BOM'].round(2)
period_data['NAV_EOM'] = period_data['NAV_EOM'].round(2)
period_data['P&L'] = period_data['P&L'].round(3)
period_data['Average_Capital'] = period_data['Average_Capital'].round(3)
period_data['Modified_Dietz_Return'] = period_data['Modified_Dietz_Return'].round(2)
period_data['Geometrically_Linked_Return'] = period_data['Geometrically_Linked_Return'].round(2)

# Display the updated Period-Level Data
print("\nUpdated Period-Level Data:")
print(period_data[['As_of_Date', 'BOM_Price', 'EOM_Price', 'Total_CF', 'WCF', 'Net_Tx',
                   'Cumulative_Net_Tx', 'NAV_BOM', 'NAV_EOM', 'P&L', 'Average_Capital',
                   'Modified_Dietz_Return', 'Geometrically_Linked_Return']])