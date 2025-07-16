import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Required API key
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing Alpha Vantage API key. Set ALPHAVANTAGE_API_KEY in .env")

# Alpha Vantage function for income statement
def fetch_quarterly_income_statement(symbol):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "INCOME_STATEMENT",
        "symbol": symbol,
        "apikey": ALPHAVANTAGE_API_KEY
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")
    
    data = response.json()
    if "quarterlyReports" not in data:
        raise ValueError("Unexpected API response structure.")
    
    df = pd.DataFrame(data["quarterlyReports"])
    df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"])
    df["totalRevenue"] = pd.to_numeric(df["totalRevenue"], errors="coerce")
    df = df.sort_values("fiscalDateEnding")
    return df[["fiscalDateEnding", "totalRevenue"]]

# Plotting function
def plot_quarterly_revenue(df, symbol):
    plt.figure(figsize=(10, 5))
    plt.plot(df["fiscalDateEnding"], df["totalRevenue"], marker="o", linestyle="-")
    plt.title(f"{symbol.upper()} - Quarterly Total Revenue")
    plt.xlabel("Fiscal Quarter")
    plt.ylabel("Revenue (USD)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Main execution
if __name__ == "__main__":
    symbol = "TOST"
    df_revenue = fetch_quarterly_income_statement(symbol)
    plot_quarterly_revenue(df_revenue, symbol)