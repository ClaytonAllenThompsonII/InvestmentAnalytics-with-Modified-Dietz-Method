#!/usr/bin/env python3

import os
import logging
import psycopg2
import pandas as pd
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Database credentials
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# DB connection
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Fetch open positions
def get_open_positions():
    query = """
        SELECT DISTINCT instrument
        FROM fifo_equity_lots
        WHERE open_quantity > 0
        ORDER BY instrument;
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df["instrument"].tolist()

# Fetch daily returns from market_data table
def fetch_daily_returns_db(symbol: str, start_date: datetime, end_date: datetime) -> pd.Series:
    query = """
        SELECT price_date, close_price
        FROM market_data
        WHERE instrument = %s
          AND price_date BETWEEN %s AND %s
        ORDER BY price_date;
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))

    if df.empty:
        return pd.Series(dtype=float)

    df.set_index('price_date', inplace=True)
    returns = df['close_price'].pct_change().dropna()
    return returns

# Run regression and plot
def run_regression_and_plot(df: pd.DataFrame, x_col: str, y_col: str,
                            start_date: datetime, end_date: datetime,
                            target_ticker: str) -> None:
    X = sm.add_constant(df[x_col])
    y = df[y_col]
    model = sm.OLS(y, X).fit()

    alpha = model.params['const']
    beta = model.params[x_col]

    print("\nRegression Results:")
    print(f"  Alpha (Intercept): {alpha:.6f}")
    print(f"  Beta (Slope):      {beta:.6f}")
    print(model.summary())

    # Minimalist Plotting
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(df[x_col], df[y_col], color='steelblue', alpha=0.5, label='Daily Observations')

    x_vals = np.linspace(df[x_col].min(), df[x_col].max(), 100)
    y_vals = alpha + beta * x_vals
    ax.plot(x_vals, y_vals, color='darkorange', linewidth=2, label='Regression Line')

    ax.set_title(f"{target_ticker} vs SPY Daily Returns\n({start_date.date()} to {end_date.date()})")
    ax.set_xlabel("SPY Daily Return")
    ax.set_ylabel(f"{target_ticker} Daily Return")
    ax.legend()
    plt.tight_layout()
    plt.show()

# Main logic
def main():
    instruments = get_open_positions()
    if not instruments:
        print("No open positions found.")
        return

    print("\n📊 Available Open Positions:")
    for idx, name in enumerate(instruments, start=1):
        print(f"{idx}. {name}")

    selection = input("\nEnter the number of the instrument to analyze: ")
    try:
        choice = int(selection)
        assert 1 <= choice <= len(instruments)
    except Exception:
        print("Invalid selection.")
        return

    target_ticker = instruments[choice - 1]

    end_date = datetime.today()
    start_date = end_date - timedelta(days=365 * 2)

    r_target = fetch_daily_returns_db(target_ticker, start_date, end_date)
    r_spy = fetch_daily_returns_db("SPY", start_date, end_date)

    if r_target.empty or r_spy.empty:
        print("Missing data for selected instrument or SPY.")
        return

    df = pd.concat([r_target, r_spy], axis=1).dropna()
    df.columns = ['R_TARGET', 'R_SPY']

    if len(df) < 2:
        print("Not enough overlapping data for regression.")
        return

    run_regression_and_plot(df, x_col='R_SPY', y_col='R_TARGET',
                            start_date=start_date, end_date=end_date,
                            target_ticker=target_ticker)

if __name__ == "__main__":
    main()