#!/usr/bin/env python3

import os
import logging
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf
import statsmodels.api as sm
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple

import matplotlib.pyplot as plt

# -----------------------------------------------------------------------
# 1. Logging & Env Setup
# -----------------------------------------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Load environment variables from .env
load_dotenv()

# Read DB credentials from environment
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    """
    Create a psycopg2 connection to the Postgres database.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise


# -----------------------------------------------------------------------
# 2. Database Query to Get Open Positions
# -----------------------------------------------------------------------

def get_open_positions():
    """
    Fetches open positions from fifo_equity_lots, grouped by instrument.
    Returns a DataFrame with columns: [instrument, total_open_quantity, total_cost].
    """
    query = """
        SELECT
            instrument,
            SUM(open_quantity) AS total_open_quantity,
            SUM(total_cost)    AS total_cost
        FROM fifo_equity_lots
        WHERE open_quantity != 0
        GROUP BY instrument
        ORDER BY instrument;
    """

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        return df

    except Exception as e:
        logging.error("Error fetching open positions: %s", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()


# -----------------------------------------------------------------------
# 3. Market Data & Regression Helpers
# -----------------------------------------------------------------------

def fetch_daily_returns(ticker: str, start_date: datetime, end_date: datetime) -> pd.Series:
    """
    Downloads (auto-adjusted) close prices for the given ticker (via yfinance)
    and returns a Series of daily % returns from the 'Close' column.
    """
    try:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
    except Exception as e:
        logging.error("Error fetching data for %s: %s", ticker, e)
        return pd.Series(dtype='float64')

    if df.empty:
        logging.warning("No data returned for %s in the given date range.", ticker)
        return pd.Series(dtype='float64')

    returns = df['Close'].pct_change().dropna()
    return returns


def compute_instrument_beta_idio_vol(instrument_returns: pd.Series,
                                     market_returns: pd.Series) -> Tuple[float, float]:
    """
    Computes (beta, daily_idio_vol) by regressing instrument_returns on market_returns.
    daily_idio_vol is the standard deviation of residuals from the OLS regression.
    """
    data = pd.concat([instrument_returns, market_returns], axis=1).dropna()
    data.columns = ['r_instrument', 'r_market']

    if len(data) < 2:
        logging.warning("Not enough overlapping data for regression.")
        return np.nan, np.nan

    X = sm.add_constant(data['r_market'])
    y = data['r_instrument']
    model = sm.OLS(y, X).fit()

    beta = model.params['r_market']
    resid_std = model.resid.std()  # daily idio vol

    return beta, resid_std


def fetch_current_price(ticker: str) -> float:
    """
    Fetch the most recent adjusted close price (1d) from yfinance.
    """
    try:
        df = yf.download(ticker, period="1d", progress=False, auto_adjust=True)
    except Exception as e:
        logging.error("Error fetching current price for %s: %s", ticker, e)
        return np.nan

    if df.empty:
        logging.warning("No current price for ticker %s.", ticker)
        return np.nan

    # Convert to float to avoid it being a single-element Series
    try:
        last_close = float(df['Close'].iloc[-1])
    except (TypeError, ValueError) as err:
        logging.warning("Could not parse final row for %s: %s", ticker, err)
        return np.nan

    return last_close


# -----------------------------------------------------------------------
# 4. Main Calculation Logic
# -----------------------------------------------------------------------

def main():
    # Minimalist Matplotlib styling (Dieter Rams–inspired)
    plt.rcParams['figure.figsize'] = (10, 5)
    plt.rcParams['axes.facecolor'] = '#fafafa'
    plt.rcParams['axes.edgecolor'] = '#333333'
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.color'] = '#cccccc'
    plt.rcParams['grid.linestyle'] = ':'
    plt.rcParams['axes.spines.top'] = False
    plt.rcParams['axes.spines.right'] = False
    plt.rcParams['axes.spines.left'] = True
    plt.rcParams['axes.spines.bottom'] = True
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.titleweight'] = 'bold'
    plt.rcParams['legend.frameon'] = False

    # A) Get open positions from DB
    positions_df = get_open_positions()
    if positions_df.empty:
        logging.error("No open positions found in fifo_equity_lots.")
        return

    # B) Define date range for returns (2-year window)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365*2)

    # C) Fetch SPY returns for reference (market)
    spy_returns = fetch_daily_returns("SPY", start_date, end_date)
    if spy_returns.empty:
        logging.error("No market data (SPY) retrieved. Cannot proceed.")
        return

    market_vol = spy_returns.std()  # daily market volatility (decimal)

    # D) For each instrument: compute Beta & daily idiosyncratic vol, get net market value, etc.
    results = []
    for idx, row in positions_df.iterrows():
        instrument = row['instrument']
        total_shares = float(row['total_open_quantity'])

        inst_returns = fetch_daily_returns(instrument, start_date, end_date)
        if inst_returns.empty:
            logging.warning("Skipping %s due to no return data.", instrument)
            continue

        beta, daily_idio_vol = compute_instrument_beta_idio_vol(inst_returns, spy_returns)

        curr_price = fetch_current_price(instrument)
        if np.isnan(curr_price):
            logging.warning("Skipping %s due to no current price.", instrument)
            continue

        net_market_value = total_shares * curr_price
        dollar_beta = beta * net_market_value
        dollar_idio_vol = daily_idio_vol * net_market_value

        results.append({
            'instrument': instrument,
            'shares': total_shares,
            'beta': beta,
            'daily_idio_vol': daily_idio_vol,
            'current_price': curr_price,
            'net_market_value': net_market_value,
            'dollar_beta': dollar_beta,
            'dollar_idio_vol': dollar_idio_vol
        })

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    if results_df.empty:
        logging.error("No valid instrument data to compute portfolio metrics.")
        return

    # E) Summaries
    # 1) Total net market value
    portfolio_net_value = results_df['net_market_value'].sum()

    # 2) Summation of Dollar Betas (absolute measure)
    portfolio_dollar_beta = results_df['dollar_beta'].sum()

    # 3) "Portfolio Beta" as ratio
    if portfolio_net_value > 0:
        portfolio_beta_decimal = portfolio_dollar_beta / portfolio_net_value
    else:
        portfolio_beta_decimal = np.nan

    # 4) Market component of vol (in USD)
    portfolio_market_component_vol = portfolio_dollar_beta * market_vol

    # 5) Idio vol in USD (assuming zero correlation among instruments)
    portfolio_idio_var = (results_df['dollar_idio_vol'] ** 2).sum()
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)  # daily

    # 6) Total portfolio vol (assuming zero correlation with market)
    portfolio_total_vol = np.sqrt(
        portfolio_market_component_vol**2 + portfolio_idio_vol**2
    )

    # -------------------------------------------------------------------
    #  New: Tracking Dollar Vol, Annualized, and % of Net Value
    # -------------------------------------------------------------------
    # daily tracking vol in USD = portfolio_idio_vol
    # annualized = daily * sqrt(252)
    annual_tracking_vol_usd = portfolio_idio_vol * np.sqrt(252)

    if portfolio_net_value > 0:
        tracking_error_pct = (annual_tracking_vol_usd / portfolio_net_value) * 100.0
    else:
        tracking_error_pct = np.nan

    # F) Log output to console
    logging.info("=== Instrument-Level Results ===")
    logging.info("\n%s", results_df.to_string(index=False))

    logging.info("\n=== Portfolio-Level Summary ===")
    logging.info("Total Net Market Value (USD):        %.2f", portfolio_net_value)
    logging.info("Portfolio Dollar Beta:              %.2f", portfolio_dollar_beta)
    logging.info("Portfolio Beta (decimal):           %.3f", portfolio_beta_decimal)
    logging.info("Daily Market Vol (decimal):         %.3f%%", (market_vol*100))
    logging.info("Market Component of Vol (USD):      %.2f", portfolio_market_component_vol)
    logging.info("Portfolio Idio Variance (USD^2):    %.2f", portfolio_idio_var)
    logging.info("Idiosyncratic Vol (USD) [Daily]:    %.2f", portfolio_idio_vol)
    logging.info("Total Portfolio Vol (USD) [Daily]:  %.2f", portfolio_total_vol)
    logging.info("---")
    logging.info("Daily Tracking Vol (USD):           %.2f", portfolio_idio_vol)
    logging.info("Annual Tracking Vol (USD):          %.2f", annual_tracking_vol_usd)
    logging.info("Tracking Error (%% of NMV):          %.2f%%", tracking_error_pct)

    # -------------------------------------------------------------------
    #  Visualize: Simple Bar Charts of Net Market Value & Dollar Beta
    # -------------------------------------------------------------------
    results_df.sort_values('net_market_value', ascending=False, inplace=True)

    instruments = results_df['instrument'].tolist()
    x_pos = np.arange(len(instruments))

    # First figure: Net Market Value per instrument
    fig, ax = plt.subplots()
    ax.bar(x_pos, results_df['net_market_value'], color='steelblue', alpha=0.7)
    ax.set_title('Net Market Value by Instrument')
    ax.set_xlabel('Instrument')
    ax.set_ylabel('Net Mkt Value (USD)')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(instruments, rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    # Second figure: Dollar Beta per instrument
    fig2, ax2 = plt.subplots()
    ax2.bar(x_pos, results_df['dollar_beta'], color='darkorange', alpha=0.7)
    ax2.set_title('Dollar Beta by Instrument')
    ax2.set_xlabel('Instrument')
    ax2.set_ylabel('Dollar Beta (USD Exposure)')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(instruments, rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    # (Optional) You could also add a third figure for daily_idio_vol or annual tracking vol if desired.


# -----------------------------------------------------------------------
# 5. Script Entry Point
# -----------------------------------------------------------------------
if __name__ == "__main__":
    main()