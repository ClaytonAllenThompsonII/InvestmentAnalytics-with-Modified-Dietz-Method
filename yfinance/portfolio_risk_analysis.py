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


# portfolio_risk_analysis.py

def compute_portfolio_risk_summary():
    """
    Runs all the logic (DB query for open positions, yfinance fetch,
    OLS Beta, etc.) and returns:
      - instrument-level DataFrame
      - portfolio-level summary (dict or DataFrame)
    """
    # A) Get open positions from DB
    positions_df = get_open_positions()
    if positions_df.empty:
        logging.error("No open positions found.")
        return pd.DataFrame(), {}

    # B) Date range for returns
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365*2)

    # C) Fetch SPY returns
    spy_returns = fetch_daily_returns("SPY", start_date, end_date)
    if spy_returns.empty:
        logging.error("No market data (SPY).")
        return pd.DataFrame(), {}

    market_vol = spy_returns.std()

    # D) Instrument-level loop
    results = []
    for idx, row in positions_df.iterrows():
        instr = row['instrument']
        shares = float(row['total_open_quantity'])

        # 1) instrument daily returns
        inst_returns = fetch_daily_returns(instr, start_date, end_date)
        if inst_returns.empty:
            continue

        # 2) Beta & daily idiosyncratic vol
        beta, daily_idio_vol = compute_instrument_beta_idio_vol(inst_returns, spy_returns)

        # 3) Current price
        curr_price = fetch_current_price(instr)
        if np.isnan(curr_price):
            continue

        net_market_value = shares * curr_price
        dollar_beta = beta * net_market_value
        dollar_idio_vol = daily_idio_vol * net_market_value

        results.append({
            'instrument': instr,
            'shares': shares,
            'beta': beta,
            'daily_idio_vol': daily_idio_vol,
            'current_price': curr_price,
            'net_market_value': net_market_value,
            'dollar_beta': dollar_beta,
            'dollar_idio_vol': dollar_idio_vol
        })

    if not results:
        logging.error("No valid instrument data.")
        return pd.DataFrame(), {}

    df_instrument = pd.DataFrame(results)

    # E) Portfolio metrics
    portfolio_net_value = df_instrument['net_market_value'].sum()
    portfolio_dollar_beta = df_instrument['dollar_beta'].sum()
    portfolio_beta_decimal = (
        portfolio_dollar_beta / portfolio_net_value
        if portfolio_net_value > 0 else np.nan
    )

    portfolio_market_component_vol = portfolio_dollar_beta * market_vol
    portfolio_idio_var = (df_instrument['dollar_idio_vol'] ** 2).sum()
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)

    portfolio_total_vol = np.sqrt(
        portfolio_market_component_vol**2 + portfolio_idio_vol**2
    )

    portfolio_summary = {
        "total_net_market_value": portfolio_net_value,
        "portfolio_dollar_beta": portfolio_dollar_beta,
        "portfolio_beta_decimal": portfolio_beta_decimal,
        "daily_market_vol": market_vol,
        "market_component_vol_usd": portfolio_market_component_vol,
        "portfolio_idio_var_usd2": portfolio_idio_var,
        "portfolio_idio_vol_usd": portfolio_idio_vol,
        "portfolio_total_vol_usd": portfolio_total_vol
    }

    return df_instrument, portfolio_summary

# -----------------------------------------------------------------------
# 4. Main Calculation Logic
# -----------------------------------------------------------------------

def main():
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
        total_cost = float(row['total_cost'])  # optional usage

        # 1) fetch daily returns for this instrument
        inst_returns = fetch_daily_returns(instrument, start_date, end_date)
        if inst_returns.empty:
            logging.warning("Skipping %s due to no return data.", instrument)
            continue

        # 2) regress to get Beta, daily idio vol
        beta, daily_idio_vol = compute_instrument_beta_idio_vol(inst_returns, spy_returns)

        # 3) fetch current price to get net market value
        curr_price = fetch_current_price(instrument)
        if np.isnan(curr_price):
            logging.warning("Skipping %s due to no current price.", instrument)
            continue

        net_market_value = total_shares * curr_price  # rename from position_value

        # 4) dollar Beta
        dollar_beta = beta * net_market_value

        # 5) dollar idiosyncratic vol
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
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)

    # 6) Total portfolio vol (assuming zero correlation with market)
    portfolio_total_vol = np.sqrt(
        portfolio_market_component_vol**2 + portfolio_idio_vol**2
    )

    # F) Print output
    logging.info("=== Instrument-Level Results ===")
    logging.info("\n%s", results_df.to_string(index=False))

    logging.info("\n=== Portfolio-Level Summary ===")
    logging.info("Total Net Market Value (USD):        %.2f", portfolio_net_value)
    logging.info("Portfolio Dollar Beta:              %.2f", portfolio_dollar_beta)
    logging.info("Portfolio Beta (decimal):           %.3f", portfolio_beta_decimal)
    logging.info("Daily Market Vol (decimal):         %.3f%%", (market_vol*100))
    logging.info("Market Component of Vol (USD):      %.2f", portfolio_market_component_vol)

    # Show the new portfolio idio variance
    logging.info("Portfolio Idio Variance (USD^2):    %.2f", portfolio_idio_var)
    logging.info("Idiosyncratic Vol (USD):            %.2f", portfolio_idio_vol)

    logging.info("Total Portfolio Vol (USD):          %.2f", portfolio_total_vol)


# -----------------------------------------------------------------------
# 5. Script Entry Point
# -----------------------------------------------------------------------
if __name__ == "__main__":
    main()