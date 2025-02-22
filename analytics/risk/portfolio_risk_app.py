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

import streamlit as st
import plotly.express as px

# -----------------------------------------------------------------------
# 1. Logging & Env Setup
# -----------------------------------------------------------------------

# Optional: Configure logging level (if you want terminal logs).
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
        return None


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

    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
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
        df = yf.download(ticker, start=start_date, end=end_date,
                         progress=False, auto_adjust=True)
    except Exception as e:
        logging.error("Error fetching data for %s: %s", ticker, e)
        return pd.Series(dtype='float64')

    if df.empty:
        logging.warning("No data returned for %s in the given date range.", ticker)
        return pd.Series(dtype='float64')

    returns = df['Close'].pct_change().dropna()
    return returns


def compute_instrument_beta_idio_vol(
    instrument_returns: pd.Series,
    market_returns: pd.Series
) -> Tuple[float, float]:
    """
    Computes (beta, daily_idio_vol) by regressing instrument_returns on market_returns.
    daily_idio_vol is the std dev of residuals from the OLS regression.
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

    try:
        last_close = float(df['Close'].iloc[-1])
    except (TypeError, ValueError) as err:
        logging.warning("Could not parse final row for %s: %s", ticker, err)
        return np.nan

    return last_close


# -----------------------------------------------------------------------
# 4. Main Streamlit App
# -----------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Portfolio Risk Dashboard", layout="wide")
    st.title("Portfolio Risk Dashboard")

    # A) Retrieve Open Positions from DB
    positions_df = get_open_positions()
    if positions_df.empty:
        st.error("No open positions found in fifo_equity_lots.")
        return

    # B) Define date range for returns (~2 years)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365*2)

    # C) Fetch daily returns for SPY (market)
    spy_returns = fetch_daily_returns("SPY", start_date, end_date)
    if spy_returns.empty:
        st.error("No market data (SPY) retrieved. Cannot proceed.")
        return

    market_vol = spy_returns.std()

    # D) Compute Betas, Idio Vol, etc. for each instrument
    results = []
    for idx, row in positions_df.iterrows():
        instr = row['instrument']
        shares = float(row['total_open_quantity'])

        inst_returns = fetch_daily_returns(instr, start_date, end_date)
        if inst_returns.empty:
            st.warning(f"Skipping {instr} due to no return data.")
            continue

        beta, daily_idio_vol = compute_instrument_beta_idio_vol(inst_returns, spy_returns)

        curr_price = fetch_current_price(instr)
        if np.isnan(curr_price):
            st.warning(f"Skipping {instr} due to no current price.")
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

    results_df = pd.DataFrame(results)
    if results_df.empty:
        st.warning("No valid instrument data to compute portfolio metrics.")
        return

    # ---------------------------
    # Rename columns for better display (Instrument-Level)
    # ---------------------------
    col_map_instrument = {
        'instrument': 'Ticker',
        'shares': 'Shares',
        'beta': 'Beta',
        'daily_idio_vol': 'Daily Idio Vol',
        'current_price': 'Current Price',
        'net_market_value': 'Net Mkt Value (USD)',
        'dollar_beta': 'Dollar Beta (USD)',
        'dollar_idio_vol': 'Dollar Idio Vol (USD)'
    }
    results_df_aliased = results_df.rename(columns=col_map_instrument)

    st.subheader("Instrument-Level Results")
    st.dataframe(results_df_aliased)

    # Summaries
    portfolio_net_value = results_df['net_market_value'].sum()
    portfolio_dollar_beta = results_df['dollar_beta'].sum()

    if portfolio_net_value > 0:
        portfolio_beta_decimal = portfolio_dollar_beta / portfolio_net_value
    else:
        portfolio_beta_decimal = np.nan

    portfolio_market_component_vol = portfolio_dollar_beta * market_vol
    portfolio_idio_var = (results_df['dollar_idio_vol'] ** 2).sum()
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)  # daily
    portfolio_total_vol = np.sqrt(portfolio_market_component_vol**2 + portfolio_idio_vol**2)

    # Tracking vol (annual)
    annual_tracking_vol_usd = portfolio_idio_vol * np.sqrt(252)
    if portfolio_net_value > 0:
        tracking_error_pct = (annual_tracking_vol_usd / portfolio_net_value) * 100.0
    else:
        tracking_error_pct = np.nan

    # ---------------------------
    # Build your single-row summary, forcing floats, then rename
    # ---------------------------
    portfolio_summary_dict = {
        "portfolio_net_value (USD)": float(portfolio_net_value),
        "portfolio_dollar_beta (USD)": float(portfolio_dollar_beta),
        "portfolio_beta_decimal": float(portfolio_beta_decimal) if not pd.isna(portfolio_beta_decimal) else np.nan,
        "daily_market_vol (decimal)": float(market_vol),
        "market_component_vol_usd": float(portfolio_market_component_vol),
        "portfolio_idio_var_usd2": float(portfolio_idio_var),
        "portfolio_idio_vol_usd (daily)": float(portfolio_idio_vol),
        "portfolio_total_vol_usd (daily)": float(portfolio_total_vol),
        "annual_tracking_vol_usd": float(annual_tracking_vol_usd),
        "tracking_error (%)": float(tracking_error_pct),
    }
    summary_df = pd.DataFrame([portfolio_summary_dict])

    # 2) rename columns => user-friendly
    alias_summary = {
        "portfolio_net_value (USD)": "Net Portfolio Value ($)",
        "portfolio_dollar_beta (USD)": "Total Dollar Beta ($)",
        "portfolio_beta_decimal": "Portfolio Beta (ratio)",
        "daily_market_vol (decimal)": "Daily Market Vol (decimal)",
        "market_component_vol_usd": "Market Component Vol ($)",
        "portfolio_idio_var_usd2": "Idio Variance ($^2)",
        "portfolio_idio_vol_usd (daily)": "Daily Idio Vol ($)",
        "portfolio_total_vol_usd (daily)": "Daily Portfolio Vol ($)",
        "annual_tracking_vol_usd": "Annual Tracking Vol ($)",
        "tracking_error (%)": "Tracking Error (%)"
    }
    summary_df_aliased = summary_df.rename(columns=alias_summary)

    # 3) transpose
    transposed_df = summary_df_aliased.T.reset_index()
    transposed_df.columns = ["Metric", "Value"]

    st.subheader("Portfolio-Level Summary")
    st.dataframe(transposed_df)

    # ---------------------------
    # Plotly bar charts
    # ---------------------------
    st.subheader("Net Market Value by Instrument")
    # Sort to have largest bars first
    df_sorted_nmv = results_df_aliased.sort_values("Net Mkt Value (USD)", ascending=False)
    fig_nmv = px.bar(
        df_sorted_nmv,
        x="Ticker",
        y="Net Mkt Value (USD)",
        title="Net Market Value (USD)",
        color="Ticker",
        labels={"Ticker": "Instrument", "Net Mkt Value (USD)": "Net Mkt Value (USD)"}
    )
    st.plotly_chart(fig_nmv, use_container_width=True)

    st.subheader("Dollar Beta by Instrument")
    df_sorted_beta = results_df_aliased.sort_values("Dollar Beta (USD)", ascending=False)
    fig_beta = px.bar(
        df_sorted_beta,
        x="Ticker",
        y="Dollar Beta (USD)",
        title="Dollar Beta (USD Exposure)",
        color="Ticker",
        labels={"Ticker": "Instrument", "Dollar Beta (USD)": "Dollar Beta (USD)"}
    )
    st.plotly_chart(fig_beta, use_container_width=True)

    st.success("Done! Review the tables and charts above.")


if __name__ == "__main__":
    main()

# streamlit run analytics/risk/portfolio_risk_app.py