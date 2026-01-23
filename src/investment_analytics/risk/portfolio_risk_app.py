#!/usr/bin/env python3

import os
import logging
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import statsmodels.api as sm
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Database credentials
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# DB connection helper
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Fetch open positions from DB
def get_open_positions():
    query = """
        SELECT
            instrument,
            SUM(open_quantity) AS total_open_quantity,
            SUM(total_cost) AS total_cost
        FROM fifo_equity_lots
        WHERE open_quantity != 0
        GROUP BY instrument
        ORDER BY instrument;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Fetch daily returns from market_data table
def fetch_daily_returns_db(symbol, start_date, end_date):
    query = """
        SELECT price_date, close_price
        FROM market_data
        WHERE instrument = %s
          AND price_date BETWEEN %s AND %s
        ORDER BY price_date;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))
    conn.close()

    df.set_index('price_date', inplace=True)
    returns = df['close_price'].pct_change().dropna()
    return returns

# Regression helper
def compute_beta_idio_vol(instrument_returns, market_returns):
    data = pd.concat([instrument_returns, market_returns], axis=1).dropna()
    data.columns = ['r_instrument', 'r_market']

    if len(data) < 2:
        return np.nan, np.nan

    X = sm.add_constant(data['r_market'])
    y = data['r_instrument']
    model = sm.OLS(y, X).fit()

    beta = model.params['r_market']
    resid_std = model.resid.std()

    return beta, resid_std

# Fetch current price from market_data table
def fetch_current_price_db(symbol):
    query = """
        SELECT close_price
        FROM market_data
        WHERE instrument = %s
        ORDER BY price_date DESC
        LIMIT 1;
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, (symbol,))
    price = cur.fetchone()
    conn.close()

    return price[0] if price else np.nan

# Streamlit app
def main():
    st.set_page_config(page_title="Portfolio Risk Dashboard", layout="wide")
    st.title("Portfolio Risk Dashboard")

    positions_df = get_open_positions()
    if positions_df.empty:
        st.error("No open positions found.")
        return

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=365 * 2)

    spy_returns = fetch_daily_returns_db("SPY", start_date, end_date)
    if spy_returns.empty:
        st.error("No SPY market data available.")
        return

    market_vol = spy_returns.std()

    results = []
    for _, row in positions_df.iterrows():
        instr = row['instrument']
        shares = row['total_open_quantity']

        inst_returns = fetch_daily_returns_db(instr, start_date, end_date)
        if inst_returns.empty:
            st.warning(f"No return data for {instr}, skipping.")
            continue

        beta, daily_idio_vol = compute_beta_idio_vol(inst_returns, spy_returns)

        curr_price = fetch_current_price_db(instr)
        if np.isnan(curr_price):
            st.warning(f"No current price for {instr}, skipping.")
            continue

        net_market_value = shares * curr_price
        dollar_beta = beta * net_market_value
        dollar_idio_vol = daily_idio_vol * net_market_value

        results.append({
            'Ticker': instr,
            'Shares': shares,
            'Beta': beta,
            'Daily Idio Vol': daily_idio_vol,
            'Current Price': curr_price,
            'Net Mkt Value (USD)': net_market_value,
            'Dollar Beta (USD)': dollar_beta,
            'Dollar Idio Vol (USD)': dollar_idio_vol
        })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        st.warning("No valid data available.")
        return

    st.subheader("Instrument-Level Results")
    st.dataframe(results_df)

    portfolio_net_value = results_df['Net Mkt Value (USD)'].sum()
    portfolio_dollar_beta = results_df['Dollar Beta (USD)'].sum()
    portfolio_beta_decimal = portfolio_dollar_beta / portfolio_net_value if portfolio_net_value else np.nan
    portfolio_market_component_vol = portfolio_dollar_beta * market_vol
    portfolio_idio_var = (results_df['Dollar Idio Vol (USD)'] ** 2).sum()
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)
    portfolio_total_vol = np.sqrt(portfolio_market_component_vol**2 + portfolio_idio_vol**2)
    annual_tracking_vol_usd = portfolio_idio_vol * np.sqrt(252)
    tracking_error_pct = (annual_tracking_vol_usd / portfolio_net_value) * 100 if portfolio_net_value else np.nan

    summary_df = pd.DataFrame([{
        "Net Portfolio Value ($)": portfolio_net_value,
        "Total Dollar Beta ($)": portfolio_dollar_beta,
        "Portfolio Beta (ratio)": portfolio_beta_decimal,
        "Daily Market Vol (decimal)": market_vol,
        "Market Component Vol ($)": portfolio_market_component_vol,
        "Idio Variance ($^2)": portfolio_idio_var,
        "Daily Idio Vol ($)": portfolio_idio_vol,
        "Daily Portfolio Vol ($)": portfolio_total_vol,
        "Annual Tracking Vol ($)": annual_tracking_vol_usd,
        "Tracking Error (%)": tracking_error_pct
    }]).T.reset_index()

    summary_df.columns = ["Metric", "Value"]

    st.subheader("Portfolio-Level Summary")
    st.dataframe(summary_df)

    fig_nmv = px.bar(results_df, x="Ticker", y="Net Mkt Value (USD)", color="Ticker")
    st.plotly_chart(fig_nmv, use_container_width=True)

    fig_beta = px.bar(results_df, x="Ticker", y="Dollar Beta (USD)", color="Ticker")
    st.plotly_chart(fig_beta, use_container_width=True)

    st.success("Dashboard Updated.")

if __name__ == "__main__":
    main()


# streamlit run analytics/risk/portfolio_risk_app.py