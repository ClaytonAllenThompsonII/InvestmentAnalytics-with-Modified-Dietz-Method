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
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
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
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))

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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            result = cur.fetchone()
    return float(result[0]) if result else np.nan

def compute_daily_pnl_attribution(results_df, spy_returns, selected_date):
    r_m_series = spy_returns.loc[spy_returns.index == selected_date]
    if r_m_series.empty:
        return pd.DataFrame()
    r_m = r_m_series.values[0]

    pnl_rows = []
    for _, row in results_df.iterrows():
        ticker = row['Ticker']
        beta = row['Beta']
        nmv = row['Net Mkt Value (USD)']

        # Get today's return for this instrument
        r_i_series = fetch_daily_returns_db(ticker, selected_date - timedelta(days=5), selected_date + timedelta(days=1))
        if selected_date not in r_i_series:
            continue
        r_i = r_i_series.loc[selected_date]

        # Attribution
        market_component = beta * r_m * nmv
        idio_component = (r_i - beta * r_m) * nmv
        total_pnl = market_component + idio_component

        pnl_rows.append({
            'Ticker': ticker,
            'Net Mkt Value (USD)': nmv,
            'r_i': r_i,
            'r_m': r_m,
            'Beta': beta,
            'Market PnL (USD)': market_component,
            'Idio PnL (USD)': idio_component,
            'Total PnL (USD)': total_pnl
        })

    return pd.DataFrame(pnl_rows)
# Streamlit app
def main():
    st.set_page_config(page_title="Portfolio Volatility Dashboard", layout="wide")
    st.title("Portfolio Volatility Dashboard")
    st.markdown("Select a date to attribute daily PnL:")
    selected_date = st.date_input(
        "Attribution Date",
        value=datetime.today().date() - timedelta(days=1),
        max_value=datetime.today().date(),
        help="PnL will be attributed as of this date."
    )

    positions_df = get_open_positions()
    if positions_df.empty:
        st.error("No open positions found.")
        return

    # --- Sidebar for Beta Lookback Window ---
    lookback_options = {
        "6 Months": 180,
        "1 Year": 365,
        "2 Years": 365 * 2,
        "3 Years": 365 * 3
    }
    lookback_label = st.sidebar.selectbox("Select Lookback Window for Beta & Volatility", options=list(lookback_options.keys()), index=2)
    lookback_days = lookback_options[lookback_label]

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=lookback_days)

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
            'Current Price': curr_price,
            'Shares': shares,
            'Beta': beta,
            'Daily Idio Vol': daily_idio_vol,
            'Net Mkt Value (USD)': net_market_value,
            'Dollar Beta (USD)': dollar_beta,
            'Dollar Idio Vol (USD)': dollar_idio_vol
        })

    results_df = pd.DataFrame(results)
    # --- Compute Daily PnL Attribution ---
    pnl_df = compute_daily_pnl_attribution(results_df, spy_returns, selected_date)
    if not pnl_df.empty:
        st.subheader("Daily PnL Attribution (Market vs Idiosyncratic)")
        st.dataframe(pnl_df.style.format({
            'r_i': "{:.2%}",
            'r_m': "{:.2%}",
            'Market PnL (USD)': "${:,.2f}",
            'Idio PnL (USD)': "${:,.2f}",
            'Total PnL (USD)': "${:,.2f}"
        }), use_container_width=True)

        st.plotly_chart(
            px.bar(pnl_df, x="Ticker", y=["Market PnL (USD)", "Idio PnL (USD)"],
                barmode="stack", title="PnL Attribution by Ticker"),
            use_container_width=True
        )
    else:
        st.info("No PnL attribution available for today.")
    if results_df.empty:
        st.warning("No valid data available.")
        return

    st.subheader("Risk Decomposition")
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

    summary_df = pd.DataFrame.from_dict({
        "Net Portfolio Value ($)": [portfolio_net_value],
        "Total Dollar Beta ($)": [portfolio_dollar_beta],
        "Portfolio Beta (ratio)": [portfolio_beta_decimal],
        "Daily Market Vol (decimal)": [market_vol],
        "Market Component Vol ($)": [portfolio_market_component_vol],
        "Idio Variance ($^2)": [portfolio_idio_var],
        "Daily Idio Vol ($)": [portfolio_idio_vol],
        "Daily Portfolio Vol ($)": [portfolio_total_vol],
        "Annual Tracking Vol ($)": [annual_tracking_vol_usd],
        "Tracking Error (%)": [tracking_error_pct]
    }, orient='index', columns=['Value']).reset_index().rename(columns={'index': 'Metric'})

    st.subheader("Portfolio-Level Summary")
    st.dataframe(summary_df)

    st.plotly_chart(px.bar(results_df, x="Ticker", y="Net Mkt Value (USD)", color="Ticker"), use_container_width=True)
    st.plotly_chart(px.bar(results_df, x="Ticker", y="Dollar Beta (USD)", color="Ticker"), use_container_width=True)

    st.success("Dashboard Updated.")

if __name__ == "__main__":
    main()

    # streamlit run src/investment_analytics/risk/portfolio_risk_app_2.py