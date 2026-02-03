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
    r_m = float(r_m_series.values[0])

    pnl_rows = []
    for _, row in results_df.iterrows():
        ticker = row["Ticker"]
        beta = row["Beta"]
        nmv = row["Net Mkt Value (USD)"]

        # Get today's return for this instrument
        r_i_series = fetch_daily_returns_db(
            ticker,
            selected_date - timedelta(days=5),
            selected_date + timedelta(days=1),
        )
        if selected_date not in r_i_series.index:
            continue
        r_i = float(r_i_series.loc[selected_date])

        # Attribution
        market_component = beta * r_m * nmv
        idio_component = (r_i - beta * r_m) * nmv
        total_pnl = market_component + idio_component

        pnl_rows.append({
            "Ticker": ticker,
            "Net Market Value": nmv,
            "Instrument Return": r_i,
            "Market Return": r_m,
            "Beta": beta,
            "Market PnL": market_component,
            "Idiosyncratic PnL": idio_component,
            "Total PnL": total_pnl,
        })

    return pd.DataFrame(pnl_rows)
# Streamlit app
def main():
    st.set_page_config(page_title="Portfolio Volatility Dashboard", layout="wide")
    st.markdown("""
    <style>
    html, body, [class*="css"] {
        font-family: "Segoe UI", -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif;
        letter-spacing: 0.01em;
    }
    .section-header {
      background-color: #f3f4f6;
      color: #374151;
      text-align: center;
      font-weight: 600;
      padding: 0.15rem 0;
      margin-bottom: 0;
      border-radius: 4px;
      border: 1px solid #e5e7eb;
    }
    .perf-footnote {
      font-size: 0.52rem;
      color: #9ca3af;
      line-height: 1.15;
      margin-top: 0.05rem;
      margin-bottom: 0.9rem;
    }
    .perf-footnote p { margin: 0.05rem 0; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="
      background-color: #f3f4f6;
      padding: 0.75rem 1rem 0.55rem 1rem;
      border-radius: 4px;
    ">
      <h1 style="margin: 0;">Risk Detail, Portfolio Volatility</h1>
      <p style="margin: 0.25rem 0 0; color: #6b7280; font-size: 1.05rem;">
        AS OF {datetime.today().strftime("%B %d, %Y")}
      </p>
    </div>

    <hr style="
      border: none;
      height: 4px;
      background-color: #0021A5;
      margin: 0 0 1.1rem;
    ">
    """, unsafe_allow_html=True)

    positions_df = get_open_positions()
    if positions_df.empty:
        st.error("No open positions found.")
        return

    # --- Global Controls (in main window, not sidebar) ---
    lookback_options = {
        "6 Months": 180,
        "1 Year": 365,
        "2 Years": 365 * 2,
        "3 Years": 365 * 3
    }

    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2], gap="large")

    with ctrl1:
        selected_date = st.date_input(
            "Attribution Date",
            value=datetime.today().date() - timedelta(days=1),
            max_value=datetime.today().date(),
            help="PnL will be attributed as of this date."
        )

    with ctrl2:
        lookback_label = st.selectbox(
            "Lookback Window (Beta & Vol)",
            options=list(lookback_options.keys()),
            index=2
        )
        lookback_days = lookback_options[lookback_label]

    with ctrl3:
        market_symbol = st.selectbox("Market Benchmark", options=["SPY"], index=0)

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=lookback_days)

    spy_returns = fetch_daily_returns_db(market_symbol, start_date, end_date)
    if spy_returns.empty:
        st.error(f"No market data available for {market_symbol}.")
        return

    # Snap selected_date back to most recent market date if needed
    if selected_date not in spy_returns.index:
        prior_dates = spy_returns.index[spy_returns.index < selected_date]
        if len(prior_dates) > 0:
            selected_date = prior_dates.max()

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
    
    # ----------------------------
    # Risk Overview (FIRST)
    # ----------------------------
    st.markdown("""<div class="section-header">Portfolio Risk Overview</div>""", unsafe_allow_html=True)

    # ---- Portfolio summary calcs (same as you already do) ----
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
        "Net Portfolio Value": [portfolio_net_value],
        "Total Dollar Beta": [portfolio_dollar_beta],
        "Portfolio Beta": [portfolio_beta_decimal],
        "Daily Market Vol": [market_vol],
        "Market Component Vol": [portfolio_market_component_vol],
        "Idio Variance": [portfolio_idio_var],
        "Daily Idio Vol": [portfolio_idio_vol],
        "Daily Portfolio Vol": [portfolio_total_vol],
        "Annual Tracking Vol": [annual_tracking_vol_usd],
        "Tracking Error (%)": [tracking_error_pct]
    }, orient='index', columns=['Value']).reset_index().rename(columns={'index': 'Metric'})

    # ---- Row 1: two tables ----
    TABLE_H = 380
    t1, t2 = st.columns([3.25, 1.75], gap="small")

    with t1:
        st.markdown("""<div class="section-header">Risk Decomposition</div>""", unsafe_allow_html=True)

        risk_styler = (
            results_df.style
            .format({
                "Current Price": lambda x: "—" if pd.isna(x) else f"{x:,.2f}",
                "Shares": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Beta": lambda x: "—" if pd.isna(x) else f"{x:.2f}",
                "Daily Idio Vol": lambda x: "—" if pd.isna(x) else f"{x:.2%}",
                "Net Mkt Value (USD)": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Dollar Beta (USD)": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Dollar Idio Vol (USD)": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
            })
            .set_properties(
                subset=["Current Price","Shares","Net Mkt Value (USD)","Dollar Beta (USD)","Dollar Idio Vol (USD)"],
                **{"text-align":"right"}
            )
        )
        st.dataframe(risk_styler, use_container_width=True, height=TABLE_H)

    with t2:
        st.markdown("""<div class="section-header">Portfolio Summary</div>""", unsafe_allow_html=True)

        summary_styler = (
            summary_df.style
            .format({
                "Value": lambda x: "—" if pd.isna(x) else (
                    f"{x:,.0f}" if abs(x) >= 10 else f"{x:.4f}"
                )
            })
            .set_properties(subset=["Value"], **{"text-align":"right"})
        )
        st.dataframe(summary_styler, use_container_width=True, height=TABLE_H)

    # ---- Row 2: two charts ----
    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.markdown("""<div class="section-header">Net Market Value</div>""", unsafe_allow_html=True)
        st.plotly_chart(
            px.bar(results_df, x="Ticker", y="Net Mkt Value (USD)", title=None),
            use_container_width=True
        )

    with c2:
        st.markdown("""<div class="section-header">Dollar Beta</div>""", unsafe_allow_html=True)
        st.plotly_chart(
            px.bar(results_df, x="Ticker", y="Dollar Beta (USD)", title=None),
            use_container_width=True
        )

    st.markdown(f"""
    <div class="perf-footnote">
      <p>Lookback: <b>{lookback_label}</b> ({lookback_days} days). Benchmark: <b>{market_symbol}</b>.</p>
      <p>Idio vol is residual σ (daily). Dollar risk scales by Net Market Value.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- Compute Daily PnL Attribution ---
    # ----------------------------
    # Daily PnL Attribution (SECOND)
    # ----------------------------
    st.markdown("""<div class="section-header">Daily PnL Attribution</div>""", unsafe_allow_html=True)

    pnl_df = compute_daily_pnl_attribution(results_df, spy_returns, selected_date)

    if pnl_df.empty:
        st.info("No PnL attribution available for that date (missing daily returns).")
    else:
        pnl_styler = (
            pnl_df.style
            .format({
                "Net Market Value": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Instrument Return": lambda x: "—" if pd.isna(x) else f"{x:.2%}",
                "Market Return": lambda x: "—" if pd.isna(x) else f"{x:.2%}",
                "Market PnL": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Idiosyncratic PnL": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
                "Total PnL": lambda x: "—" if pd.isna(x) else f"{x:,.0f}",
            })
            .set_properties(
                subset=["Net Market Value", "Market PnL", "Idiosyncratic PnL", "Total PnL"],
                **{"text-align": "right"}
            )
        )

        st.dataframe(pnl_styler, use_container_width=True)

        st.plotly_chart(
            px.bar(
                pnl_df,
                x="Ticker",
                y=["Market PnL", "Idiosyncratic PnL"],
                barmode="stack",
                title=None
            ),
            use_container_width=True
        )

        st.markdown(f"""
        <div class="perf-footnote">
        <p><b>Attribution model:</b> Market PnL = β · Market Return · Net Market Value; Idiosyncratic PnL = (Instrument Return − β · Market Return) · Net Market Value.</p>
        <p>Date used: <b>{selected_date}</b>. Benchmark: <b>{market_symbol}</b>.</p>
        </div>
        """, unsafe_allow_html=True)

    st.success("Dashboard Updated.")

if __name__ == "__main__":
    main()

    # streamlit run src/investment_analytics/risk/portfolio_risk_app_2.py