import os
import logging
import psycopg2
import pandas as pd
import statsmodels.api as sm
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv
from investment_analytics.performance.investment_returns import (
    get_connection,
    get_asset_performance_data,
    build_instrument_summary,
    get_portfolio_performance_data,
    build_portfolio_summary,
    get_benchmark_data,
    build_benchmark_summary,
    get_current_position_start_from_lots
)

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# DB connection helper
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def get_market_data_as_of_date():
    query = "SELECT MAX(price_date) AS as_of_date FROM market_data_daily_adjusted;"
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    as_of = df.loc[0, "as_of_date"]

    if pd.isna(as_of):
        return None

    return pd.to_datetime(as_of).date()

# Streamlit page config
st.set_page_config(page_title="Investment Returns Dashboard", layout="wide")
st.title(" \U0001F4C8  Investment Returns Summary") # 

# ------------------------------------------------------------------------------
# 1. Instrument-Level Summary
# ------------------------------------------------------------------------------
st.header("Instrument-Level Returns")

with st.spinner("Loading instrument performance data..."):
    perf_df = get_asset_performance_data()
    summary_df = build_instrument_summary(perf_df)
    market_data_as_of = get_market_data_as_of_date()
    # Add total/aggregate row
    
    #numeric_cols = summary_df.select_dtypes(include=np.number).columns
    # Add portfolio row using new summary logic
    portfolio_df = get_portfolio_performance_data()
    portfolio_row = build_portfolio_summary(portfolio_df)
    summary_df = pd.concat([summary_df, pd.DataFrame([portfolio_row])], ignore_index=True)

if not summary_df.empty:

    as_of_text = (
        market_data_as_of.strftime("%m-%d-%Y")
        if market_data_as_of
        else "Unavailable"
    )

    st.markdown(f"""
    **Assumptions & Methodology:**
    - Returns are calculated using the **Modified Dietz Method** on a net basis (after fees, taxes, etc.).
    - Returns are linked monthly for compounding.
    - Trailing periods are annualized where appropriate and marked accordingly (`Ann.` for annualized, `SI` for since inception).
    - NAV figures reflect End-of-Month balances and are updated from actual holdings data.
    - Market data is current as of **` {as_of_text} `**.
    """)
    
    st.dataframe(summary_df, use_container_width=True)
else:
    st.warning("No data available to display instrument-level summary.")


# ------------------------------------------------------------------------------
# 1b. Benchmark Return Summary
# ------------------------------------------------------------------------------
st.subheader("Benchmark Return Summary")

benchmark_tickers = ["XLY", "EEM", "SPY", "QQQ", "XLK", "IXUS"]
benchmark_dfs = []

for ticker in benchmark_tickers:
    df_bench = get_benchmark_data(ticker)
    if df_bench.empty:
        continue

    summary = build_benchmark_summary(df_bench, name=ticker)

    for col in ["MTD", "QTD", "T3M", "YTD", "TTM", "T2Y", "T5Y"]:
        summary[col] = summary[col].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else "-")

    benchmark_dfs.append(summary)

if benchmark_dfs:
    benchmark_summary_df = pd.concat(benchmark_dfs, ignore_index=True)
    st.dataframe(benchmark_summary_df, use_container_width=True)
else:
    st.warning("No benchmark data available.")

    
# ------------------------------------------------------------------------------
# 2. Portfolio EOM NAV Area Chart
# ------------------------------------------------------------------------------
st.header("Portfolio EOM NAV Over Time")

def get_filtered_nav_data():
    nav_df = get_asset_performance_data()
    filtered_rows = []
    for instrument in nav_df['instrument'].unique():
        start_date = get_current_position_start_from_lots(instrument)
        if start_date:
            filtered_rows.append(nav_df[(nav_df['instrument'] == instrument) & (nav_df['period_end_date'] >= start_date)])
    return pd.concat(filtered_rows) if filtered_rows else pd.DataFrame()

def plot_portfolio_nav_area(df_port):
    if df_port.empty:
        st.warning("No portfolio data to plot for NAV EOM.")
        return

    df = df_port.groupby("period_end_date")["nav_eom"].sum().reset_index()
    df.rename(columns={"nav_eom": "portfolio_eom"}, inplace=True)

    fig_nav = go.Figure()
    fig_nav.add_trace(go.Scatter(
        x=df["period_end_date"],
        y=df["portfolio_eom"],
        fill='tozeroy',
        mode='lines',
        name='Portfolio EOM NAV',
        line_color='blue'
    ))
    fig_nav.update_layout(
        title="Portfolio End-of-Month NAV",
        xaxis_title="Date",
        yaxis_title="NAV ($)",
        template="plotly_white"
    )
    st.plotly_chart(fig_nav, use_container_width=True)

filtered_nav_df = get_filtered_nav_data()
plot_portfolio_nav_area(filtered_nav_df)

# ------------------------------------------------------------------------------
# 3. Risk Table + Charts
# ------------------------------------------------------------------------------
st.header("Portfolio Risk Summary")

def get_open_positions():
    query = """
        SELECT instrument, SUM(open_quantity) AS total_open_quantity, SUM(total_cost) AS total_cost
        FROM fifo_equity_lots
        WHERE open_quantity != 0
        GROUP BY instrument
        ORDER BY instrument;
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df

def fetch_daily_returns_db(symbol, start_date, end_date):
    query = """
        SELECT price_date, adjusted_close FROM market_data_daily_adjusted
        WHERE instrument = %s AND price_date BETWEEN %s AND %s
        ORDER BY price_date;
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))
    df.set_index('price_date', inplace=True)
    return df['adjusted_close'].pct_change().dropna()

def fetch_current_price_db(symbol):
    query = """
        SELECT adjusted_close FROM market_data_daily_adjusted
        WHERE instrument = %s ORDER BY price_date DESC LIMIT 1;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            result = cur.fetchone()
    return float(result[0]) if result else np.nan

def compute_beta_idio_vol(instrument_returns, market_returns):
    data = pd.concat([instrument_returns, market_returns], axis=1).dropna()
    data.columns = ['r_instrument', 'r_market']
    if len(data) < 2:
        return np.nan, np.nan
    X = sm.add_constant(data['r_market'])
    y = data['r_instrument']
    model = sm.OLS(y, X).fit()
    return model.params['r_market'], model.resid.std()

positions_df = get_open_positions()
end_date = datetime.today().date()
start_date = end_date - timedelta(days=365 * 2)
spy_returns = fetch_daily_returns_db("SPY", start_date, end_date)
market_vol = spy_returns.std()

results = []
for _, row in positions_df.iterrows():
    instr = row['instrument']
    shares = row['total_open_quantity']
    inst_returns = fetch_daily_returns_db(instr, start_date, end_date)
    beta, daily_idio_vol = compute_beta_idio_vol(inst_returns, spy_returns)
    curr_price = fetch_current_price_db(instr)
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
st.subheader("Instrument-Level Risk")
st.dataframe(results_df)

# Summary Metrics
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

st.subheader("Portfolio-Level Risk Summary")
st.dataframe(summary_df)

# Charts
st.plotly_chart(px.bar(results_df, x="Ticker", y="Net Mkt Value (USD)", color="Ticker"), use_container_width=True)
st.plotly_chart(px.bar(results_df, x="Ticker", y="Dollar Beta (USD)", color="Ticker"), use_container_width=True)

st.success("Dashboard Updated.")

# streamlit run "src/investment_analytics/performance/investment_returns_app2.py"