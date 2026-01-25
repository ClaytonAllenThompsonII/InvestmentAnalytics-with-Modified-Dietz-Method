#!/usr/bin/env python3
"""
Performance-only Streamlit app.

Displays:
- Instrument-level returns summary (with Portfolio row)
- Benchmark returns summary

Notes:
- Performance figures are evaluated as-of the latest completed month-end in the performance views.
- Benchmarks are evaluated using the benchmark logic defined in investment_returns.py (ideally month-end snapped).
- Daily market data "as of" is shown only as a data-currency reference.
"""

import os
import logging
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from investment_analytics.performance.investment_returns import (
    get_asset_performance_data,
    build_instrument_summary,
    get_portfolio_performance_data,
    build_portfolio_summary,
    get_benchmark_data,
    build_benchmark_summary,
)

# ------------------------------------------------------------------------------
# LOAD ENV + LOGGING
# ------------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ------------------------------------------------------------------------------
# DB CONNECTION (local helper)
# ------------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def get_performance_as_of_date():
    """
    Latest completed month-end in the performance model (portfolio_performance_view).
    Used to label return tables that are derived from monthly performance data.
    """
    query = "SELECT MAX(period_end_date) AS as_of_date FROM portfolio_performance_view;"
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    as_of = df.loc[0, "as_of_date"]
    if pd.isna(as_of):
        return None
    return pd.to_datetime(as_of).date()

def get_market_data_as_of_date_daily():
    """
    Latest available daily market price date.
    Used only as a data-currency reference (not a performance anchor).
    """
    query = "SELECT MAX(price_date) AS as_of_date FROM market_data_daily_adjusted;"
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    as_of = df.loc[0, "as_of_date"]
    if pd.isna(as_of):
        return None
    return pd.to_datetime(as_of).date()

# ------------------------------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Investment Returns | Performance", layout="wide")
st.title("📈 Investment Returns Summary")

performance_as_of = get_performance_as_of_date()
daily_mkt_as_of = get_market_data_as_of_date_daily()

perf_as_of_text = performance_as_of.strftime("%m-%d-%Y") if performance_as_of else "Unavailable"
mkt_as_of_text = daily_mkt_as_of.strftime("%m-%d-%Y") if daily_mkt_as_of else "Unavailable"

st.markdown(f"""
**Reporting anchors**
- **Performance (portfolio & instruments):** month-end, as-of **`{perf_as_of_text}`**
- **Daily market data (reference only):** as-of **`{mkt_as_of_text}`**
""")

# ------------------------------------------------------------------------------
# 1) INSTRUMENT-LEVEL SUMMARY (+ Portfolio row)
# ------------------------------------------------------------------------------
st.header("Instrument-Level Returns")

with st.spinner("Loading instrument performance data..."):
    perf_df = get_asset_performance_data()
    instrument_summary_df = build_instrument_summary(perf_df)

    portfolio_df = get_portfolio_performance_data()
    portfolio_row = build_portfolio_summary(portfolio_df)

    # Append portfolio row
    instrument_summary_df = pd.concat(
        [instrument_summary_df, pd.DataFrame([portfolio_row])],
        ignore_index=True
    )

st.markdown(f"""
**Assumptions & Methodology (Performance)**
- Monthly returns are computed using **Modified Dietz** (money-weighted within each month).
- Trailing horizons are **chain-linked monthly** (time-weighted across months).
- Short histories are labeled: `SI` (since inception) or `Ann.` (annualized where applicable).
- NAV reflects **end-of-month** values from the performance model.
""")

if instrument_summary_df.empty:
    st.warning("No data available to display instrument-level summary.")
else:
    st.dataframe(instrument_summary_df, use_container_width=True)

# ------------------------------------------------------------------------------
# 2) BENCHMARK SUMMARY
# ------------------------------------------------------------------------------
st.header("Benchmark Returns")

st.markdown(f"""
**Assumptions & Methodology (Benchmarks)**
- Benchmarks are computed from **adjusted prices** (total return proxy via dividend reinvestment).
- For clean comparability vs portfolio performance, benchmark returns should be evaluated on a **month-end schedule**.
""")

benchmark_tickers = ["XLY", "EEM", "SPY", "QQQ", "XLK", "IXUS"]
benchmark_rows = []

with st.spinner("Loading benchmark data..."):
    for ticker in benchmark_tickers:
        df_bench = get_benchmark_data(ticker)
        if df_bench is None or df_bench.empty:
            continue

        bench_summary = build_benchmark_summary(df_bench, name=ticker)

        # Format to percent strings for display consistency
        for col in ["MTD", "QTD", "T3M", "YTD", "TTM", "T2Y", "T5Y"]:
            if col in bench_summary.columns:
                bench_summary[col] = bench_summary[col].apply(
                    lambda x: f"{x:.2%}" if pd.notnull(x) else "-"
                )

        benchmark_rows.append(bench_summary)

if benchmark_rows:
    benchmark_summary_df = pd.concat(benchmark_rows, ignore_index=True)
    st.dataframe(benchmark_summary_df, use_container_width=True)
else:
    st.warning("No benchmark data available.")


# streamlit run "src/investment_analytics/performance/investment_returns_performance_app.py"