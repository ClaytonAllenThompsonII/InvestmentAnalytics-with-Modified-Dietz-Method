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
    query = """
        SELECT MAX(period_end_date) AS as_of_date
        FROM portfolio_performance_view
        WHERE period_end_date < date_trunc('month', CURRENT_DATE)
    """
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
st.markdown("""
<style>
/* Global font override for Streamlit */
html, body, [class*="css"] {
    font-family: "Segoe UI", -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif;
    letter-spacing: 0.01em;
}
</style>
""", unsafe_allow_html=True)

performance_as_of = get_performance_as_of_date()
daily_mkt_as_of = get_market_data_as_of_date_daily()

perf_as_of_text = performance_as_of.strftime("%m-%d-%Y") if performance_as_of else "Unavailable"
mkt_as_of_text = daily_mkt_as_of.strftime("%m-%d-%Y") if daily_mkt_as_of else "Unavailable"

st.markdown(f"""
<div style="
  background-color: #f3f4f6;
  padding: 0.75rem 1rem 0.55rem 1rem;
  border-radius: 4px;
">
  <h1 style="margin: 0;">Performance Detail</h1>
  <p style="margin: 0.25rem 0 0; color: #6b7280; font-size: 1.05rem;">
    AS OF {performance_as_of.strftime("%B %d, %Y") if performance_as_of else "Unavailable"}
  </p>
</div>

<hr style="
  border: none;
  height: 4px;
  background-color: #0021A5;
  margin: 0 0 1.1rem;
">
""", unsafe_allow_html=True)

st.markdown("""
<style>
.section-header {
  background-color: #f3f4f6;
  color: #374151;
  text-align: center;
  font-weight: 600;
  padding: 0.15rem 0;      /* minimal vertical padding */
  margin-bottom: 0;       /* no gap before table */
  border-radius: 4px;
  border: 1px solid #e5e7eb;
}

.perf-footnote {
  font-size: 0.52rem;   /* footnote, not invisible */
  color: #9ca3af;
  line-height: 1.15;
  margin-top: 0.05rem;
  margin-bottom: 0.9rem;   /* space before next section */
}
.perf-footnote p {
  margin: 0.05rem 0;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 1) INSTRUMENT-LEVEL SUMMARY (+ Portfolio row)
# ------------------------------------------------------------------------------

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


if instrument_summary_df.empty:
    st.warning("No data available to display instrument-level summary.")
else:
    st.markdown("""
    <div class="section-header">
    Reported Performance
    </div>
    """, unsafe_allow_html=True)

    st.dataframe(instrument_summary_df, use_container_width=True)

    st.markdown("""
    <div class="perf-footnote">
    <p>Returns methodology: money-weighted (Modified Dietz) within each month; time-weighted via geometric linking across periods.</p>
    <p>Labels: <code>SI</code> = since inception; <code>Ann.</code> = annualized.</p>
    <p>Net Asset Value (NAV) reflects end-of-month balances from the performance model.</p>
    </div>
    """, unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# 2) BENCHMARK SUMMARY
# ------------------------------------------------------------------------------
st.markdown("""
<div class="section-header">
  Benchmark Performance
</div>
""", unsafe_allow_html=True)

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

    # Benchmark definitions (ETF names)
    benchmark_definitions = {
        "SPY": "SPDR S&P 500 ETF Trust",
        "QQQ": "Invesco QQQ Trust (Nasdaq-100)",
        "XLY": "Consumer Discretionary Select Sector SPDR Fund",
        "XLK": "Technology Select Sector SPDR Fund",
        "EEM": "iShares MSCI Emerging Markets ETF",
        "IXUS": "iShares Core MSCI Total International Stock ETF",
    }

    # Build a concise definitions string in your ticker order
    defs_text = "; ".join(
        [f"{t} = {benchmark_definitions.get(t, 'Unknown')}" for t in benchmark_tickers]
    )

    st.markdown(f"""
    <div class="perf-footnote">
      <p>Benchmark inputs are adjusted prices (total-return proxy).</p>
      <p>For comparability, benchmark returns should be evaluated on a month-end schedule (aligned to {perf_as_of_text}).</p>
      <p><b>Benchmarks:</b> {defs_text}.</p>
    </div>
    """, unsafe_allow_html=True)

else:
    st.warning("No benchmark data available.")


# streamlit run "src/investment_analytics/performance/investment_returns_performance_app.py"