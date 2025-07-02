import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from investment_returns import (
    get_asset_performance_data,
    build_instrument_summary,
    get_benchmark_data,
    build_benchmark_summary,
    get_current_position_start_from_lots
)

st.set_page_config(page_title="Investment Returns Dashboard", layout="wide")
st.title("📈 Investment Returns Summary")

# ------------------------------------------------------------------------------
# Load & Display Instrument-Level Summary
# ------------------------------------------------------------------------------
st.header("Instrument-Level Returns")

with st.spinner("Loading data..."):
    perf_df = get_asset_performance_data()
    summary_df = build_instrument_summary(perf_df)

# 🠗 Show assumptions ABOVE the tables
st.markdown("""
**Assumptions & Methodology:**
- Returns are calculated using the **Modified Dietz Method** on a net basis (after fees, taxes, etc.).
- Returns are geometrically linked monthly for compounding.
- Trailing periods are annualized where appropriate and marked accordingly (`Ann.` for annualized, `SI` for since inception).
- NAV figures reflect End-of-Month balances and are updated from actual holdings data.
""")

if not summary_df.empty:
    st.dataframe(summary_df, use_container_width=True)
else:
    st.warning("No data available to display instrument-level summary.")

# ------------------------------------------------------------------------------
# Benchmark Return Summary (now correctly placed outside the 'else')
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
# Portfolio NAV Area Chart
# ------------------------------------------------------------------------------
st.header("Portfolio EOM NAV Over Time")

def plot_portfolio_nav_area(df_port):
    if df_port.empty:
        st.warning("No portfolio data to plot for NAV EOM.")
        return

    fig_nav = go.Figure()
    fig_nav.add_trace(go.Scatter(
        x=df_port["period_end_date"],
        y=df_port["portfolio_eom"],
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

# Create df_port based on active positions
if not perf_df.empty:
    instrument_starts = {
        instrument: get_current_position_start_from_lots(instrument)
        for instrument in perf_df["instrument"].unique()
    }

    # Filter NAV data to include only data since active position start
    df_active_nav = perf_df.copy()
    df_active_nav["period_end_date"] = pd.to_datetime(df_active_nav["period_end_date"])

    mask = df_active_nav.apply(
        lambda row: instrument_starts.get(row["instrument"]) is not None and 
                    row["period_end_date"] >= instrument_starts[row["instrument"]],
        axis=1
    )
    df_active_nav = df_active_nav[mask]

    # Aggregate to portfolio-level NAV
    df_port = df_active_nav.groupby("period_end_date")["nav_eom"].sum().reset_index()
    df_port.rename(columns={"nav_eom": "portfolio_eom"}, inplace=True)
    plot_portfolio_nav_area(df_port)
else:
    st.info("NAV data not available to plot portfolio chart.")

# Run with:
# streamlit run "analytics/performance/investment_returns_app.py"