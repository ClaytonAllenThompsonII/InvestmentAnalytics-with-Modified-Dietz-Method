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
import numpy as np
import altair as alt

from investment_analytics.performance.investment_returns import (
    get_asset_performance_data,
    build_instrument_summary,
    get_portfolio_performance_data,
    build_portfolio_summary,
    get_benchmark_data,
    build_benchmark_summary,
    build_instrument_return_series_for_chart,   # NEW
    benchmark_monthly_returns,  # NEW (for benchmark overlay time-series)
)

def finalize_altair_chart(chart: alt.Chart, height: int = 420) -> alt.Chart:
    """
    Make Altair charts render reliably in Streamlit:
    - Enough bottom padding so x-axis labels aren't clipped
    - Consistent sizing
    - Reasonable x-axis label behavior
    """
    return (
        chart
        .properties(
            height=height,
            padding=alt.Padding(left=10, right=10, top=10, bottom=40)  # <-- key fix
        )
        .configure_view(stroke=None)
        .configure_axis(
            labelFontSize=11,
            titleFontSize=12,
            labelLimit=120,      # allow longer labels without truncating too aggressively
            labelOverlap=False,  # don't auto-hide labels
        )
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
perf_as_of_pretty = performance_as_of.strftime("%B %d, %Y") if performance_as_of else "Unavailable"
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

    st.markdown(f"""
    <div class="perf-footnote">
    <p>Returns methodology: money-weighted (Modified Dietz) within each month; time-weighted via geometric linking across periods.</p>
    <p>SI = since inception; Ann. = annualized.</p>
    <p>Net Asset Value (NAV) reflects end-of-month balances from the performance model.</p>
    <p>As-of date: {perf_as_of_pretty}. All reported returns are evaluated using the latest completed month-end.</p>
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


# ------------------------------------------------------------------------------
# 3) INSTRUMENT RETURN LINES — settings row + 3 clean blocks
# ------------------------------------------------------------------------------
st.markdown("""
<div class="section-header">
  Instrument Return Lines
</div>
""", unsafe_allow_html=True)

instrument_list = sorted(perf_df["instrument"].dropna().unique().tolist())
if not instrument_list:
    st.warning("No instruments available for charting.")
else:
    # ---- Settings row (aligned with chart width) ----
    c1, c2, c3 = st.columns([3, 2, 1])

    with c1:
        selected_instrument = st.selectbox(
            "Instrument",
            options=instrument_list,
            index=0,
        )

    # Benchmark overlay selector (None + your benchmarks)
    overlay_options = ["None", "SPY", "QQQ", "XLK", "XLY", "EEM", "IXUS"]
    with c2:
        overlay_benchmark = st.selectbox(
            "Benchmark overlay",
            options=overlay_options,
            index=0,
        )

    with c3:
        use_log_scale = st.checkbox("Log scale", value=False)

    # ---- Build instrument chart series once ----
    windows = (12, 24, 36, 60)

    with st.spinner("Building return line series..."):
        inst_long = build_instrument_return_series_for_chart(
            perf_df=perf_df,
            instrument=selected_instrument,
            return_col="md_return_net",
            windows=windows,
            include_si=True,   # we always compute SI for the SI chart
        )

    if inst_long.empty:
        st.warning("No chart data available for that instrument.")
    else:
        # Helper: build benchmark series in same shape as instrument series
        def build_benchmark_lines(ticker: str, windows=(12, 24, 36, 60)) -> pd.DataFrame:
            df_daily = get_benchmark_data(ticker)
            if df_daily is None or df_daily.empty:
                return pd.DataFrame(columns=["period_end_date", "horizon", "return"])

            monthly = benchmark_monthly_returns(df_daily, price_col="adj_price")
            if monthly.empty:
                return pd.DataFrame(columns=["period_end_date", "horizon", "return"])

            # monthly_return is decimal; build SI + rolling windows in log space
            r = monthly["monthly_return"].fillna(0).astype(float)
            log_1p = np.log1p(r)

            monthly = monthly.copy()
            monthly["SI"] = np.expm1(log_1p.cumsum())

            for k in windows:
                monthly[f"{k}M"] = np.expm1(log_1p.rolling(k).sum())

            parts = []
            # SI
            parts.append(monthly[["period_end_date", "SI"]].rename(columns={"SI": "return"}).assign(horizon="SI"))
            # windows
            for k in windows:
                parts.append(
                    monthly[["period_end_date", f"{k}M"]]
                    .rename(columns={f"{k}M": "return"})
                    .assign(horizon=f"{k}M")
                )

            out = pd.concat(parts, ignore_index=True).dropna(subset=["return"])
            return out.sort_values(["horizon", "period_end_date"]).reset_index(drop=True)

        bench_long = None
        if overlay_benchmark != "None":
            bench_long = build_benchmark_lines(overlay_benchmark, windows=windows)
            if bench_long.empty:
                bench_long = None

        # ------------------------------------------------------------
        # Block 1) Long-Term Compounding (SI cumulative return)
        # ------------------------------------------------------------
        st.markdown("""
        <div class="section-header">
          Long-Term Compounding
        </div>
        """, unsafe_allow_html=True)

        si_inst = inst_long[inst_long["horizon"] == "SI"].copy()
        si_inst["series"] = selected_instrument

        si_plot_df = si_inst[["period_end_date", "return", "series"]].copy()

        if bench_long is not None:
            si_bench = bench_long[bench_long["horizon"] == "SI"].copy()
            si_bench["series"] = overlay_benchmark
            si_plot_df = pd.concat(
                [si_plot_df, si_bench[["period_end_date", "return", "series"]]],
                ignore_index=True
            )

        # Altair line chart with percent axis; optional log scale for y
        y_scale = alt.Scale(type="log") if use_log_scale else alt.Scale()

        si_chart = (
            alt.Chart(si_plot_df)
            .mark_line()
            .encode(
                x=alt.X(
                    "yearmonth(period_end_date):T",
                    title=None,
                    axis=alt.Axis(
                        format="%Y",        # 2021, 2022, 2023...
                        tickCount="year",   # yearly tick marks
                        labelAngle=0,
                        labelOverlap=True
                    ),
                ),
                y=alt.Y(
                    "return:Q",
                    title=None,
                    axis=alt.Axis(format="%"),
                    scale=y_scale
                ),
                color=alt.Color("series:N", title=None),
                tooltip=[
                    alt.Tooltip("period_end_date:T", title="Date"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("return:Q", title="Return", format=".2%")
                ],
            )
        )

        st.altair_chart(finalize_altair_chart(si_chart, height=460), use_container_width=True)

        st.markdown(f"""
        <div class="perf-footnote">
          <p><b>SI cumulative return</b> for {selected_instrument}{f" vs {overlay_benchmark}" if overlay_benchmark != "None" else ""}.</p>
          <p>Values are decimal returns shown as % on the axis.</p>
        </div>
        """, unsafe_allow_html=True)

        # ------------------------------------------------------------
        # Block 2) Rolling Holding-Period Returns (single horizon)
        # ------------------------------------------------------------
        st.markdown("""
        <div class="section-header">
          Rolling Holding-Period Returns
        </div>
        """, unsafe_allow_html=True)

        # "pill" selector (Streamlit native)
        horizon_choice = st.radio(
            "Holding period",
            options=["12M", "24M", "36M", "60M"],
            horizontal=True,
            label_visibility="collapsed",
        )

        roll_inst = inst_long[inst_long["horizon"] == horizon_choice].copy()
        roll_inst["series"] = selected_instrument

        roll_plot_df = roll_inst[["period_end_date", "return", "series"]].copy()

        if bench_long is not None:
            roll_bench = bench_long[bench_long["horizon"] == horizon_choice].copy()
            roll_bench["series"] = overlay_benchmark
            roll_plot_df = pd.concat(
                [roll_plot_df, roll_bench[["period_end_date", "return", "series"]]],
                ignore_index=True
            )

        roll_chart = (
            alt.Chart(roll_plot_df)
            .mark_line()
            .encode(
                x=alt.X(
                    "yearmonth(period_end_date):T",
                    title=None,
                    axis=alt.Axis(
                        format="%Y",        # 2021, 2022, 2023...
                        tickCount="year",   # yearly tick marks
                        labelAngle=0,
                        labelOverlap=True
                    ),
                ),
                y=alt.Y(
                    "return:Q",
                    title=None,
                    axis=alt.Axis(format="%"),
                    scale=y_scale
                ),
                color=alt.Color("series:N", title=None),
                tooltip=[
                    alt.Tooltip("period_end_date:T", title="Date"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("return:Q", title="Rolling return", format=".2%")
                ],
            )
        )

        st.altair_chart(finalize_altair_chart(roll_chart, height=440), use_container_width=True)

        st.markdown(f"""
        <div class="perf-footnote">
          <p><b>{horizon_choice}</b> rolling trailing returns (each point is the return over the prior {horizon_choice} window ending on that month).</p>
          <p>Early months are blank until enough history exists.</p>
        </div>
        """, unsafe_allow_html=True)

        # ------------------------------------------------------------
        # Block 3) Summary Stats (for the selected rolling horizon)
        # ------------------------------------------------------------
        st.markdown("""
        <div class="section-header">
          Summary Stats
        </div>
        """, unsafe_allow_html=True)

        def summarize_series(df_sub: pd.DataFrame, label: str) -> dict:
            s = df_sub["return"].dropna()
            if s.empty:
                return {
                    "Series": label,
                    "Min": None,
                    "Median": None,
                    "Max": None,
                    "% Positive": None,
                    "Observations": 0
                }
            return {
                "Series": label,
                "Min": s.min(),
                "Median": s.median(),
                "Max": s.max(),
                "% Positive": float((s > 0).mean()),
                "Observations": int(s.shape[0]),
            }

        stats_rows = [summarize_series(roll_inst, selected_instrument)]
        if bench_long is not None:
            stats_rows.append(summarize_series(bench_long[bench_long["horizon"] == horizon_choice], overlay_benchmark))

        stats_df = pd.DataFrame(stats_rows)

        # display-friendly formatting
        for col in ["Min", "Median", "Max"]:
            stats_df[col] = stats_df[col].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else "-")
        stats_df["% Positive"] = stats_df["% Positive"].apply(lambda x: f"{x:.1%}" if pd.notnull(x) else "-")

        st.dataframe(stats_df, use_container_width=True)

        st.markdown(f"""
        <div class="perf-footnote">
          <p>Stats computed over the available {horizon_choice} rolling series (non-null observations only).</p>
        </div>
        """, unsafe_allow_html=True)


# streamlit run "src/investment_analytics/performance/investment_returns_performance_app.py"