#!/usr/bin/env python3
"""
investment_returns.py

Core performance utilities for:
- Reading monthly performance model outputs (Postgres views)
- Computing linked returns (Modified Dietz monthly -> time-weighted across months)
- Building instrument/portfolio/benchmark summaries for Streamlit + CLI
- Building instrument time-series (line series) for charts (SI + rolling windows)

Terminology:
- "line series" / "time series" = values over time (what we plot)
- "curve" reserved for fitted functions (not used here)
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta


# ==============================================================================
# 0) LOAD ENV + LOGGING
# ==============================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ------------------------------------------------------------------------------
# 1) DATABASE CONNECTION
# ------------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# ==============================================================================
# 2) DATA ACCESS (READS) — Postgres -> pandas
# ==============================================================================
# Purpose: pull raw / modeled datasets from Postgres for downstream analytics.

def get_asset_performance_data():
    query = """
        SELECT *
        FROM asset_performance_view
        ORDER BY instrument, period_end_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
        df["period_end_date"] = pd.to_datetime(df["period_end_date"])
        return df

def get_portfolio_performance_data():
    query = """
        SELECT *
        FROM portfolio_performance_view
        ORDER BY period_end_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
        df["period_end_date"] = pd.to_datetime(df["period_end_date"])
        return df

def get_benchmark_data(benchmark="SPY"):
    query = """
        SELECT
            price_date,
            COALESCE(adjusted_close, close_price) AS adj_price
        FROM market_data_daily_adjusted
        WHERE instrument = %s
          AND price_date >= CURRENT_DATE - INTERVAL '60 months'
        ORDER BY price_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(benchmark,))
    df["price_date"] = pd.to_datetime(df["price_date"])
    return df

def get_current_position_start_from_lots(instrument: str) -> pd.Timestamp:
    query = """
        SELECT MIN(lot_open_date) AS start_date
        FROM fifo_equity_lots
        WHERE instrument = %s AND open_quantity > 0
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(instrument,))
        if df.empty or pd.isna(df.iloc[0]["start_date"]):
            return None
        return pd.to_datetime(df.iloc[0]["start_date"])


# ==============================================================================
# 3) RETURN MATH — core compounding utilities
# ==============================================================================
# Purpose: compute returns from a provided dataframe/series.
# Note: keep these free of DB calls where possible (easier to test).

# ------------------------------------------------------------------------------
# Portfolio & Position Returns (Monthly, Modified Dietz)
# ------------------------------------------------------------------------------
# Scope:
#   - Active portfolio and instrument-level performance
#   - Monthly Modified Dietz returns (money-weighted within period)
#   - Geometrically-linked across months (time-weighted across periods)
#
# Notes:
#   - Used for portfolio, position, and attribution reporting
#   - All trailing returns are evaluated as-of month-end
#   - Position start dates may clip trailing windows

def calculate_twr_from_monthly(df, return_col="md_return_net"):
    """
    Chain-link monthly Modified Dietz returns to produce a time-weighted return.

    This reflects the standard institutional approach: returns are
    time-weighted across months via geometric linking, while each
    monthly sub-period return is money-weighted (Modified Dietz)
    within the period.
    """
    if df.empty:
        return None
    return (1 + df[return_col].fillna(0)).prod() - 1

def calculate_trailing_return(df, months, return_col="md_return_net", instrument=None):
    """
    Compute a trailing chain-linked return over the last N months.

    Uses monthly Modified Dietz returns (money-weighted within each month)
    and chain-links them across months (time-weighted across periods).
    If `instrument` is provided, the window is also clipped to the current
    open-position start date; short histories are labeled as SI or Ann.
    """
    if df.empty:
        return None, None

    max_date = df["period_end_date"].max()
    cutoff = max_date - relativedelta(months=months)

    # Get position start date
    start_date = None
    if instrument:
        start_date = get_current_position_start_from_lots(instrument)

    # Subset for trailing period
    subset = df[df["period_end_date"] > cutoff]

    if start_date:
        subset = subset[subset["period_end_date"] >= start_date]

    if subset.empty:
        return None, "SI"

    raw_return = calculate_twr_from_monthly(subset, return_col)
    actual_months = subset["period_end_date"].nunique()

    # Determine label based on actual months
    if actual_months < months:
        if actual_months >= 12:
            ann_return = (1 + raw_return) ** (12 / actual_months) - 1
            return ann_return, "Ann."
        else:
            return raw_return, "SI"

    return raw_return, None

def calculate_trailing_return_for_portfolio(df, months, return_col="md_return_net"):
    """
    Compute a trailing portfolio return over the last N months via chain-linking.

    Assumes the portfolio return series is already aggregated at the portfolio
    level per month (e.g., portfolio_performance_view md_return_net). Returns are
    geometrically linked across months (time-weighted across periods), while each
    monthly sub-period return is a Modified Dietz return (money-weighted within
    the month). No position start-date filtering is applied.
    """
    if df.empty:
        return None, None

    df = df.sort_values("period_end_date").copy()
    max_date = df["period_end_date"].max()
    cutoff = max_date - relativedelta(months=months)

    subset = df[df["period_end_date"] > cutoff]
    if subset.empty:
        return None, None

    raw_return = (1 + subset[return_col].fillna(0)).prod() - 1
    actual_months = subset["period_end_date"].nunique()

    if actual_months < months:
        if actual_months >= 12:
            ann_return = (1 + raw_return) ** (12 / actual_months) - 1
            return ann_return, "Ann."
        else:
            return raw_return, "SI"

    return raw_return, None


# ==============================================================================
# 4) MONTHLY LINE-SERIES BUILDERS — canonical inputs to charting
# ==============================================================================
# Purpose: build instrument time series (line series) for charts:
# - SI cumulative return line series
# - rolling trailing return line series (12M/24M/36M/60M)

def get_instrument_monthly_returns(
    perf_df: pd.DataFrame,
    instrument: str,
    return_col: str = "md_return_net",
    ) -> pd.DataFrame:
    """
    Instrument monthly return time series (one row per month-end).

    Output columns:
      - period_end_date (Timestamp)
      - monthly_return (float)
    """
    df = perf_df.loc[perf_df["instrument"] == instrument, ["period_end_date", return_col]].copy()
    if df.empty:
        return pd.DataFrame(columns=["period_end_date", "monthly_return"])

    df = df.sort_values("period_end_date")
    df = df.rename(columns={return_col: "monthly_return"})
    df["period_end_date"] = pd.to_datetime(df["period_end_date"])
    df["monthly_return"] = pd.to_numeric(df["monthly_return"], errors="coerce")
    return df.reset_index(drop=True)

def add_cumulative_return_si_log(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a Since Inception (SI) cumulative return line series over time.

    Uses log-compounding for numerical stability:
      SI_t = exp( sum_{0..t} log(1 + r_t) ) - 1
    """
    df = monthly_df.copy()
    if df.empty:
        df["si_return"] = pd.Series(dtype="float64")
        return df

    log_1p = np.log1p(df["monthly_return"].fillna(0).astype(float))
    df["si_return"] = np.expm1(log_1p.cumsum())  # exp(cumsum) - 1
    return df

def add_rolling_trailing_returns_log(
    monthly_df: pd.DataFrame,
    windows=(12, 24, 36, 60),
) -> pd.DataFrame:
    """
    Add trailing rolling return line series over time for each window k months.

    Trailing return at time t:
      T{k}M_t = exp( sum_{t-k+1..t} log(1+r_t) ) - 1

    Notes:
    - First k-1 rows are NaN (insufficient history). Expected behavior.
    """
    df = monthly_df.copy()
    if df.empty:
        for k in windows:
            df[f"t{k}m_return"] = pd.Series(dtype="float64")
        return df

    log_1p = np.log1p(df["monthly_return"].fillna(0).astype(float))
    for k in windows:
        df[f"t{k}m_return"] = np.expm1(log_1p.rolling(k).sum())

    return df


# ==============================================================================
# 5) BENCHMARK RETURNS — month-end snapped passive series
# ==============================================================================
# Scope:
#   - Passive benchmark indices (e.g., SPY, QQQ, XLK)
#   - Total return approximated via adjusted prices
#   - Daily prices are snapped to month-end before return calculation
#
# Notes:
#   - Benchmarks are evaluated on the same month-end schedule as portfolio returns
#   - No cash-flow timing or Modified Dietz logic applies
#   - Intended for relative performance comparison, not attribution

def benchmark_monthly_returns(df_daily: pd.DataFrame, price_col: str = "adj_price") -> pd.DataFrame:
    """
    Convert daily benchmark prices into a month-end series of monthly total returns.

    - Snaps to the last available trading day in each month (month-end proxy).
    - Uses adjusted prices so monthly returns approximate total return (dividend reinvestment).
    """
    if df_daily is None or df_daily.empty:
        return pd.DataFrame(columns=["period_end_date", "monthly_return"])

    df = df_daily.sort_values("price_date").copy()

    # Month bucket
    df["month"] = df["price_date"].dt.to_period("M").dt.to_timestamp()

    # Last trading day price per month
    eom = (
        df.groupby("month", as_index=False)
          .agg(period_end_date=("price_date", "max"),
               eom_price=(price_col, "last"))
          .sort_values("period_end_date")
    )

    # Monthly total return (needs previous month-end)
    eom["monthly_return"] = eom["eom_price"].pct_change()

    # Drop the first month (no prior month to compute return)
    eom = eom.dropna(subset=["monthly_return"])

    return eom[["period_end_date", "monthly_return"]]

def calculate_trailing_return_benchmark_eom(df_daily, months, price_col="adj_price"):
    """
    Trailing benchmark return over N months, anchored to latest completed month-end.

    Uses month-end snapped monthly total returns (from adjusted prices) and chain-links them.
    """
    monthly = benchmark_monthly_returns(df_daily, price_col=price_col)
    if monthly.empty:
        return None

    # Reuse your chain-link helper (expects a return column)
    cutoff = monthly["period_end_date"].max() - relativedelta(months=months)
    subset = monthly[monthly["period_end_date"] > cutoff]
    return calculate_twr_from_monthly(subset, return_col="monthly_return")

# ==============================================================================
# ## --- PRESENTATION BUILDERS (TABLES & PLOTS)
# ==============================================================================
# Purpose:
#   - Transform canonical return series into presentation-ready structures
#   - Produce *tidy* DataFrames for tables and charts
#   - No database access; no business logic; no return math
#
# Scope:
#   - Summary tables (instrument, portfolio, benchmark)
#   - Plot-ready long-form time series (e.g., rolling windows, SI curves)
#
# Notes:
#   - These functions should only compose lower-level helpers
#   - Output shapes are designed for Streamlit / plotting libraries
#   - Any change here should NOT affect return correctness

def build_instrument_summary(df): 
    summary = []

    def fmt(val, flag):
        if val is None:
            return " " * 6 + "-"
        pct_str = f"{val:>7.2%}"
        if flag == "SI":
            return f"{'SI':<6}{pct_str}"
        elif flag == "Ann.":
            return f"{'Ann.':<6}{pct_str}"
        else:
            return " " * 6 + pct_str

    for instrument, group in df.groupby("instrument"):
        group = group.sort_values("period_end_date").copy()
        if group.empty:
            continue

        start_date = get_current_position_start_from_lots(instrument)
        if start_date:
            group = group[group["period_end_date"] >= start_date]
        if group.empty:
            continue

        latest = group["period_end_date"].max()
        mtd_start = latest.replace(day=1)
        qtd_start = latest.replace(day=1) - relativedelta(months=(latest.month - 1) % 3)
        ytd_start = latest.replace(month=1, day=1)

        t3m_val, t3m_flag = calculate_trailing_return(group, 3, instrument=instrument)
        ttm_val, ttm_flag = calculate_trailing_return(group, 12, instrument=instrument)
        t2y_val, t2y_flag = calculate_trailing_return(group, 24, instrument=instrument)

        summary.append({
            "instrument": instrument,
            "MTD": fmt(calculate_twr_from_monthly(group[group["period_end_date"] >= mtd_start]), None),
            "QTD": fmt(calculate_twr_from_monthly(group[group["period_end_date"] >= qtd_start]), None),
            "T3M": fmt(t3m_val, t3m_flag),
            "YTD": fmt(calculate_twr_from_monthly(group[group["period_end_date"] >= ytd_start]), None),
            "TTM": fmt(ttm_val, ttm_flag),
            "T2Y": fmt(t2y_val, t2y_flag),
            "LTD": fmt(calculate_twr_from_monthly(group), None),
            "Latest NAV": group["nav_eom"].iloc[-1],
            "Total Shares": group["eom_shares_cumulative"].iloc[-1] if "eom_shares_cumulative" in group.columns else None
        })

    return pd.DataFrame(summary)


def build_portfolio_summary(portfolio_df):
    def fmt(val, flag):
        if val is None:
            return " " * 6 + "-"
        pct_str = f"{val:>7.2%}"
        if flag == "SI":
            return f"{'SI':<6}{pct_str}"
        elif flag == "Ann.":
            return f"{'Ann.':<6}{pct_str}"
        else:
            return " " * 6 + pct_str

    portfolio_df = portfolio_df.sort_values("period_end_date")

    latest = portfolio_df["period_end_date"].max()
    mtd_start = latest.replace(day=1)
    qtd_start = latest.replace(day=1) - relativedelta(months=(latest.month - 1) % 3)
    ytd_start = latest.replace(month=1, day=1)

    def calc_linked_return(subset):
        if subset.empty:
            return None
        return (1 + subset["md_return_net"].fillna(0)).prod() - 1

    t3m_val, t3m_flag = calculate_trailing_return_for_portfolio(portfolio_df, 3)
    ttm_val, ttm_flag = calculate_trailing_return_for_portfolio(portfolio_df, 12)
    t2y_val, t2y_flag = calculate_trailing_return_for_portfolio(portfolio_df, 24)

    return {
        "instrument": "Portfolio",
        "MTD": fmt(calc_linked_return(portfolio_df[portfolio_df["period_end_date"] >= mtd_start]), None),
        "QTD": fmt(calc_linked_return(portfolio_df[portfolio_df["period_end_date"] >= qtd_start]), None),
        "T3M": fmt(t3m_val, t3m_flag),
        "YTD": fmt(calc_linked_return(portfolio_df[portfolio_df["period_end_date"] >= ytd_start]), None),
        "TTM": fmt(ttm_val, ttm_flag),
        "T2Y": fmt(t2y_val, t2y_flag),
        "LTD": fmt(calc_linked_return(portfolio_df), None),
        "Latest NAV": portfolio_df["nav_eom"].iloc[-1],
        "Total Shares": None
    }

# ------------------------------------------------------------------------------
# Benchmark Return Summary (Month-End, Total Return Proxy)
# ------------------------------------------------------------------------------
# Purpose:
#   - Produce benchmark returns on the same month-end grid as portfolio performance
#   - Support like-for-like relative performance comparison in reporting tables

def build_benchmark_summary(df_daily, name="Benchmark"):
    """
    Build a benchmark return summary aligned to month-end reporting.

    Benchmarks are treated as passive total-return series using adjusted prices,
    then aggregated to month-end and chain-linked across months for trailing periods.
    """
    # Convert daily adjusted prices → month-end monthly return series
    monthly = benchmark_monthly_returns(df_daily, price_col="adj_price")
    if monthly.empty:
        return pd.DataFrame([{
            "Benchmark": name,
            "MTD": None, "QTD": None, "T3M": None, "YTD": None, "TTM": None, "T2Y": None, "T5Y": None
        }])

    # As-of date = latest month-end available in the monthly series
    asof = monthly["period_end_date"].max()

    # Period starts (month-end anchored)
    mtd_start = asof.replace(day=1)
    qtd_start = asof.replace(day=1) - relativedelta(months=(asof.month - 1) % 3)
    ytd_start = asof.replace(month=1, day=1)

    # Helper: chain-link monthly returns over a subset
    def linked(sub):
        if sub is None or sub.empty:
            return None
        return calculate_twr_from_monthly(sub, return_col="monthly_return")

    return pd.DataFrame([{
        "Benchmark": name,
        # “MTD/QTD/YTD” here means “since period start through latest month-end”
        "MTD": linked(monthly[monthly["period_end_date"] >= mtd_start]),
        "QTD": linked(monthly[monthly["period_end_date"] >= qtd_start]),
        "YTD": linked(monthly[monthly["period_end_date"] >= ytd_start]),
        # Trailing horizons (chain-linked month-end series)
        "T3M": calculate_trailing_return_benchmark_eom(df_daily, 3, price_col="adj_price"),
        "TTM": calculate_trailing_return_benchmark_eom(df_daily, 12, price_col="adj_price"),
        "T2Y": calculate_trailing_return_benchmark_eom(df_daily, 24, price_col="adj_price"),
        "T5Y": calculate_trailing_return_benchmark_eom(df_daily, 60, price_col="adj_price"),
    }])

# ------------------------------------------------------------------------------
# Instrument Return Series Builder (Plot-Ready)
# ------------------------------------------------------------------------------
# Purpose:
#   - Build tidy, long-form return series for instrument-level charts
#   - Supports rolling trailing windows and Since Inception (SI) returns
#
# Output Shape (tidy / long format):
#   - period_end_date : Timestamp (x-axis)
#   - horizon         : str  ('SI', '12M', '24M', '36M', '60M')
#   - return          : float (decimal return, not %)
#
# Notes:
#   - Line *series* over time (not curves in the mathematical sense)
#   - Rolling windows will naturally produce NaNs for early periods
#   - Uses log-compounding for numerical stability and correctness
#   - No DB access; composes lower-level helpers only
#
# Intended Use:
#   - Streamlit line charts
#   - Overlaying multiple horizons for a single instrument
# ------------------------------------------------------------------------------

def build_instrument_return_series_for_chart(
    perf_df: pd.DataFrame,
    instrument: str,
    return_col: str = "md_return_net",
    windows=(12, 24, 36, 60),
    include_si: bool = True,
) -> pd.DataFrame:
    """
    Build plot-ready return series for a single instrument.

    Parameters
    ----------
    perf_df : pd.DataFrame
        Asset-level performance data (asset_performance_view)
    instrument : str
        Ticker / instrument identifier
    return_col : str
        Monthly return column to use (default: md_return_net)
    windows : tuple[int]
        Rolling trailing windows in months (e.g. 12, 24, 36, 60)
    include_si : bool
        Whether to include Since Inception cumulative return series

    Returns
    -------
    pd.DataFrame
        Long-form dataframe with columns:
        - period_end_date
        - horizon
        - return
    """
    # 1) Base monthly return series
    monthly = get_instrument_monthly_returns(
        perf_df,
        instrument,
        return_col=return_col,
    )

    if monthly.empty:
        return pd.DataFrame(columns=["period_end_date", "horizon", "return"])

    # 2) Rolling trailing returns (log-compounded)
    monthly = add_rolling_trailing_returns_log(
        monthly,
        windows=windows,
    )

    # 3) Since Inception cumulative return (optional)
    if include_si:
        monthly = add_cumulative_return_si_log(monthly)

    # 4) Reshape to tidy / long format
    parts = []

    for k in windows:
        col = f"t{k}m_return"
        tmp = (
            monthly[["period_end_date", col]]
            .rename(columns={col: "return"})
            .assign(horizon=f"{k}M")
        )
        parts.append(tmp)

    if include_si:
        tmp = (
            monthly[["period_end_date", "si_return"]]
            .rename(columns={"si_return": "return"})
            .assign(horizon="SI")
        )
        parts.append(tmp)

    out = (
        pd.concat(parts, ignore_index=True)
        .dropna(subset=["return"])
        .sort_values(["horizon", "period_end_date"])
        .reset_index(drop=True)
    )

    return out



#==============================================================================
# ## --- CLI / PRINT HELPERS
# ==============================================================================
# Purpose: terminal formatting helpers (not used by Streamlit, but useful for debug).

def print_table_with_lines(df):
    col_names = df.columns.tolist()
    
    # Build header
    header = " | ".join([f"{col:<15}" for col in col_names])
    separator = "-+-".join(["-" * 15 for _ in col_names])

    print(header)
    print(separator)

    # Print each row
    for _, row in df.iterrows():
        line = " | ".join([f"{str(val):<15}" for val in row])
        print(line)


# ------------------------------------------------------------------------------
# MAIN (SCRIPT ENTRYPOINT)
# ------------------------------------------------------------------------------
def main():
    logging.info("Loading asset performance data...")
    perf_df = get_asset_performance_data()

    logging.info("Building instrument summary...")
    summary_df = build_instrument_summary(perf_df)

    summary_df["Latest NAV"] = summary_df["Latest NAV"].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "-")
    summary_df["Total Shares"] = summary_df["Total Shares"].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "-")

    print("\nInstrument-Level Returns Summary:")
    # Then immediately follow with:
    print("""
    Assumptions and Methodology:
    - Returns are net of transaction costs, taxes, and fees.
    - Sub-period returns are linked using the Modified Dietz method.
    - Periods under 12 months are not annualized unless specified (marked as 'Ann.').
    - Positions with insufficient return history are marked as 'SI' (Since Inception).
    - Holdings are filtered by active positions (i.e., open lots only).
    - All return figures reflect time-weighted performance at the position level.
    """)
    print_table_with_lines(summary_df)

    logging.info("Loading portfolio performance data...")
    portfolio_df = get_portfolio_performance_data()

    logging.info("Building portfolio summary...")
    portfolio_summary = build_portfolio_summary(portfolio_df)

    print("\nPortfolio-Level Returns Summary:")
    print_table_with_lines(pd.DataFrame([portfolio_summary]))

    portfolio_summary["Latest NAV"] = f"${portfolio_summary['Latest NAV']:,.2f}" if pd.notnull(portfolio_summary["Latest NAV"]) else "-"
    portfolio_summary["Total Shares"] = f"{portfolio_summary['Total Shares']:,.2f}" if pd.notnull(portfolio_summary["Total Shares"]) else "-"
    
    benchmark_tickers = ["XLY", "EEM", "SPY", "QQQ", "XLK", "IXUS"]
    benchmark_dfs = []

    def fmt_benchmark(val):
        if pd.isnull(val):
            return "      -"
        return f"{val:>7.2%}"

    for ticker in benchmark_tickers:
        logging.info(f"Loading benchmark data for {ticker}...")
        df = get_benchmark_data(ticker)
        if df.empty:
            logging.warning(f"No data found for {ticker}. Skipping.")
            continue

        logging.info(f"{ticker} data loaded: {df['price_date'].min().date()} to {df['price_date'].max().date()}")
        summary_df = build_benchmark_summary(df, name=ticker)

        for col in ["MTD", "QTD", "T3M", "YTD", "TTM", "T2Y", "T5Y"]:
            summary_df[col] = summary_df[col].apply(fmt_benchmark)

        summary_df = summary_df[["Benchmark", "MTD", "QTD", "T3M", "YTD", "TTM", "T2Y", "T5Y"]]
        benchmark_dfs.append(summary_df)

    full_benchmark_summary = pd.concat(benchmark_dfs, ignore_index=True)

    print("\nBenchmark Return Summary:")
    print_table_with_lines(full_benchmark_summary)

    print("IMPORT CHECK:", "build_instrument_return_series_for_chart" in globals())


if __name__ == "__main__":
    main()