#!/usr/bin/env python3

import os
import logging
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------------
# LOAD ENV + SETUP LOGGING
# ------------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ------------------------------------------------------------------------------
# DATABASE CONNECTION
# ------------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# ------------------------------------------------------------------------------
# DATA ACCESS (READS)
# ------------------------------------------------------------------------------
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
# ## --- RETURN MATH (CORE CALCULATIONS)
# ==============================================================================
# Purpose: pure-ish functions that compute returns from a provided dataframe.
# Note: keep these free of DB calls where possible (easier to test).

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

def simple_return(df, price_col="adj_price"):
    """
    Compute a simple total return over a period using adjusted prices.

    Intended for benchmark analysis (e.g., SPY, QQQ), where dividends and
    corporate actions are assumed to be continuously reinvested. Using
    adjusted prices produces a total return series without explicitly
    modeling cash flows, fees, or timing effects, consistent with
    institutional benchmark practice.
    """
    if df is None or df.empty or len(df) < 2:
        return None
    df = df.sort_values("price_date")
    start_price = df[price_col].iloc[0]
    end_price = df[price_col].iloc[-1]
    if pd.isna(start_price) or pd.isna(end_price) or start_price == 0:
        return None
    return (end_price / start_price) - 1

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

def calculate_trailing_return_from_daily(df, months, price_col="adj_price"):
    """
    Calculate a trailing total return over N months using daily price data.

    This function is designed primarily for benchmarks, not active portfolios.
    It computes a simple start-to-end total return over the trailing window
    using adjusted prices, implicitly assuming dividend reinvestment and no
    cash-flow timing effects. Unlike the Modified Dietz approach, this method
    does not account for intra-period cash flows and is therefore appropriate
    for passive index comparisons rather than manager performance attribution.
    """
    if df is None or df.empty:
        return None

    df = df.sort_values("price_date").copy()
    latest_date = df["price_date"].max()
    cutoff_date = latest_date - relativedelta(months=months)

    subset = df[df["price_date"] >= cutoff_date]
    return simple_return(subset, price_col=price_col)


# ==============================================================================
# ## --- SUMMARY BUILDERS (PRESENTATION LAYER)
# ==============================================================================
# Purpose: take canonical datasets + compute formatted outputs for tables/UI.

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


def build_benchmark_summary(df, name="Benchmark"):
    df = df.sort_values("price_date")
    latest = df["price_date"].max()
    mtd_start = latest.replace(day=1)
    qtd_start = latest.replace(day=1) - pd.DateOffset(months=(latest.month - 1) % 3)
    ytd_start = latest.replace(month=1, day=1)

    return pd.DataFrame([{
        "Benchmark": name,
        "MTD": simple_return(df[df["price_date"] >= mtd_start]),
        "QTD": simple_return(df[df["price_date"] >= qtd_start]),
        "T3M": calculate_trailing_return_from_daily(df, 3),
        "YTD": simple_return(df[df["price_date"] >= ytd_start]),
        "TTM": calculate_trailing_return_from_daily(df, 12),
        "T2Y": calculate_trailing_return_from_daily(df, 24),
        "T5Y": calculate_trailing_return_from_daily(df, 60),
    }])

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

if __name__ == "__main__":
    main()