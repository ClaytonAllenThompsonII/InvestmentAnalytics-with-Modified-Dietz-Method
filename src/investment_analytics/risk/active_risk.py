
"""Rolling benchmark-relative risk analytics."""

import numpy as np
import pandas as pd

from investment_analytics.data_access.performance import (
    get_portfolio_monthly_returns,
    get_benchmark_daily_prices,
)


TRADING_MONTHS_PER_YEAR = 12
DEFAULT_WINDOW_MONTHS = 24


def calculate_rolling_tracking_error(
    portfolio_monthly: pd.DataFrame,
    benchmark_daily_prices: pd.DataFrame,
    benchmark: str = "SPY",
    window_months: int = DEFAULT_WINDOW_MONTHS,
) -> pd.DataFrame:
    """
    Calculate rolling annualized tracking error.

    Uses monthly portfolio Modified Dietz returns minus monthly
    adjusted-price benchmark returns.

    Requires consecutive, complete monthly return observations.
    The current partial reporting month is excluded.
    """

    if window_months < 2:
        raise ValueError("Rolling window must contain at least 2 months.")

    # -------------------------------------------------------------
    # PORTFOLIO MONTHLY RETURNS
    # -------------------------------------------------------------

    portfolio = portfolio_monthly.copy()

    if portfolio.empty:
        raise ValueError("No monthly portfolio returns available.")

    portfolio["period_start"] = pd.to_datetime(
        portfolio["period_start"]
    )
    portfolio["nav_date"] = pd.to_datetime(
        portfolio["nav_date"]
    )
    portfolio["md_return"] = pd.to_numeric(
        portfolio["md_return"],
        errors="coerce",
    )

    as_of = portfolio["nav_date"].max()

    if pd.isna(as_of):
        raise ValueError("Missing portfolio reporting date.")

    # Exclude the current MTD reporting period.
    last_completed_month = as_of.to_period("M") - 1

    portfolio["month"] = (
        portfolio["period_start"].dt.to_period("M")
    )

    portfolio = portfolio.loc[
        portfolio["month"] <= last_completed_month
    ].copy()

    if portfolio.empty:
        raise ValueError("No completed portfolio months available.")

    if portfolio["month"].duplicated().any():
        raise ValueError("Duplicate monthly portfolio observations.")

    portfolio_returns = (
        portfolio
        .set_index("month")["md_return"]
        .rename("portfolio_return")
        .sort_index()
    )

    # -------------------------------------------------------------
    # BENCHMARK MONTHLY RETURNS
    # -------------------------------------------------------------

    prices = benchmark_daily_prices.loc[
        benchmark_daily_prices["instrument"].eq(benchmark)
    ].copy()

    if prices.empty:
        raise ValueError(f"No benchmark prices found for {benchmark}.")

    prices["price_date"] = pd.to_datetime(
        prices["price_date"]
    )
    prices["adjusted_close"] = pd.to_numeric(
        prices["adjusted_close"],
        errors="coerce",
    )

    # Invalid adjusted prices must not generate returns.
    prices.loc[
        prices["adjusted_close"] <= 0,
        "adjusted_close",
    ] = np.nan

    # Last available trading-day adjusted price per month.
    monthly_prices = (
        prices
        .set_index("price_date")["adjusted_close"]
        .sort_index()
        .resample("ME")
        .last()
    )

    benchmark_returns = (
        monthly_prices
        .pct_change(fill_method=None)
        .rename("benchmark_return")
    )

    benchmark_returns.index = (
        benchmark_returns.index.to_period("M")
    )

    # -------------------------------------------------------------
    # ALIGN MONTHLY ACTIVE RETURNS
    # -------------------------------------------------------------

    monthly = portfolio_returns.to_frame().join(
        benchmark_returns,
        how="left",
    )

    # Explicitly preserve missing calendar months.
    # A gap must not be treated as a valid monthly observation.
    calendar = pd.period_range(
        start=monthly.index.min(),
        end=last_completed_month,
        freq="M",
    )

    monthly = monthly.reindex(calendar)

    monthly["active_return"] = (
        monthly["portfolio_return"]
        - monthly["benchmark_return"]
    )

    # -------------------------------------------------------------
    # ROLLING TRACKING ERROR
    # -------------------------------------------------------------

    monthly["tracking_error_24m"] = (
        monthly["active_return"]
        .rolling(
            window=window_months,
            min_periods=window_months,
        )
        .std(ddof=1)
        * np.sqrt(TRADING_MONTHS_PER_YEAR)
    )

    # Month-end date for chart plotting.
    monthly["month_end"] = (
        monthly.index.to_timestamp(how="end").normalize()
    )

    result = monthly.loc[
        monthly["tracking_error_24m"].notna()
    ].copy()

    if result.empty:
        raise ValueError(
            f"No complete {window_months}-month tracking error "
            "windows available. Check monthly return coverage."
        )

    return result[
        [
            "month_end",
            "portfolio_return",
            "benchmark_return",
            "active_return",
            "tracking_error_24m",
        ]
    ].reset_index(drop=True)


def load_tracking_error_report(
    benchmark: str = "SPY",
) -> pd.DataFrame:
    """Load source data and calculate rolling tracking error."""

    return calculate_rolling_tracking_error(
        portfolio_monthly=get_portfolio_monthly_returns(),
        benchmark_daily_prices=get_benchmark_daily_prices(),
        benchmark=benchmark.upper(),
    )
