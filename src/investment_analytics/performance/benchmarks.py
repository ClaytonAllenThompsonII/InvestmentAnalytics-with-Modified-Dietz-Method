
"""Benchmark total returns using adjusted closing prices."""

from datetime import date
from decimal import Decimal

import pandas as pd


def calculate_benchmark_total_return(
    prices: pd.DataFrame,
    instrument: str,
    start_valuation_date: date,
    end_valuation_date: date,
) -> Decimal:
    """
    Calculate a benchmark's cumulative total-return proxy.

    Uses adjusted closing prices at two valuation dates.

    Formula:
        Return = (Ending Adjusted Close / Starting Adjusted Close) - 1
    """

    selected = prices.loc[
        prices["instrument"] == instrument
    ].copy()

    if selected.empty:
        raise ValueError(
            f"{instrument}: No benchmark prices available."
        )

    selected["price_date"] = pd.to_datetime(
        selected["price_date"]
    ).dt.date

    if selected["price_date"].duplicated().any():
        raise ValueError(
            f"{instrument}: Duplicate price dates detected."
        )

    selected = selected.set_index("price_date")

    if start_valuation_date >= end_valuation_date:
        raise ValueError("Invalid benchmark reporting period.")

    for required_date in (
        start_valuation_date,
        end_valuation_date,
    ):
        if required_date not in selected.index:
            raise ValueError(
                f"{instrument}: Price missing for "
                f"{required_date}."
            )

    start_price = Decimal(
        str(selected.loc[start_valuation_date, "adjusted_close"])
    )

    end_price = Decimal(
        str(selected.loc[end_valuation_date, "adjusted_close"])
    )

    if (
        not start_price.is_finite()
        or not end_price.is_finite()
        or start_price <= 0
        or end_price <= 0
    ):
        raise ValueError(
            f"{instrument}: Invalid adjusted closing price."
        )

    return (end_price / start_price) - 1


def resolve_benchmark_valuation_dates(
    prices: pd.DataFrame,
    instrument: str,
    period_start: date,
    period_end: date,
    *,
    include_start_date_return: bool,
) -> tuple[date, date]:
    """
    Resolve benchmark valuation dates for a reporting period.

    If include_start_date_return is True:
        Use the latest valuation strictly before period_start.

    If False:
        Use the latest valuation on or before period_start.

    The ending valuation must match period_end exactly.
    """

    if period_start > period_end:
        raise ValueError("Invalid benchmark reporting period.")

    dates = pd.to_datetime(
        prices.loc[
            prices["instrument"] == instrument,
            "price_date",
        ]
    ).dt.date.sort_values()

    if dates.empty:
        raise ValueError(f"{instrument}: No prices available.")

    if dates.duplicated().any():
        raise ValueError(f"{instrument}: Duplicate price dates.")

    if include_start_date_return:
        candidates = dates[dates < period_start]
    else:
        candidates = dates[dates <= period_start]

    if candidates.empty:
        raise ValueError(
            f"{instrument}: No starting valuation available."
        )

    if period_end not in set(dates):
        raise ValueError(
            f"{instrument}: Missing ending price for {period_end}."
        )

    return candidates.iloc[-1], period_end
