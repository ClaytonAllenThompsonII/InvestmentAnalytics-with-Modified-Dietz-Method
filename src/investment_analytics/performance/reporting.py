
"""
Unified portfolio and benchmark performance reporting.

Daily TWR and Modified Dietz determine reporting periods.
Benchmark returns use adjusted closing prices.
"""

from datetime import date
from decimal import Decimal

import pandas as pd

from .annualization import annualize_return
from .benchmarks import (
    calculate_benchmark_total_return,
    resolve_benchmark_valuation_dates,
)
from .daily_twr import (
    calculate_daily_calendar_return_details,
    calculate_daily_trailing_return_details,
)
from .modified_dietz import (
    calculate_calendar_return_details,
    calculate_trailing_monthly_return_details,
)


HORIZONS = (
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
)

TRAILING_MONTHS = {
    "T12M": 12,
    "T24M": 24,
    "T36M": 36,
    "T60M": 60,
}

BENCHMARKS = (
    "SPY", "QQQ", "XLK", "XLY", "EEM", "IXUS",
)


def build_portfolio_performance_report(
    monthly_returns: pd.DataFrame,
    daily_returns: pd.DataFrame,
    inception_date: date,
    benchmark_prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build a unified portfolio and benchmark performance dataset.

    Returns one observation per asset, methodology, and horizon.

    Benchmark prices are optional to preserve compatibility
    with portfolio-only reporting.
    """

    twr = {
        **calculate_daily_calendar_return_details(
            daily_returns, inception_date
        ),
        **calculate_daily_trailing_return_details(
            daily_returns, inception_date
        ),
    }

    md = {
        **calculate_calendar_return_details(
            monthly_returns, inception_date
        ),
        **calculate_trailing_monthly_return_details(
            monthly_returns
        ),
    }

    methods = {
        "daily_twr": twr,
        "modified_dietz": md,
    }

    records = []

    for methodology, details in methods.items():
        for horizon in HORIZONS:
            result = details[horizon]
            cumulative = result["return"]
            annualized = None

            # Portfolio annualization
            if horizon in TRAILING_MONTHS and cumulative is not None:
                if methodology == "modified_dietz":
                    years = (
                        Decimal(TRAILING_MONTHS[horizon])
                        / Decimal(12)
                    )
                else:
                    elapsed_days = (
                        result["as_of_date"]
                        - result["valuation_start_date"]
                    ).days

                    years = (
                        Decimal(elapsed_days)
                        / Decimal("365.25")
                    )

                annualized = annualize_return(cumulative, years)

            records.append({
                "asset": "Portfolio",
                "methodology": methodology,
                "horizon": horizon,
                "cumulative_return": cumulative,
                "annualized_return": annualized,
                "start_date": result["start_date"],
                "valuation_start_date": result.get(
                    "valuation_start_date"
                ),
                "as_of_date": result["as_of_date"],
                "period_status": result["period_status"],
                "valuation_status": result["valuation_status"],
            })

            if benchmark_prices is None:
                continue

            # Benchmark total returns over matching periods
            for instrument in BENCHMARKS:
                benchmark_return = None
                benchmark_annualized = None
                anchor = None
                benchmark_status = "unavailable"

                if cumulative is not None:
                    include_start = (
                        methodology == "modified_dietz"
                        or horizon in ("MTD", "QTD", "YTD")
                    )

                    anchor, ending = resolve_benchmark_valuation_dates(
                        benchmark_prices,
                        instrument,
                        result["start_date"],
                        result["as_of_date"],
                        include_start_date_return=include_start,
                    )

                    # Require identical beginning valuations for
                    # the portfolio and benchmark trailing TWR.
                    if (
                        methodology == "daily_twr"
                        and horizon in TRAILING_MONTHS
                        and anchor != result["valuation_start_date"]
                    ):
                        raise ValueError(
                            f"{instrument} {horizon}: Benchmark "
                            "anchor differs from portfolio anchor."
                        )

                    benchmark_return = calculate_benchmark_total_return(
                        benchmark_prices,
                        instrument,
                        anchor,
                        ending,
                    )

                    if horizon in TRAILING_MONTHS:
                        if methodology == "modified_dietz":
                            years = (
                                Decimal(TRAILING_MONTHS[horizon])
                                / Decimal(12)
                            )
                        else:
                            years = (
                                Decimal((ending - anchor).days)
                                / Decimal("365.25")
                            )

                        benchmark_annualized = annualize_return(
                            benchmark_return, years
                        )

                    benchmark_status = "complete"

                records.append({
                    "asset": instrument,
                    "methodology": methodology,
                    "horizon": horizon,
                    "cumulative_return": benchmark_return,
                    "annualized_return": benchmark_annualized,
                    "start_date": result["start_date"],
                    "valuation_start_date": anchor,
                    "as_of_date": result["as_of_date"],
                    "period_status": result["period_status"],
                    "valuation_status": benchmark_status,
                })

    return pd.DataFrame(records)
