
"""
Unified portfolio performance reporting.

Combines daily TWR and linked Modified Dietz into a
consistent dataset for downstream reporting applications.

Returns remain in decimal form.
"""

from datetime import date
from decimal import Decimal

import pandas as pd

from .annualization import annualize_return
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


def build_portfolio_performance_report(
    monthly_returns: pd.DataFrame,
    daily_returns: pd.DataFrame,
    inception_date: date,
) -> pd.DataFrame:
    """
    Build a unified portfolio performance dataset.

    Combines:
        - Daily-valued TWR
        - Geometrically linked monthly Modified Dietz

    Annualizes trailing returns only.

    Modified Dietz trailing periods use completed months.
    Daily TWR trailing periods use valuation-date anchors.

    Returns one row per methodology and reporting horizon.
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

            if (
                horizon in TRAILING_MONTHS
                and cumulative is not None
            ):
                if methodology == "modified_dietz":
                    years = (
                        Decimal(TRAILING_MONTHS[horizon])
                        / Decimal(12)
                    )
                else:
                    valuation_start = result[
                        "valuation_start_date"
                    ]

                    elapsed_days = (
                        result["as_of_date"]
                        - valuation_start
                    ).days

                    years = (
                        Decimal(elapsed_days)
                        / Decimal("365.25")
                    )

                annualized = annualize_return(
                    cumulative, years
                )

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

    return pd.DataFrame(records)
