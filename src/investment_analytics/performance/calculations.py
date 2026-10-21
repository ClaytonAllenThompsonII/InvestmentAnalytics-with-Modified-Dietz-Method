
"""Calendar-period portfolio performance calculations."""

from datetime import date
from decimal import Decimal

import pandas as pd

from .horizons import horizon_starts
from .linking import geometric_link


def calculate_calendar_returns(
    monthly_returns: pd.DataFrame,
    inception_date: date,
) -> dict[str, Decimal]:
    """
    Calculate MTD, QTD, and YTD portfolio returns.

    Monthly Modified Dietz observations are geometrically
    linked to produce cumulative calendar-period returns.

    Returns remain in decimal form.
    """

    if monthly_returns.empty:
        raise ValueError("No monthly performance observations available.")

    # Anchor calculations to the latest available NAV.
    as_of = monthly_returns["nav_date"].max()

    starts = horizon_starts(as_of, inception_date)

    results = {}

    for horizon in ("MTD", "QTD", "YTD"):

        # Monthly observations are keyed to the first day of
        # the month, including the portfolio's inception month.
        start_month = starts[horizon].replace(day=1)

        selected = monthly_returns.loc[
            (monthly_returns["period_start"] >= start_month)
            & (monthly_returns["period_start"] <= as_of)
        ].sort_values("period_start")

        # Reject incomplete or duplicate monthly histories.
        expected_months = (
            (as_of.year - start_month.year) * 12
            + as_of.month - start_month.month
            + 1
        )

        if (
            len(selected) != expected_months
            or selected["period_start"].duplicated().any()
        ):
            raise ValueError(
                f"{horizon}: Missing or duplicate monthly observations."
            )

        if selected["md_return"].isna().any():
            raise ValueError(
                f"{horizon}: Missing Modified Dietz return."
            )

        results[horizon] = geometric_link(
            selected["md_return"]
        )

    return results
