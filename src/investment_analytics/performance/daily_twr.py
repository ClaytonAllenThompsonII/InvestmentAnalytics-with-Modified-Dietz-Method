
"""
Daily-valued portfolio time-weighted return calculations.

Geometrically links daily portfolio returns across
standard calendar reporting horizons.
"""

from datetime import date
from decimal import Decimal

import pandas as pd

from .horizons import horizon_starts
from .linking import geometric_link


def calculate_daily_calendar_returns(
    daily_returns: pd.DataFrame,
    inception_date: date,
) -> dict[str, Decimal]:
    """
    Calculate MTD, QTD, and YTD daily-linked TWR.

    Returns remain in decimal form.

    The first inception-day observation has a NULL return
    and cannot be geometrically linked.
    """

    if daily_returns.empty:
        raise ValueError("No daily performance observations available.")

    if daily_returns["nav_date"].duplicated().any():
        raise ValueError("Duplicate daily NAV dates detected.")

    # Anchor reporting to the latest available valuation.
    as_of = daily_returns["nav_date"].max()

    starts = horizon_starts(as_of, inception_date)

    results = {}

    for horizon in ("MTD", "QTD", "YTD"):

        start_date = starts[horizon]

        selected = daily_returns.loc[
            (daily_returns["nav_date"] >= start_date)
            & (daily_returns["nav_date"] <= as_of)
        ].sort_values("nav_date")

        if selected.empty:
            raise ValueError(
                f"{horizon}: No daily return observations available."
            )

        if selected["daily_return"].isna().any():
            raise ValueError(
                f"{horizon}: Missing daily returns."
            )

        results[horizon] = geometric_link(
            selected["daily_return"]
        )

    return results


def calculate_daily_calendar_return_details(
    daily_returns: pd.DataFrame,
    inception_date: date,
) -> dict[str, dict]:
    """
    Calculate MTD, QTD, and YTD daily TWR with reporting metadata.

    Returns the same reporting structure as the Modified Dietz
    calculation, including dates, period status, and valuation quality.
    """

    linked_returns = calculate_daily_calendar_returns(
        daily_returns,
        inception_date,
    )

    as_of = daily_returns["nav_date"].max()
    starts = horizon_starts(as_of, inception_date)

    results = {}

    for horizon, linked_return in linked_returns.items():
        start_date = starts[horizon]

        selected = daily_returns.loc[
            (daily_returns["nav_date"] >= start_date)
            & (daily_returns["nav_date"] <= as_of)
        ]

        valuation_status = (
            "complete"
            if selected["valuation_status"].eq("complete").all()
            else "partial"
        )

        # Calendar-period completion is conservative:
        # do not mark complete before the calendar end date.
        if horizon == "MTD":
            from calendar import monthrange
            period_end = date(
                as_of.year,
                as_of.month,
                monthrange(as_of.year, as_of.month)[1],
            )
        elif horizon == "QTD":
            quarter_end_month = ((as_of.month - 1) // 3 + 1) * 3
            from calendar import monthrange
            period_end = date(
                as_of.year,
                quarter_end_month,
                monthrange(as_of.year, quarter_end_month)[1],
            )
        else:
            period_end = date(as_of.year, 12, 31)

        results[horizon] = {
            "return": linked_return,
            "start_date": start_date,
            "as_of_date": as_of,
            "period_status": (
                "complete" if as_of >= period_end else "in_progress"
            ),
            "valuation_status": valuation_status,
        }

    return results



def calculate_daily_since_inception_details(
    daily_returns: pd.DataFrame,
    inception_date: date,
) -> dict:
    """
    Calculate cumulative since-inception daily-linked TWR.

    Excludes the inception-day NULL return, since no prior
    portfolio valuation exists.

    Returns cumulative performance and reporting metadata.
    """

    if daily_returns.empty:
        raise ValueError("No daily performance observations available.")

    selected = daily_returns.loc[
        daily_returns["nav_date"] >= inception_date
    ].sort_values("nav_date")

    if selected.empty or selected.iloc[0]["nav_date"] != inception_date:
        raise ValueError("SI: Inception valuation is missing.")

    if selected["nav_date"].duplicated().any():
        raise ValueError("SI: Duplicate valuation dates detected.")

    # The inception-day return is expected to be NULL.
    if not pd.isna(selected.iloc[0]["daily_return"]):
        raise ValueError("SI: Expected NULL return on inception date.")

    # All subsequent observations must have valid returns.
    returns = selected.iloc[1:]["daily_return"]

    if returns.empty or returns.isna().any():
        raise ValueError("SI: Missing daily return observations.")

    return {
        "return": geometric_link(returns),
        "start_date": inception_date,
        "as_of_date": selected["nav_date"].max(),
        "period_status": "in_progress",
        "valuation_status": (
            "complete"
            if selected["valuation_status"].eq("complete").all()
            else "partial"
        ),
    }


def calculate_daily_trailing_return_details(
    daily_returns: pd.DataFrame,
    inception_date: date,
) -> dict[str, dict]:
    """
    Calculate trailing 12-, 24-, 36-, and 60-month daily TWR.

    Uses calendar-anniversary start dates and the latest NAV
    on or before each anniversary as the beginning valuation.

    Returns cumulative, non-annualized performance.
    """

    if daily_returns.empty:
        raise ValueError("No daily performance observations available.")

    if daily_returns["nav_date"].isna().any():
        raise ValueError("Missing NAV dates detected.")

    if daily_returns["nav_date"].duplicated().any():
        raise ValueError("Duplicate NAV dates detected.")

    daily = daily_returns.sort_values("nav_date")

    as_of = daily["nav_date"].iloc[-1]
    starts = horizon_starts(as_of, inception_date)

    results = {}

    for horizon in ("T12M", "T24M", "T36M", "T60M"):
        start_date = starts[horizon]

        # A full trailing period must be available.
        if start_date < inception_date:
            results[horizon] = {
                "return": None,
                "start_date": start_date,
                "valuation_start_date": None,
                "as_of_date": as_of,
                "period_status": "unavailable",
                "valuation_status": "unavailable",
            }
            continue

        # Starting valuation: latest NAV on or before
        # the calendar-anniversary date.
        anchors = daily.loc[
            daily["nav_date"] <= start_date
        ]

        if anchors.empty:
            raise ValueError(
                f"{horizon}: Starting NAV unavailable."
            )

        anchor = anchors.iloc[-1]

        # Exclude the starting date's return.
        selected = daily.loc[
            (daily["nav_date"] > start_date)
            & (daily["nav_date"] <= as_of)
        ]

        if selected.empty or selected["daily_return"].isna().any():
            raise ValueError(
                f"{horizon}: Missing daily return observations."
            )

        # Include beginning valuation quality because
        # the linked period depends on the starting NAV.
        fully_valued = (
            anchor["valuation_status"] == "complete"
            and selected["valuation_status"].eq("complete").all()
        )

        results[horizon] = {
            "return": geometric_link(selected["daily_return"]),
            "start_date": start_date,
            "valuation_start_date": anchor["nav_date"],
            "as_of_date": as_of,
            "period_status": "rolling",
            "valuation_status": (
                "complete" if fully_valued else "partial"
            ),
        }

    return results
