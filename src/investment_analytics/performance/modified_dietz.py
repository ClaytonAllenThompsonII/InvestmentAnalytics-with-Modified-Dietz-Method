
"""Calendar-period portfolio performance calculations."""

from datetime import date
from decimal import Decimal
from dateutil.relativedelta import relativedelta
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


def calculate_calendar_return_details(
    monthly_returns: pd.DataFrame,
    inception_date: date,
) -> dict[str, dict]:
    """
    Calculate MTD, QTD, and YTD returns with reporting metadata.

    Uses the existing calendar-return function for geometric
    linking, then attaches reporting dates and data-quality status.

    Returns
    -------
    dict
        One record per horizon containing:
        - return
        - start_date
        - as_of_date
        - period_status
        - valuation_status
    """

    # Reuse our validated return calculations.
    linked_returns = calculate_calendar_returns(
        monthly_returns,
        inception_date,
    )

    # Reporting is anchored to the latest available NAV date.
    as_of = monthly_returns["nav_date"].max()
    starts = horizon_starts(as_of, inception_date)

    latest_month = monthly_returns.loc[
        monthly_returns["nav_date"] == as_of
    ].iloc[-1]

    latest_month_complete = (
        latest_month["period_status"] == "complete"
    )

    results = {}

    for horizon, linked_return in linked_returns.items():

        start_date = starts[horizon]
        start_month = start_date.replace(day=1)

        # Identify monthly observations contributing to the return.
        selected = monthly_returns.loc[
            (monthly_returns["period_start"] >= start_month)
            & (monthly_returns["period_start"] <= as_of)
        ]

        # A linked return is partial if ANY underlying
        # monthly observation has incomplete valuation coverage.
        valuation_status = (
            "complete"
            if selected["valuation_status"].eq("complete").all()
            else "partial"
        )

        # Determine whether the reporting calendar period has ended.
        if horizon == "MTD":
            period_complete = latest_month_complete

        elif horizon == "QTD":
            period_complete = (
                latest_month_complete
                and as_of.month in (3, 6, 9, 12)
            )

        elif horizon == "YTD":
            period_complete = (
                latest_month_complete
                and as_of.month == 12
            )

        else:
            raise ValueError(f"Unsupported horizon: {horizon}")

        results[horizon] = {
            "return": linked_return,
            "start_date": start_date,
            "as_of_date": as_of,
            "period_status": (
                "complete" if period_complete else "in_progress"
            ),
            "valuation_status": valuation_status,
        }

    return results


def calculate_since_inception_return_details(
    monthly_returns: pd.DataFrame,
    inception_date: date,
) -> dict:
    """
    Calculate cumulative since-inception portfolio performance.

    Geometrically links monthly Modified Dietz observations
    from portfolio inception through the latest NAV date.

    Returns reporting dates and valuation-quality metadata.
    """

    if monthly_returns.empty:
        raise ValueError("No monthly performance observations available.")

    as_of = monthly_returns["nav_date"].max()
    inception_month = inception_date.replace(day=1)

    selected = monthly_returns.loc[
        (monthly_returns["period_start"] >= inception_month)
        & (monthly_returns["period_start"] <= as_of)
    ].sort_values("period_start")

    expected_months = (
        (as_of.year - inception_month.year) * 12
        + as_of.month - inception_month.month
        + 1
    )

    if (
        len(selected) != expected_months
        or selected["period_start"].duplicated().any()
    ):
        raise ValueError(
            "SI: Missing or duplicate monthly observations."
        )

    if selected["md_return"].isna().any():
        raise ValueError("SI: Missing Modified Dietz return.")

    return {
        "return": geometric_link(selected["md_return"]),
        "start_date": inception_date,
        "as_of_date": as_of,
        "period_status": "in_progress",
        "valuation_status": (
            "complete"
            if selected["valuation_status"].eq("complete").all()
            else "partial"
        ),
    }


def calculate_trailing_monthly_return_details(
    monthly_returns: pd.DataFrame,
) -> dict[str, dict]:
    """
    Calculate trailing 12-, 24-, 36-, and 60-month
    geometrically linked Modified Dietz returns.

    Uses only completed calendar months.

    Returns cumulative, non-annualized performance,
    reporting dates, and valuation-quality metadata.
    """

    if monthly_returns.empty:
        raise ValueError("No monthly performance observations.")

    if monthly_returns["period_start"].duplicated().any():
        raise ValueError("Duplicate monthly periods detected.")

    monthly = monthly_returns.sort_values("period_start")

    # Identify the latest completed reporting month.
    completed = monthly.loc[
        monthly["period_status"] == "complete"
    ]

    if completed.empty:
        raise ValueError("No completed monthly periods available.")

    latest = completed.iloc[-1]

    end_month = latest["period_start"]
    end_date = latest["period_end"]
    as_of_date = latest["nav_date"]

    first_month = monthly.iloc[0]["period_start"]

    results = {}

    for months in (12, 24, 36, 60):

        horizon = f"T{months}M"

        start_month = end_month - relativedelta(
            months=months - 1
        )

        # Insufficient history for this trailing horizon.
        if start_month < first_month:
            results[horizon] = {
                "return": None,
                "start_date": start_month,
                "end_date": end_date,
                "as_of_date": as_of_date,
                "period_status": "unavailable",
                "valuation_status": "unavailable",
            }
            continue

        selected = monthly.loc[
            (monthly["period_start"] >= start_month)
            & (monthly["period_start"] <= end_month)
        ].sort_values("period_start")

        # Ensure every expected calendar month exists.
        expected = [
            start_month + relativedelta(months=i)
            for i in range(months)
        ]

        if selected["period_start"].tolist() != expected:
            raise ValueError(
                f"{horizon}: Missing or duplicate monthly periods."
            )

        if not selected["period_status"].eq("complete").all():
            raise ValueError(
                f"{horizon}: Incomplete reporting month detected."
            )

        if selected["md_return"].isna().any():
            raise ValueError(
                f"{horizon}: Missing Modified Dietz return."
            )

        linked_return = geometric_link(
            selected["md_return"]
        )

        valuation_status = (
            "complete"
            if selected["valuation_status"].eq("complete").all()
            else "partial"
        )

        results[horizon] = {
            "return": linked_return,
            "start_date": start_month,
            "end_date": end_date,
            "as_of_date": as_of_date,
            "period_status": "complete",
            "valuation_status": valuation_status,
        }

    return results
