
"""Reporting horizon definitions for portfolio performance."""

from datetime import date
from dateutil.relativedelta import relativedelta


def horizon_starts(
    as_of: date,
    inception_date: date,
) -> dict[str, date]:
    """
    Return calendar start dates for standard performance horizons.

    All horizons are anchored to the latest available NAV date.

    Notes
    -----
    MTD, QTD, YTD:
        Calendar-period starts.

    T12M, T24M, T36M, T60M:
        Exact calendar-anniversary valuation anchors.

    SI:
        Portfolio inception date.

    This function defines dates only. It does not calculate
    returns, resolve non-trading-day NAVs, or validate whether
    sufficient historical data exists for a trailing horizon.
    """
    if as_of < inception_date:
        raise ValueError("As-of date precedes portfolio inception.")

    quarter_start_month = ((as_of.month - 1) // 3) * 3 + 1

    return {
        "MTD": max(as_of.replace(day=1), inception_date),
        "QTD": max(
            date(as_of.year, quarter_start_month, 1),
            inception_date,
        ),
        "YTD": max(date(as_of.year, 1, 1), inception_date),
        "T12M": as_of - relativedelta(months=12),
        "T24M": as_of - relativedelta(months=24),
        "T36M": as_of - relativedelta(months=36),
        "T60M": as_of - relativedelta(months=60),
        "SI": inception_date,
    }
