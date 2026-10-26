
"""
Individual-security performance reporting.

Geometrically links daily security returns across standard
reporting horizons while preserving partial investment histories,
inactive periods, and valuation-quality exceptions.

Daily returns are sourced from mart.security_daily_twr_v.
"""
from decimal import Decimal

from calendar import monthrange
from datetime import date
from dateutil.relativedelta import relativedelta

[]
import pandas as pd

from .horizons import horizon_starts
from .linking import geometric_link

from .annualization import annualize_return


HORIZONS = (
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
)

TRAILING_HORIZONS = {
    "T12M", "T24M", "T36M", "T60M",
}

RETURN_COLUMNS = {
    "gross_daily_return",
    "net_fee_daily_return",
    "after_withholding_daily_return",
}

FLOW_COLUMNS = (
    "purchases",
    "sales",
    "gross_dividend_paid",
    "adr_fee_cash",
    "withholding_tax_cash",
)


def _calendar_period_end(
    horizon: str,
    as_of: date,
) -> date:
    """Determine the calendar end date for MTD, QTD, or YTD."""

    if horizon == "MTD":
        end_month = as_of.month
    elif horizon == "QTD":
        end_month = ((as_of.month - 1) // 3 + 1) * 3
    elif horizon == "YTD":
        end_month = 12
    else:
        raise ValueError(f"Not a calendar horizon: {horizon}")

    return date(
        as_of.year,
        end_month,
        monthrange(as_of.year, end_month)[1],
    )


def calculate_security_daily_return_details(
    daily_returns: pd.DataFrame,
    inception_date: date,
    as_of_date: date,
    return_column: str = "gross_daily_return",
) -> dict[str, dict]:
    """
    Calculate seven reporting horizons for one security.

    Parameters
    ----------
    daily_returns:
        Complete daily history for one instrument from
        mart.security_daily_twr_v, including inactive dates.

    inception_date:
        Portfolio reporting inception date. Used to maintain
        common requested horizon boundaries.

    as_of_date:
        Shared portfolio reporting endpoint.

    return_column:
        One of the three security daily-return conventions.

    Notes
    -----
    - Complete daily returns are geometrically linked.
    - No-capital days are excluded from linking.
    - Missing valuations invalidate the affected horizon.
    - Partial ownership histories retain available returns.
    - Inactive gaps between investment episodes are flagged.
    - Trailing horizons exclude the anchor-date return,
      except for an initial purchase on that date.
    """

    if return_column not in RETURN_COLUMNS:
        raise ValueError(
            f"Unsupported return column: {return_column}"
        )


    if daily_returns.empty:
        raise ValueError("No security daily observations.")

    required = {
        "date",
        "previous_valuation_date",
        "beginning_value",
        "ending_value",
        "valuation_status",
        return_column,
        *FLOW_COLUMNS,
    }

    missing = required - set(daily_returns.columns)

    if missing:
        raise ValueError(
            f"Missing security reporting columns: {sorted(missing)}"
        )

    daily = daily_returns.copy()

    daily["date"] = pd.to_datetime(
        daily["date"]
    ).dt.date

    daily["previous_valuation_date"] = pd.to_datetime(
        daily["previous_valuation_date"]
    ).dt.date

    if daily["date"].isna().any():
        raise ValueError("Missing security valuation dates.")

    if daily["date"].duplicated().any():
        raise ValueError("Duplicate security valuation dates.")

    if (
        "instrument" in daily.columns
        and daily["instrument"].nunique() != 1
    ):
        raise ValueError(
            "Pass observations for exactly one instrument."
        )

    if daily[list(FLOW_COLUMNS)].isna().any().any():
        raise ValueError("Missing classified cash-flow values.")

    valid_statuses = {
        "complete", "no_capital", "unpriced",
    }

    if not daily["valuation_status"].isin(valid_statuses).all():
        raise ValueError("Unexpected security valuation status.")

    as_of = pd.Timestamp(as_of_date).date()

    daily = daily.loc[
        daily["date"] <= as_of
    ].sort_values("date").reset_index(drop=True)

    if daily.empty or daily.iloc[-1]["date"] != as_of:
        raise ValueError(
            "Security history does not reach the reporting as-of date."
        )

    starts = horizon_starts(as_of, inception_date)

    # The initial dataset observation has no preceding value.
    # It can serve as a baseline if its ending value is known
    # and it contains no cash-flow events.
    daily["_baseline"] = (
        daily["previous_valuation_date"].isna()
        & daily["date"].eq(daily["date"].min())
        & daily["ending_value"].notna()
        & daily[list(FLOW_COLUMNS)].eq(0).all(axis=1)
    )

    daily["_has_flow"] = (
        daily[list(FLOW_COLUMNS)].ne(0).any(axis=1)
    )

    results = {}

    for horizon in HORIZONS:
        start_date = starts[horizon]
        is_trailing = horizon in TRAILING_HORIZONS

        # Match the portfolio convention:
        # calendar horizons include the start-date return;
        # trailing horizons exclude the anchor-date return.
        #
        # Exception: a newly opened security on the exact
        # anniversary date needs its purchase-day return.
        opening_on_anchor = (
            daily["date"].eq(start_date)
            & daily["beginning_value"].eq(0)
            & daily["purchases"].gt(0)
        )

        if is_trailing:
            mask = (
                daily["date"].gt(start_date)
                | opening_on_anchor
            )
            anchors = daily.loc[
                daily["date"] <= start_date
            ]
        else:
            mask = daily["date"].ge(start_date)
            anchors = daily.loc[
                daily["date"] < start_date
            ]

        selected = daily.loc[mask].copy()

        anchor = (
            anchors.iloc[-1]
            if not anchors.empty
            else None
        )

        # Was economic capital already invested at the
        # requested reporting boundary?
        invested_at_start = False

        if anchor is not None:
            anchor_value = anchor["ending_value"]

            if (
                is_trailing
                and anchor["date"] == start_date
            ):
                anchor_value = anchor["beginning_value"]

            invested_at_start = (
                pd.notna(anchor_value)
                and anchor_value > 0
            )

        # An initial baseline is not a missing return.
        missing_values = selected.loc[
            selected["valuation_status"].eq("unpriced")
            & ~selected["_baseline"]
        ]

        # Cash activity without invested capital must not
        # silently disappear from the linked return.
        invalid_inactive = selected.loc[
            selected["valuation_status"].eq("no_capital")
            & selected["_has_flow"]
        ]

        active = selected.loc[
            selected["valuation_status"].eq("complete")
        ]

        active_returns = active[return_column]

        invalid_returns = active_returns.isna().any()

        if not invalid_returns:
            invalid_returns = (
                active_returns.map(lambda value: value < -1)
                .any()
            )

        # A trailing starting valuation is also required
        # to be valid, unless it is the known first baseline.
        invalid_anchor = (
            is_trailing
            and anchor is not None
            and anchor["valuation_status"] == "unpriced"
            and not anchor["_baseline"]
        )

        has_missing_data = (
            not missing_values.empty
            or not invalid_inactive.empty
            or invalid_returns
            or invalid_anchor
        )

        # Determine whether ownership was intermittent.
        ownership_gap = False

        if not active.empty:
            first_active = active["date"].min()
            last_active = active["date"].max()

            between = selected.loc[
                selected["date"].gt(first_active)
                & selected["date"].lt(last_active)
            ]

            ownership_gap = (
                between["valuation_status"]
                .eq("no_capital")
                .any()
            )

        is_partial = not invested_at_start

        if active.empty:
            history_status = "unavailable"
            effective_start = None
        else:
            effective_start = (
                active["date"].min()
                if is_partial
                else start_date
            )

            if is_partial and ownership_gap:
                history_status = "partial_intermittent"
            elif ownership_gap:
                history_status = "intermittent"
            elif is_partial:
                history_status = "partial"
            else:
                history_status = "full"

        # Never link across unresolved valuations.
        if has_missing_data:
            linked_return = None
            valuation_status = "incomplete"

        elif active.empty:
            linked_return = None
            valuation_status = "unavailable"

        else:
            linked_return = geometric_link(active_returns)
            valuation_status = "complete"

        if is_trailing:
            period_status = "rolling"
        else:
            period_status = (
                "complete"
                if as_of >= _calendar_period_end(
                    horizon, as_of
                )
                else "in_progress"
            )

        results[horizon] = {
            "return": linked_return,
            "start_date": start_date,
            "effective_start_date": effective_start,
            "valuation_start_date": (
                anchor["date"]
                if is_trailing and anchor is not None
                else None
            ),
            "as_of_date": as_of,
            "period_status": period_status,
            "valuation_status": valuation_status,
            "history_status": history_status,
            "is_partial_period": is_partial,
            "has_ownership_gap": bool(ownership_gap),
            "complete_days": len(active),
            "missing_valuation_days": len(missing_values),
        }

    return results





MONTHLY_CAPITAL_COLUMNS = {
    "gross_md_return": "gross_avg_capital",
    "net_fee_md_return": "net_fee_avg_capital",
    "after_withholding_md_return": "after_withholding_avg_capital",
}

MONTHLY_RETURN_COLUMNS = set(MONTHLY_CAPITAL_COLUMNS)

MONTHLY_FLOW_COLUMNS = (
    "trade_cash",
    "dividend_cash",
    "adr_fee_cash",
    "withholding_tax_cash",
)


def calculate_security_monthly_return_details(
    monthly_returns: pd.DataFrame,
    inception_date: date,
    as_of_date: date,
    last_completed_month: date,
    return_column: str = "gross_md_return",
) -> dict[str, dict]:
    """
    Link security-level monthly Modified Dietz returns.

    Calendar horizons include the current reporting month.
    Trailing horizons use completed calendar months only.

    Inactive months are excluded. Active months with invalid
    valuations, capital, or returns invalidate the horizon.

    Partial and intermittent histories are retained and flagged.
    """

    if return_column not in MONTHLY_RETURN_COLUMNS:
        raise ValueError(
            f"Unsupported return column: {return_column}"
        )
    
    capital_column = MONTHLY_CAPITAL_COLUMNS[return_column]

    if monthly_returns.empty:
        raise ValueError("No monthly security observations.")
    

    required = {
        "period_start",
        "nav_date",
        "value_bom",
        "value_eom",
        capital_column,
        return_column,
        *MONTHLY_FLOW_COLUMNS,
    }

    missing = required - set(monthly_returns.columns)
    if missing:
        raise ValueError(
            f"Missing monthly columns: {sorted(missing)}"
        )

    monthly = monthly_returns.copy()

    for col in ("period_start", "nav_date"):
        monthly[col] = pd.to_datetime(monthly[col]).dt.date

    if monthly["period_start"].isna().any():
        raise ValueError("Missing monthly period dates.")

    if monthly["period_start"].duplicated().any():
        raise ValueError("Duplicate monthly security periods.")

    monthly = monthly.sort_values(
        "period_start"
    ).reset_index(drop=True)

    as_of = pd.Timestamp(as_of_date).date()
    completed_month = pd.Timestamp(
        last_completed_month
    ).date().replace(day=1)

    if completed_month > as_of.replace(day=1):
        raise ValueError(
            "Completed month cannot be after reporting date."
        )

    monthly = monthly.loc[
        monthly["nav_date"] <= as_of
    ].copy()

    if monthly.empty:
        raise ValueError("No monthly data through as-of date.")

    starts = horizon_starts(as_of, inception_date)

    # Inactive means zero beginning/end economic value
    # and no attributable monthly cash movements.
    monthly["_inactive"] = (
        monthly["value_bom"].eq(0)
        & monthly["value_eom"].eq(0)
        & monthly[list(MONTHLY_FLOW_COLUMNS)]
            .eq(0).all(axis=1)
    )

    results = {}

    for horizon in HORIZONS:

        is_trailing = horizon in TRAILING_HORIZONS

        if is_trailing:
            n_months = int(horizon[1:-1])

            end_month = completed_month
            start_month = end_month - relativedelta(
                months=n_months - 1
            )

        else:
            start_month = starts[horizon].replace(day=1)
            end_month = as_of.replace(day=1)

        # Allow partial history before the first stored month.
        available_start = max(
            start_month,
            monthly["period_start"].iloc[0],
        )

        selected = monthly.loc[
            monthly["period_start"].between(
                available_start, end_month
            )
        ].copy()

        expected_count = (
            (end_month.year - available_start.year) * 12
            + end_month.month - available_start.month
            + 1
        )

        expected_months = [
            available_start + relativedelta(months=i)
            for i in range(max(expected_count, 0))
        ]

        if selected["period_start"].tolist() != expected_months:
            raise ValueError(
                f"{horizon}: Missing monthly observations."
            )

        active = selected.loc[
            ~selected["_inactive"]
        ]

        capital = active[capital_column]

        bad_active = active.loc[
            active["value_bom"].isna()
            | active["value_eom"].isna()
            | capital.isna()
            | capital.le(0)
            | active[return_column].isna()
            | active[return_column].lt(-1)
        ]

        if active.empty:
            linked_return = None
            valuation_status = "unavailable"
            history_status = "unavailable"
            first_active_month = None
            ownership_gap = False
            is_partial = True

        else:
            first_active_month = active.iloc[0]["period_start"]
            last_active_month = active.iloc[-1]["period_start"]

            beginning_month = (
                selected.iloc[0]
                if not selected.empty
                else None
            )

            invested_at_start = (
                beginning_month is not None
                and beginning_month["period_start"] == start_month
                and pd.notna(beginning_month["value_bom"])
                and beginning_month["value_bom"] > 0
            )

            is_partial = not invested_at_start

            middle = selected.loc[
                selected["period_start"].gt(first_active_month)
                & selected["period_start"].lt(last_active_month)
            ]

            ownership_gap = bool(middle["_inactive"].any())

            if is_partial and ownership_gap:
                history_status = "partial_intermittent"
            elif ownership_gap:
                history_status = "intermittent"
            elif is_partial:
                history_status = "partial"
            else:
                history_status = "full"

            if not bad_active.empty:
                linked_return = None
                valuation_status = "incomplete"
            else:
                linked_return = geometric_link(
                    active[return_column]
                )
                valuation_status = "complete"

        if is_trailing:
            period_status = "complete"
        else:
            latest_month = selected.iloc[-1]

            month_finished = (
                latest_month["nav_date"].year,
                latest_month["nav_date"].month,
            ) > (
                end_month.year,
                end_month.month,
            )

            # Conservatively regard the current calendar
            # month as in progress until calendar month-end.
            from calendar import monthrange

            last_calendar_day = date(
                end_month.year,
                end_month.month,
                monthrange(
                    end_month.year,
                    end_month.month
                )[1],
            )

            current_complete = (
                latest_month["nav_date"] >= last_calendar_day
            )

            if horizon == "MTD":
                period_complete = current_complete
            elif horizon == "QTD":
                period_complete = (
                    current_complete
                    and end_month.month in (3, 6, 9, 12)
                )
            else:
                period_complete = (
                    current_complete and end_month.month == 12
                )

            period_status = (
                "complete" if period_complete
                else "in_progress"
            )

        results[horizon] = {
            "return": linked_return,
            "start_date": start_month,
            "effective_start_month": first_active_month,
            "as_of_date": (
                selected.iloc[-1]["nav_date"]
                if not selected.empty
                else as_of
            ),
            "period_status": period_status,
            "valuation_status": valuation_status,
            "history_status": history_status,
            "is_partial_period": is_partial,
            "has_ownership_gap": ownership_gap,
            "active_months": len(active),
            "invalid_months": len(bad_active),
        }

    return results


SECURITY_RETURN_BASES = {
    "gross": (
        "gross_daily_return",
        "gross_md_return",
    ),
    "net_fee": (
        "net_fee_daily_return",
        "net_fee_md_return",
    ),
    "after_withholding": (
        "after_withholding_daily_return",
        "after_withholding_md_return",
    ),
}


def build_security_performance_report(
    daily_returns: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    current_holdings: pd.DataFrame,
    inception_date: date,
    as_of_date: date,
    last_completed_month: date,
) -> pd.DataFrame:
    """
    Build security performance records for current holdings.

    Produces one row per:
        security x methodology x return basis x horizon.

    Partial and intermittent histories retain cumulative
    returns but are not annualized.

    Portfolio and benchmark performance are handled separately.
    """

    if current_holdings.empty:
        raise ValueError("No current holdings available.")

    if current_holdings["instrument"].duplicated().any():
        raise ValueError("Duplicate current holdings.")

    holdings = current_holdings.sort_values(
        "current_value",
        ascending=False,
    )

    records = []

    for position in holdings.itertuples(index=False):

        instrument = position.instrument

        security_daily = daily_returns.loc[
            daily_returns["instrument"] == instrument
        ].copy()

        security_monthly = monthly_returns.loc[
            monthly_returns["instrument"] == instrument
        ].copy()

        if security_daily.empty or security_monthly.empty:
            raise ValueError(
                f"{instrument}: Missing performance history."
            )

        for basis, (daily_col, monthly_col) in (
            SECURITY_RETURN_BASES.items()
        ):

            daily_details = (
                calculate_security_daily_return_details(
                    daily_returns=security_daily,
                    inception_date=inception_date,
                    as_of_date=as_of_date,
                    return_column=daily_col,
                )
            )

            monthly_details = (
                calculate_security_monthly_return_details(
                    monthly_returns=security_monthly,
                    inception_date=inception_date,
                    as_of_date=as_of_date,
                    last_completed_month=last_completed_month,
                    return_column=monthly_col,
                )
            )

            methods = {
                "daily_twr": daily_details,
                "modified_dietz": monthly_details,
            }

            for methodology, details in methods.items():

                for horizon in HORIZONS:

                    result = details[horizon]
                    cumulative = result["return"]
                    annualized = None

                    full_coverage = (
                        result["valuation_status"] == "complete"
                        and result["history_status"] == "full"
                    )

                    if (
                        horizon in TRAILING_HORIZONS
                        and cumulative is not None
                        and full_coverage
                    ):

                        if methodology == "daily_twr":
                            anchor = result["valuation_start_date"]

                            if anchor is not None:
                                elapsed_days = (
                                    result["as_of_date"] - anchor
                                ).days

                                years = (
                                    Decimal(elapsed_days)
                                    / Decimal("365.25")
                                )
                            else:
                                years = Decimal(0)

                        else:
                            months = int(horizon[1:-1])
                            years = (
                                Decimal(months) / Decimal(12)
                            )

                        if years > 0:
                            annualized = annualize_return(
                                cumulative,
                                years,
                            )

                    effective_start = (
                        result.get("effective_start_date")
                        if methodology == "daily_twr"
                        else result.get("effective_start_month")
                    )

                    records.append({
                        "asset": instrument,
                        "asset_group": "security",
                        "current_value": position.current_value,
                        "methodology": methodology,
                        "return_basis": basis,
                        "horizon": horizon,
                        "cumulative_return": cumulative,
                        "annualized_return": annualized,
                        "start_date": result["start_date"],
                        "effective_start_date": effective_start,
                        "valuation_start_date": result.get(
                            "valuation_start_date"
                        ),
                        "as_of_date": result["as_of_date"],
                        "period_status": result["period_status"],
                        "valuation_status": result["valuation_status"],
                        "history_status": result["history_status"],
                        "is_partial_period": result["is_partial_period"],
                        "has_ownership_gap": result["has_ownership_gap"],
                    })

    return pd.DataFrame(records)

