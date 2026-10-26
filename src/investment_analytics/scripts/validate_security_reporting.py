
"""
Validate individual-security daily TWR reporting.

Test case:
DRAM | May 2026 | Purchase and complete liquidation

Expected cumulative return:
(324.15 / 295.65) - 1 = approximately 9.6398%
"""
import math

from datetime import date

import pandas as pd

from investment_analytics.data_access.database import query_dataframe
from investment_analytics.performance.security_reporting import (
    calculate_security_daily_return_details,
    calculate_security_monthly_return_details,
    build_security_performance_report,
)


# --------------------------------------------------
# 1. Retrieve DRAM daily history from PostgreSQL
# --------------------------------------------------

query = """
SELECT
    instrument,
    date,
    previous_valuation_date,
    beginning_value,
    ending_value,
    purchases,
    sales,
    gross_dividend_paid,
    adr_fee_cash,
    withholding_tax_cash,
    gross_daily_return,
    net_fee_daily_return,
    after_withholding_daily_return,
    valuation_status

FROM mart.security_daily_twr_v

WHERE instrument = 'DRAM'
  AND date <= DATE '2026-05-15'

ORDER BY date;
"""

dram_daily = query_dataframe(query, ())

if dram_daily.empty:
    raise ValueError("No DRAM daily observations returned.")

print(f"\nDRAM observations loaded: {len(dram_daily)}")


# --------------------------------------------------
# 2. Calculate security performance
# --------------------------------------------------

results = calculate_security_daily_return_details(
    daily_returns=dram_daily,
    inception_date=date(2020, 7, 6),
    as_of_date=date(2026, 5, 15),
    return_column="gross_daily_return",
)

mtd = results["MTD"]


# --------------------------------------------------
# 3. Independent holding-period reconciliation
# --------------------------------------------------

purchase_cost = 295.65
sale_proceeds = 324.15

expected_return = (
    sale_proceeds / purchase_cost
) - 1

actual_return = float(mtd["return"])

print("\nDRAM — May 2026")
print("-" * 45)

print(f"Purchase cost:       ${purchase_cost:,.2f}")
print(f"Sale proceeds:       ${sale_proceeds:,.2f}")
print(f"Realized P&L:        ${sale_proceeds - purchase_cost:,.2f}")

print(f"\nExpected return:     {expected_return:.4%}")
print(f"Daily-linked TWR:    {actual_return:.4%}")

print(f"\nHistory status:      {mtd['history_status']}")
print(f"Valuation status:    {mtd['valuation_status']}")
print(f"Effective start:     {mtd['effective_start_date']}")
print(f"As of date:          {mtd['as_of_date']}")
print(f"Complete days:       {mtd['complete_days']}")


# --------------------------------------------------
# 4. Automated assertions
# --------------------------------------------------

assert abs(actual_return - expected_return) < 1e-8, (
    "Daily TWR does not reconcile to holding-period return."
)

assert mtd["valuation_status"] == "complete"
assert mtd["history_status"] == "partial"
assert mtd["effective_start_date"] == date(2026, 5, 12)
assert mtd["complete_days"] == 3
assert mtd["missing_valuation_days"] == 0

print("\nPASS — DRAM daily TWR reconciles.")
print("PASS — Partial-history classification.")
print("PASS — Valuation coverage.")



# --------------------------------------------------
# 5. TSM — Dividend and withholding validation
# --------------------------------------------------

tsm_query = """
SELECT
    instrument,
    date,
    previous_valuation_date,
    beginning_value,
    ending_value,
    purchases,
    sales,
    gross_dividend_paid,
    adr_fee_cash,
    withholding_tax_cash,
    gross_daily_return,
    net_fee_daily_return,
    after_withholding_daily_return,
    valuation_status
FROM mart.security_daily_twr_v
WHERE instrument = 'TSM'
  AND date <= DATE '2026-04-09'
ORDER BY date;
"""

tsm_daily = query_dataframe(tsm_query, ())

if tsm_daily.empty:
    raise ValueError("No TSM observations returned.")

tsm_as_of = date(2026, 4, 9)

return_columns = {
    "gross": "gross_daily_return",
    "net_fee": "net_fee_daily_return",
    "after_withholding": "after_withholding_daily_return",
}

tsm_results = {}

for basis, column in return_columns.items():
    tsm_results[basis] = (
        calculate_security_daily_return_details(
            daily_returns=tsm_daily,
            inception_date=date(2020, 7, 6),
            as_of_date=tsm_as_of,
            return_column=column,
        )["MTD"]
    )

# Check the dividend settlement day independently.
settlement = tsm_daily.loc[
    pd.to_datetime(tsm_daily["date"]).dt.date
    == tsm_as_of
].iloc[0]

assert abs(float(settlement["gross_dividend_paid"]) - 15.02) < 1e-8
assert abs(float(settlement["withholding_tax_cash"]) + 3.15) < 1e-8
assert float(settlement["adr_fee_cash"]) == 0

assert round(
    float(settlement["gross_daily_return"]) * 100, 4
) == -0.1118

assert round(
    float(settlement["after_withholding_daily_return"]) * 100, 4
) == -0.1654

gross = float(tsm_results["gross"]["return"])
net_fee = float(tsm_results["net_fee"]["return"])
after_withholding = float(
    tsm_results["after_withholding"]["return"]
)

# April contains no TSM ADR fees, so these should agree.
assert abs(gross - net_fee) < 1e-10

# Foreign withholding should reduce investor performance.
assert after_withholding < gross

for result in tsm_results.values():
    assert result["valuation_status"] == "complete"

print("\nTSM — April 2026 MTD through April 9")
print("-" * 45)

print(f"Gross return:             {gross:.4%}")
print(f"Net-of-fees return:       {net_fee:.4%}")
print(f"After-withholding return: {after_withholding:.4%}")

print(f"\nHistory status:           {tsm_results['gross']['history_status']}")
print(f"Valuation status:         {tsm_results['gross']['valuation_status']}")
print(f"As of date:              {tsm_results['gross']['as_of_date']}")

print("\nPASS — TSM dividend settlement.")
print("PASS — Gross and net-of-fees consistency.")
print("PASS — Foreign withholding reduces return.")
print("PASS — Daily security TWR reporting coverage.")



# --------------------------------------------------
# 6. All current holdings — Full reporting validation
# --------------------------------------------------


print("\nCURRENT HOLDINGS — FULL REPORTING VALIDATION")
print("-" * 65)

# Use the latest portfolio NAV date as our common as-of date.
# Select current equities dynamically, excluding closed positions.

holdings_query = """
WITH reporting_date AS (
    SELECT MAX(date)::date AS as_of_date
    FROM core.portfolio_nav_daily_v
),

current_holdings AS (
    SELECT
        s.instrument,
        s.qty_raw AS current_shares,
        s.security_value AS current_value
    FROM core.security_value_daily_v s
    JOIN reporting_date r
      ON s.date = r.as_of_date
    WHERE s.qty_raw > 0
)

SELECT
    t.instrument,
    t.date,
    t.previous_valuation_date,
    t.beginning_value,
    t.ending_value,
    t.purchases,
    t.sales,
    t.gross_dividend_paid,
    t.adr_fee_cash,
    t.withholding_tax_cash,
    t.gross_daily_return,
    t.net_fee_daily_return,
    t.after_withholding_daily_return,
    t.valuation_status,
    h.current_shares,
    h.current_value,
    r.as_of_date

FROM mart.security_daily_twr_v t
JOIN current_holdings h
  ON t.instrument = h.instrument
CROSS JOIN reporting_date r

WHERE t.date <= r.as_of_date

ORDER BY h.current_value DESC, t.instrument, t.date;
"""

all_daily = query_dataframe(holdings_query, ())

if all_daily.empty:
    raise ValueError("No current equity holdings found.")

as_of = pd.to_datetime(
    all_daily["as_of_date"].iloc[0]
).date()

holdings = (
    all_daily.groupby("instrument", as_index=False)
    .agg(current_value=("current_value", "first"))
    .sort_values("current_value", ascending=False)
)

print(f"As of date: {as_of}")
print(f"Current holdings: {len(holdings)}")
print(f"Daily observations: {len(all_daily)}")


# --------------------------------------------------
# 7. Validate valuation-calendar completeness
# --------------------------------------------------

calendar_query = """
SELECT date
FROM core.portfolio_nav_daily_v
WHERE date <= (
    SELECT MAX(date)
    FROM core.portfolio_nav_daily_v
)
ORDER BY date;
"""

calendar = query_dataframe(calendar_query, ())

expected_dates = set(
    pd.to_datetime(calendar["date"]).dt.date
)

for instrument in holdings["instrument"]:
    security = all_daily.loc[
        all_daily["instrument"] == instrument
    ]

    actual_dates = set(
        pd.to_datetime(security["date"]).dt.date
    )

    assert actual_dates == expected_dates, (
        f"{instrument}: Missing or unexpected valuation dates."
    )

print("PASS — Current holdings have complete date coverage.")


# --------------------------------------------------
# 8. Validate all horizons and return bases
# --------------------------------------------------

horizons = (
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
)

bases = {
    "gross": "gross_daily_return",
    "net_fee": "net_fee_daily_return",
    "after_withholding": "after_withholding_daily_return",
}

report_rows = []
validation_count = 0

for instrument in holdings["instrument"]:

    security = all_daily.loc[
        all_daily["instrument"] == instrument
    ].copy()

    basis_results = {}

    for basis, return_column in bases.items():

        results = calculate_security_daily_return_details(
            daily_returns=security,
            inception_date=date(2020, 7, 6),
            as_of_date=as_of,
            return_column=return_column,
        )

        assert set(results) == set(horizons)

        basis_results[basis] = results

        for horizon in horizons:

            result = results[horizon]
            cumulative = result["return"]
            status = result["valuation_status"]

            assert result["as_of_date"] == as_of

            if status == "complete":
                assert cumulative is not None, (
                    f"{instrument} {horizon} {basis}: "
                    "Missing return despite complete status."
                )
                assert math.isfinite(float(cumulative))
            else:
                assert cumulative is None, (
                    f"{instrument} {horizon} {basis}: "
                    "Invalid return should be NULL."
                )

            validation_count += 1

    # Return-basis selection must not change history
    # classification or valuation coverage.

    for horizon in horizons:

        gross = basis_results["gross"][horizon]
        net = basis_results["net_fee"][horizon]
        after = basis_results["after_withholding"][horizon]

        assert (
            gross["history_status"]
            == net["history_status"]
            == after["history_status"]
        )

        assert (
            gross["valuation_status"]
            == net["valuation_status"]
            == after["valuation_status"]
        )

        value = gross["return"]

        if value is None:
            display = "INC" if (
                gross["valuation_status"] == "incomplete"
            ) else "—"
        else:
            display = f"{float(value):+.2%}"

            if gross["history_status"] in (
                "partial", "partial_intermittent"
            ):
                display += "†"
            elif gross["history_status"] == "intermittent":
                display += "*"

        report_rows.append({
            "instrument": instrument,
            "horizon": horizon,
            "gross_return": display,
            "history_status": gross["history_status"],
            "valuation_status": gross["valuation_status"],
            "effective_start": gross["effective_start_date"],
        })


# --------------------------------------------------
# 9. Display cumulative performance matrix
# --------------------------------------------------

report = pd.DataFrame(report_rows)

matrix = report.pivot(
    index="instrument",
    columns="horizon",
    values="gross_return",
).reindex(
    index=holdings["instrument"],
    columns=list(horizons),
)

print("\nGROSS DAILY TWR — CURRENT HOLDINGS")
print(matrix.to_string())

print("\nCOVERAGE STATUS COUNTS")
print(
    report.groupby(
        ["history_status", "valuation_status"]
    ).size().to_string()
)

print(f"\nPASS — {validation_count} return calculations validated.")
print("PASS — All seven horizons and three return bases.")
print("PASS — Consistent history and valuation metadata.")


print("\nINTERMITTENT OWNERSHIP DIAGNOSTIC")
print("-" * 45)

print(
    report.loc[
        report["history_status"].str.contains("intermittent"),
        [
            "instrument",
            "horizon",
            "effective_start",
            "history_status",
            "valuation_status",
        ],
    ].to_string(index=False)
)


from investment_analytics.performance.security_reporting import (
    calculate_security_monthly_return_details,
)

monthly_query = """
SELECT
    instrument,
    period_start,
    nav_date,
    value_bom,
    value_eom,
    trade_cash,
    dividend_cash,
    adr_fee_cash,
    withholding_tax_cash,
    gross_avg_capital,
    gross_md_return,
    net_fee_md_return,
    after_withholding_md_return
FROM mart.security_modified_dietz_accrual_v
WHERE instrument = 'TOST'
ORDER BY period_start;
"""

tost_monthly = query_dataframe(monthly_query, ())

md_results = calculate_security_monthly_return_details(
    monthly_returns=tost_monthly,
    inception_date=date(2020, 7, 6),
    as_of_date=date(2026, 10, 20),
    last_completed_month=date(2026, 9, 1),
    return_column="gross_md_return",
)

print("\nTOST — MODIFIED DIETZ REPORTING")
print("-" * 55)

for horizon in ("YTD", "T12M", "T36M", "T60M"):
    result = md_results[horizon]

    value = result["return"]
    display = (
        f"{float(value):+.4%}"
        if value is not None else "N/A"
    )

    print(
        f"{horizon:5} | {display:>12} | "
        f"{result['history_status']:>21} | "
        f"As of {result['as_of_date']}"
    )

assert md_results["T60M"]["history_status"] == (
    "partial_intermittent"
)
assert md_results["T60M"]["valuation_status"] == "complete"
assert md_results["T60M"]["as_of_date"] == date(2026, 9, 30)

print("\nPASS — TOST monthly Modified Dietz reporting.")



# --------------------------------------------------
# 10. All current holdings — Modified Dietz validation
# --------------------------------------------------

print("\nCURRENT HOLDINGS — MODIFIED DIETZ VALIDATION")
print("-" * 65)

monthly_query = """
WITH reporting_date AS (
    SELECT MAX(date)::date AS as_of_date
    FROM core.portfolio_nav_daily_v
),

current_holdings AS (
    SELECT s.instrument
    FROM core.security_value_daily_v s
    JOIN reporting_date r
      ON s.date = r.as_of_date
    WHERE s.qty_raw > 0
)

SELECT
    m.instrument,
    m.period_start,
    m.nav_date,
    m.value_bom,
    m.value_eom,
    m.trade_cash,
    m.dividend_cash,
    m.adr_fee_cash,
    m.withholding_tax_cash,

    m.gross_avg_capital,
    m.net_fee_avg_capital,
    m.after_withholding_avg_capital,

    m.gross_md_return,
    m.net_fee_md_return,
    m.after_withholding_md_return

FROM mart.security_modified_dietz_accrual_v m
JOIN current_holdings h
  ON h.instrument = m.instrument

ORDER BY m.instrument, m.period_start;
"""

all_monthly = query_dataframe(monthly_query, ())

if all_monthly.empty:
    raise ValueError("No monthly security observations returned.")

md_bases = {
    "gross": "gross_md_return",
    "net_fee": "net_fee_md_return",
    "after_withholding": "after_withholding_md_return",
}

md_horizons = (
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
)

md_rows = []
md_validation_count = 0

for instrument in holdings["instrument"]:

    security = all_monthly.loc[
        all_monthly["instrument"] == instrument
    ].copy()

    if security.empty:
        raise ValueError(
            f"{instrument}: Missing monthly history."
        )

    basis_results = {}

    for basis, return_column in md_bases.items():

        results = calculate_security_monthly_return_details(
            monthly_returns=security,
            inception_date=date(2020, 7, 6),
            as_of_date=as_of,
            last_completed_month=date(2026, 9, 1),
            return_column=return_column,
        )

        assert set(results) == set(md_horizons)

        basis_results[basis] = results

        for horizon, result in results.items():

            value = result["return"]
            status = result["valuation_status"]

            if status == "complete":
                assert value is not None
                assert math.isfinite(float(value))
            else:
                assert value is None

            expected_as_of = (
                date(2026, 9, 30)
                if horizon.startswith("T")
                else as_of
            )

            assert result["as_of_date"] == expected_as_of, (
                f"{instrument} {horizon}: "
                "Incorrect reporting endpoint."
            )

            md_validation_count += 1

    for horizon in md_horizons:

        gross = basis_results["gross"][horizon]
        net = basis_results["net_fee"][horizon]
        after = basis_results["after_withholding"][horizon]

        # History classification is independent
        # of the chosen return convention.
        assert (
            gross["history_status"]
            == net["history_status"]
            == after["history_status"]
        )

        value = gross["return"]

        if value is None:
            display = (
                "INC"
                if gross["valuation_status"] == "incomplete"
                else "—"
            )
        else:
            display = f"{float(value):+.2%}"

            if gross["is_partial_period"]:
                display += "†"
            elif gross["has_ownership_gap"]:
                display += "*"

        md_rows.append({
            "instrument": instrument,
            "horizon": horizon,
            "gross_return": display,
            "history_status": gross["history_status"],
            "gross_status": gross["valuation_status"],
            "net_fee_status": net["valuation_status"],
            "after_withholding_status": after["valuation_status"],
        })


md_report = pd.DataFrame(md_rows)

md_matrix = md_report.pivot(
    index="instrument",
    columns="horizon",
    values="gross_return",
).reindex(
    index=holdings["instrument"],
    columns=list(md_horizons),
)

print("\nGROSS MODIFIED DIETZ — CURRENT HOLDINGS")
print(md_matrix.to_string())

print("\nHISTORY COVERAGE")
print(
    md_report.groupby(
        ["history_status", "gross_status"]
    ).size().to_string()
)

status_differences = md_report.loc[
    (md_report["gross_status"] != md_report["net_fee_status"])
    | (
        md_report["gross_status"]
        != md_report["after_withholding_status"]
    )
]

print("\nRETURN-BASIS COVERAGE DIFFERENCES")
print(
    "None"
    if status_differences.empty
    else status_differences.to_string(index=False)
)

print(
    f"\nPASS — {md_validation_count} Modified Dietz "
    "return calculations validated."
)
print("PASS — All seven horizons and three return bases.")
print("PASS — Reporting endpoints and history classifications.")



security_report = build_security_performance_report(
    daily_returns=all_daily,
    monthly_returns=all_monthly,
    current_holdings=holdings,
    inception_date=date(2020, 7, 6),
    as_of_date=as_of,
    last_completed_month=date(2026, 9, 1),
)

assert len(security_report) == 378

assert not security_report.duplicated(
    ["asset", "methodology", "return_basis", "horizon"]
).any()

print("\nUNIFIED SECURITY REPORT")
print("-" * 45)
print(f"Records: {len(security_report)}")

print(
    security_report.groupby(
        ["methodology", "return_basis"]
    ).size().to_string()
)

print("\nANNUALIZATION ELIGIBILITY")

print(
    security_report.groupby(
        ["history_status", "annualized_return"],
        dropna=False,
    ).size().groupby(level=0).sum().to_string()
)

print("\nPASS — Security report dataset constructed.")

# Validate annualization eligibility.

trailing = security_report["horizon"].isin(
    ["T12M", "T24M", "T36M", "T60M"]
)

eligible = (
    trailing
    & security_report["history_status"].eq("full")
    & security_report["valuation_status"].eq("complete")
    & security_report["cumulative_return"].notna()
)

assert security_report.loc[
    eligible, "annualized_return"
].notna().all()

assert security_report.loc[
    ~eligible, "annualized_return"
].isna().all()

print("\nANNUALIZATION VALIDATION")
print(f"Eligible records: {eligible.sum()}")
print(
    "Populated annualized returns:",
    security_report["annualized_return"].notna().sum()
)
print("PASS — Annualization eligibility validated.")