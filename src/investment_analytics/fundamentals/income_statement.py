"""
Income-statement analytics for the Investment Analytics Platform.

This module contains reusable calculations for company income statements.
It does not access PostgreSQL directly and does not contain application or
visualization logic.
"""

import pandas as pd


INCOME_STATEMENT_NUMERIC_COLUMNS = [
    "total_revenue",
    "cost_of_revenue",
    "cost_of_goods_and_services_sold",
    "gross_profit",
    "operating_income",
    "operating_expenses",
    "research_and_development",
    "selling_general_and_administrative",
    "depreciation",
    "depreciation_and_amortization",
    "ebit",
    "ebitda",
    "income_before_tax",
    "income_tax_expense",
    "net_income",
    "net_income_from_continuing_operations",
    "comprehensive_income_net_of_tax",
    "interest_expense",
    "interest_income",
    "interest_and_debt_expense",
    "net_interest_income",
    "investment_income_net",
    "non_interest_income",
    "other_non_operating_income",
]


def prepare_income_statement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare income-statement data for analytical calculations.

    Converts fiscal dates to datetime, converts financial fields to numeric
    values, and sorts observations chronologically.
    """

    result = df.copy()

    result["fiscal_date"] = pd.to_datetime(result["fiscal_date"])

    for column in INCOME_STATEMENT_NUMERIC_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(
                result[column],
                errors="coerce",
            )

    return result.sort_values("fiscal_date").reset_index(drop=True)

def _calculate_yoy_growth(
    df: pd.DataFrame,
    value_column: str,
    tolerance_days: int = 31,
) -> pd.Series:
    """
    Calculate year-over-year growth by matching each fiscal period
    to the corresponding period approximately one year earlier.

    A date tolerance allows for fiscal calendars whose reporting dates
    shift slightly from year to year.
    """

    history = (
        df.set_index("fiscal_date")[value_column]
        .sort_index()
    )

    prior_year_dates = (
        pd.DatetimeIndex(df["fiscal_date"])
        - pd.DateOffset(years=1)
    )

    prior_values = history.reindex(
        prior_year_dates,
        method="nearest",
        tolerance=pd.Timedelta(days=tolerance_days),
    )

    prior_values = pd.Series(
        prior_values.to_numpy(),
        index=df.index,
    )

    prior_values = prior_values.where(prior_values != 0)

    return (
        df[value_column]
        .div(prior_values)
        .sub(1)
    )

def _calculate_ttm_sum(
    df: pd.DataFrame,
    value_column: str,
    tolerance_days: int = 31,
) -> pd.Series:
    """
    Calculate trailing-twelve-month sums from four consecutive fiscal quarters.

    Each observation must have corresponding fiscal periods approximately
    3, 6, and 9 months earlier. If any required quarter is missing, the
    TTM value is unavailable.
    """

    history = (
        df.set_index("fiscal_date")[value_column]
        .sort_index()
    )

    ttm_values = []

    for fiscal_date in df["fiscal_date"]:
        expected_dates = [
            fiscal_date,
            fiscal_date - pd.DateOffset(months=3),
            fiscal_date - pd.DateOffset(months=6),
            fiscal_date - pd.DateOffset(months=9),
        ]

        values = []

        for expected_date in expected_dates:
            matched = history.reindex(
                pd.DatetimeIndex([expected_date]),
                method="nearest",
                tolerance=pd.Timedelta(days=tolerance_days),
            ).iloc[0]

            values.append(matched)

        if pd.isna(values).any():
            ttm_values.append(float("nan"))
        else:
            ttm_values.append(sum(values))

    return pd.Series(
        ttm_values,
        index=df.index,
        dtype="float64",
    )

def calculate_income_statement_metrics(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate reusable growth and profitability metrics.

    The input should contain only one reporting frequency.
    """

    result = prepare_income_statement(df)

    frequencies = result["frequency"].dropna().unique()

    if len(frequencies) != 1:
        raise ValueError(
            "Income-statement metrics require exactly one reporting frequency."
        )

    result["revenue_growth"] = _calculate_yoy_growth(
    result,
    "total_revenue",
    )

    result["net_income_growth"] = _calculate_yoy_growth(
        result,
        "net_income",
    )

    result["gross_margin"] = (
        result["gross_profit"] / result["total_revenue"]
    )

    result["operating_margin"] = (
        result["operating_income"] / result["total_revenue"]
    )

    result["ebitda_margin"] = (
        result["ebitda"] / result["total_revenue"]
    )

    result["net_margin"] = (
        result["net_income"] / result["total_revenue"]
    )

    result["rd_pct_revenue"] = (
        result["research_and_development"] / result["total_revenue"]
    )

    result["sga_pct_revenue"] = (
        result["selling_general_and_administrative"]
        / result["total_revenue"]
    )

    return result

def calculate_ttm_metrics(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate trailing-twelve-month income-statement metrics.

    The input must contain quarterly observations for one instrument.
    TTM values are calculated as rolling sums of the latest four quarters.
    """

    result = prepare_income_statement(df)

    frequencies = result["frequency"].dropna().unique()

    if len(frequencies) != 1 or frequencies[0] != "quarterly":
        raise ValueError(
            "TTM metrics require quarterly income-statement data."
        )

    ttm_columns = [
        "total_revenue",
        "gross_profit",
        "operating_income",
        "ebitda",
        "net_income",
        "research_and_development",
        "selling_general_and_administrative",
    ]

    for column in ttm_columns:
        result[f"{column}_ttm"] = _calculate_ttm_sum(
            result,
            column,
        )

    result["gross_margin_ttm"] = (
        result["gross_profit_ttm"]
        / result["total_revenue_ttm"]
    )

    result["operating_margin_ttm"] = (
        result["operating_income_ttm"]
        / result["total_revenue_ttm"]
    )

    result["ebitda_margin_ttm"] = (
        result["ebitda_ttm"]
        / result["total_revenue_ttm"]
    )

    result["net_margin_ttm"] = (
        result["net_income_ttm"]
        / result["total_revenue_ttm"]
    )

    result["revenue_growth_ttm"] = _calculate_yoy_growth(
        result,
        "total_revenue_ttm",
    )

    return result