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

    frequency = frequencies[0]

    growth_periods = 1 if frequency == "annual" else 4

    result["revenue_growth"] = result["total_revenue"].pct_change(
        periods=growth_periods,
        fill_method=None,
    )

    result["net_income_growth"] = result["net_income"].pct_change(
        periods=growth_periods,
        fill_method=None,
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