
"""
PostgreSQL data access for portfolio performance reporting.

Retrieves monthly Modified Dietz observations from the
investment analytics mart for downstream Python calculations.
"""

import pandas as pd

from investment_analytics.data_access.database import query_dataframe

def get_portfolio_monthly_returns() -> pd.DataFrame:
    """
    Retrieve monthly portfolio Modified Dietz returns.

    Returns one observation per reporting month, including
    the current MTD period when available.

    Returns remain in decimal form:
        0.05 represents +5%, not 0.05%.

    Numeric values are preserved without converting to float.
    """

    query = """
        SELECT
            period_start,
            period_end,
            nav_date,
            nav_bom,
            nav_eom,
            external_flow,
            weighted_external_flow,
            period_pnl,
            avg_capital,
            md_return,
            period_status,
            valuation_status

        FROM mart.portfolio_modified_dietz_mv

        ORDER BY period_start;
    """

    return query_dataframe(query)



def get_portfolio_daily_returns() -> pd.DataFrame:
    """
    Retrieve daily-valued portfolio TWR observations.

    Returns one observation per valuation date, including
    the inception date with a NULL daily return.

    Returns remain in decimal form:
        0.05 represents +5%, not 0.05%.
    """

    query = """
        SELECT
            nav_date,
            previous_nav_date,
            beginning_nav,
            ending_nav,
            external_flow,
            daily_return,
            valuation_status

        FROM mart.portfolio_daily_twr_mv

        ORDER BY nav_date;
    """

    return query_dataframe(query)

