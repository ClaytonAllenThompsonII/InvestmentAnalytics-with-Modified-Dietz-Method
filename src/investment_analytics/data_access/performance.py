
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


BENCHMARK_SYMBOLS = (
    "SPY",
    "QQQ",
    "XLK",
    "XLY",
    "EEM",
    "IXUS",
)


def get_benchmark_daily_prices() -> pd.DataFrame:
    """
    Retrieve adjusted daily closing prices for
    portfolio performance benchmarks.
    """

    sql = """
        SELECT
            instrument,
            date AS price_date,
            adj_close AS adjusted_close
        FROM core.daily_prices_v
        WHERE instrument = ANY(%s)
        ORDER BY instrument, date
    """

    return query_dataframe(
        sql,
        (list(BENCHMARK_SYMBOLS),),
    )


def get_security_daily_returns() -> pd.DataFrame:
    """Retrieve daily TWR observations for current holdings."""

    query = """
        WITH current_holdings AS (
            SELECT DISTINCT instrument
            FROM core.security_value_daily_v
            WHERE date = (
                SELECT MAX(date)
                FROM core.portfolio_nav_daily_v
            )
            AND qty_raw > 0
        )

        SELECT
            d.instrument,
            d.date,
            d.previous_valuation_date,
            d.beginning_value,
            d.ending_value,
            d.purchases,
            d.sales,
            d.gross_dividend_paid,
            d.adr_fee_cash,
            d.withholding_tax_cash,
            d.gross_daily_return,
            d.net_fee_daily_return,
            d.after_withholding_daily_return,
            d.valuation_status

        FROM mart.security_daily_twr_mv d
        JOIN current_holdings h
          ON d.instrument = h.instrument

        ORDER BY d.instrument, d.date;
    """

    return query_dataframe(query)

def get_security_monthly_returns() -> pd.DataFrame:
    """Retrieve monthly Modified Dietz observations for current holdings."""

    query = """
        WITH current_holdings AS (
            SELECT DISTINCT instrument
            FROM core.security_value_daily_v
            WHERE date = (
                SELECT MAX(date)
                FROM core.portfolio_nav_daily_v
            )
            AND qty_raw > 0
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

        FROM mart.security_modified_dietz_accrual_mv m
        JOIN current_holdings h
          ON m.instrument = h.instrument

        ORDER BY m.instrument, m.period_start;
    """

    return query_dataframe(query)



def get_current_holdings() -> pd.DataFrame:
    """
    Retrieve current positive-share equity holdings,
    ranked by total economic security value.

    Includes accrued dividend receivables.
    """

    query = """
        WITH reporting_date AS (
            SELECT MAX(date) AS as_of_date
            FROM core.portfolio_nav_daily_v
        )

        SELECT
            s.instrument,
            s.security_value AS current_value

        FROM core.security_value_daily_v s

        JOIN reporting_date r
          ON s.date = r.as_of_date

        WHERE s.qty_raw > 0

        ORDER BY current_value DESC;
    """

    holdings = query_dataframe(query)

    if holdings.empty:
        raise ValueError("No current equity holdings found.")

    if holdings["current_value"].isna().any():
        raise ValueError(
            "Current holdings contain missing valuations."
        )

    return holdings
