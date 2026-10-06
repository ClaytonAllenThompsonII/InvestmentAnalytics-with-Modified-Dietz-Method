"""
Data-access functions for company fundamental data.

This module provides read-only access to fundamental financial data stored
in PostgreSQL. Analytical calculations belong in
investment_analytics.fundamentals, not here.
"""

from typing import Literal

import pandas as pd

from investment_analytics.data_access.database import get_connection


Frequency = Literal["annual", "quarterly"]


def get_income_statements(
    instrument: str,
    frequency: Frequency | None = None,
) -> pd.DataFrame:
    """
    Return income-statement history for one instrument.

    Results are ordered chronologically by fiscal date.
    """

    query = """
        SELECT
            instrument,
            fiscal_date,
            frequency,
            reported_currency,
            total_revenue,
            cost_of_revenue,
            cost_of_goods_and_services_sold,
            gross_profit,
            operating_income,
            operating_expenses,
            research_and_development,
            selling_general_and_administrative,
            depreciation,
            depreciation_and_amortization,
            ebit,
            ebitda,
            income_before_tax,
            income_tax_expense,
            net_income,
            net_income_from_continuing_operations,
            comprehensive_income_net_of_tax,
            interest_expense,
            interest_income,
            interest_and_debt_expense,
            net_interest_income,
            investment_income_net,
            non_interest_income,
            other_non_operating_income
        FROM source.income_statements
        WHERE instrument = %s
    """

    params = [instrument]

    if frequency is not None:
        query += """
            AND frequency = %s
        """
        params.append(frequency)

    query += """
        ORDER BY fiscal_date;
    """

    with get_connection() as conn:
        return pd.read_sql_query(
            query,
            conn,
            params=params,
        )


def get_income_statement_instruments() -> list[str]:
    """
    Return instruments currently available in the income-statement store.
    """

    query = """
        SELECT DISTINCT instrument
        FROM source.income_statements
        ORDER BY instrument;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]


def get_latest_income_statement(
    instrument: str,
    frequency: Frequency = "quarterly",
) -> pd.DataFrame:
    """
    Return the latest available income statement for an instrument.
    """

    query = """
        SELECT *
        FROM source.income_statements
        WHERE instrument = %s
          AND frequency = %s
        ORDER BY fiscal_date DESC
        LIMIT 1;
    """

    with get_connection() as conn:
        return pd.read_sql_query(
            query,
            conn,
            params=[instrument, frequency],
        )