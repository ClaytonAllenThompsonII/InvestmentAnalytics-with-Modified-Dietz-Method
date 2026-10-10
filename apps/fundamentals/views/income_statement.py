import streamlit as st

from investment_analytics.data_access.fundamentals import (
    get_income_statement_instruments,
    get_income_statements,
)

from investment_analytics.fundamentals.income_statement import (
    build_income_statement_summary,
    calculate_income_statement_metrics,
)

from charts.income_statement import (
    build_revenue_growth_chart,
)


def _format_amount(value: float) -> str:
    """Format a financial amount using an appropriate magnitude."""

    absolute_value = abs(value)

    if absolute_value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"

    if absolute_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"

    if absolute_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"

    return f"{value:,.0f}"


def render_income_statement_page():
    st.header("Income Statement")

    instruments = get_income_statement_instruments()

    instrument = st.selectbox(
        "Company",
        instruments,
    )

    annual = get_income_statements(
        instrument,
        "annual",
    )

    quarterly = get_income_statements(
        instrument,
        "quarterly",
    )

    if annual.empty or quarterly.empty:
        st.warning(
            "Insufficient income-statement data for this company."
        )
        return

    summary = build_income_statement_summary(
        annual,
        quarterly,
    )

    currency = summary["reported_currency"]
    latest = summary["latest_quarter"]
    ttm = summary["ttm"]
    long_term = summary["long_term"]

    annual_metrics = calculate_income_statement_metrics(
        annual
    )

    revenue_growth_chart = build_revenue_growth_chart(
        annual_metrics,
        currency=currency,
    )


    st.caption(
        f"{instrument} · Reporting currency: {currency} · "
        f"Latest quarter: {latest['fiscal_date']}"
    )

    st.subheader("Latest Quarter")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Revenue",
        f"{currency} {_format_amount(latest['revenue'])}",
    )

    col2.metric(
        "YoY Revenue Growth",
        f"{latest['revenue_growth']:.1%}",
    )

    col3.metric(
        "Operating Margin",
        f"{latest['operating_margin']:.1%}",
    )

    col4.metric(
        "Net Margin",
        f"{latest['net_margin']:.1%}",
    )

    st.subheader("Trailing Twelve Months")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Revenue",
        f"{currency} {_format_amount(ttm['revenue'])}",
    )

    col2.metric(
        "YoY Revenue Growth",
        f"{ttm['revenue_growth']:.1%}",
    )

    col3.metric(
        "Operating Margin",
        f"{ttm['operating_margin']:.1%}",
    )

    col4.metric(
        "Net Margin",
        f"{ttm['net_margin']:.1%}",
    )

    st.subheader("Long-Term Growth")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "3Y Revenue CAGR",
        f"{long_term['revenue_cagr_3y']:.1%}",
    )

    col2.metric(
        "5Y Revenue CAGR",
        f"{long_term['revenue_cagr_5y']:.1%}",
    )

    col3.metric(
        "3Y Operating Income CAGR",
        f"{long_term['operating_income_cagr_3y']:.1%}",
    )

    st.plotly_chart(
            revenue_growth_chart,
            use_container_width=True,
        )