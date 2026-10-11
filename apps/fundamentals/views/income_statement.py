import math
import streamlit as st

from investment_analytics.data_access.fundamentals import (
    get_income_statement_instruments,
    get_income_statements,
)

from investment_analytics.fundamentals.income_statement import (
    build_income_statement_summary,
    calculate_income_statement_metrics,
    calculate_ttm_metrics,
)

from charts.income_statement import (
    build_revenue_growth_chart,
)

def _format_percent(value: float | None) -> str:
    """Format a percentage while handling missing values."""

    if value is None or not math.isfinite(value):
        return "—"

    return f"{value:.1%}"


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

    quarterly_metrics = calculate_income_statement_metrics(
        quarterly
    )

    ttm_metrics = calculate_ttm_metrics(
        quarterly
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
        _format_percent(long_term["revenue_cagr_5y"]),
    )

    col3.metric(
        "3Y Operating Income CAGR",
        _format_percent(long_term["operating_income_cagr_3y"]),
    )

    st.subheader("Revenue & Growth")
    revenue_frequency = st.segmented_control(
        "Period",
        options=["Quarterly", "Annual" ],
        default="Quarterly",
        label_visibility="collapsed",
    )
    if revenue_frequency == "Quarterly":
        revenue_chart_data = quarterly_metrics
        chart_frequency = "quarterly"
        chart_periods = 12
    else:
        revenue_chart_data = annual_metrics
        chart_frequency = "annual"
        chart_periods = 10

    revenue_growth_chart = build_revenue_growth_chart(
        revenue_chart_data,
        currency=currency,
        instrument=instrument,
        frequency=chart_frequency,
        periods=chart_periods,
        ttm_df=ttm_metrics if chart_frequency == "quarterly" else None,
    )

    st.plotly_chart(
        revenue_growth_chart,
        use_container_width=True,
        config={
            "displayModeBar": False,
        },
    )