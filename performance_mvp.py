
"""Portfolio Performance — Streamlit MVP."""

from datetime import date

import pandas as pd
import streamlit as st

from investment_analytics.data_access.performance import (
    get_portfolio_daily_returns,
    get_portfolio_monthly_returns,
    get_benchmark_daily_prices,
)
from investment_analytics.performance.reporting import (
    build_portfolio_performance_report,
)


INCEPTION_DATE = date(2020, 7, 6)

HORIZONS = [
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
]

HORIZON_LABELS = {
    "T12M": "TTM",
    "T24M": "2Y",
    "T36M": "3Y",
    "T60M": "5Y",
}

TRAILING_HORIZONS = HORIZONS[3:]

BENCHMARKS = ["SPY", "QQQ", "XLK", "XLY", "EEM", "IXUS"]


@st.cache_data(ttl=300)
def load_report() -> pd.DataFrame:
    """Load portfolio performance from materialized PostgreSQL marts."""
    return build_portfolio_performance_report(
        get_portfolio_monthly_returns(),
        get_portfolio_daily_returns(),
        INCEPTION_DATE,
        get_benchmark_daily_prices(),
    )


def build_matrix(
    report: pd.DataFrame,
    methodology: str,
    metric: str,
    horizons: list[str],
) -> pd.DataFrame:
    """Build a portfolio-first performance matrix."""

    selected = (
        report.loc[report["methodology"] == methodology]
        .set_index(["asset", "horizon"])
    )

    assets = ["Portfolio", *BENCHMARKS]

    rows = []

    for asset in assets:
        row = {"Asset": asset}

        for horizon in horizons:
            observation = selected.loc[(asset, horizon)]
            value = observation[metric]

            if pd.isna(value):
                row[horizon] = "—"
            else:
                suffix = (
                    " †"
                    if observation["valuation_status"] == "partial"
                    else ""
                )

                row[horizon] = f"{value:+.2%}{suffix}"

        rows.append(row)

    return (
        pd.DataFrame(rows)
        .set_index("Asset")
        .rename(columns=HORIZON_LABELS)
    )


def style_performance_matrix(matrix: pd.DataFrame):
    """Style returns and highlight the portfolio row."""

    def color_return(value):
        if isinstance(value, str):
            if value.startswith("+"):
                return "color: #16a34a"
            if value.startswith("-"):
                return "color: #dc2626"
        return ""

    def highlight_portfolio(row):
        if row.name == "Portfolio":
            return [
                "background-color: rgba(100, 116, 139, 0.15); "
                "font-weight: bold"
            ] * len(row)

        return [""] * len(row)

    return (
        matrix.style
        .map(color_return)
        .apply(highlight_portfolio, axis=1)
    )



st.set_page_config(
    page_title="Portfolio Performance",
    layout="wide",
)

st.title("Portfolio Performance")

method_label = st.radio(
    "Performance methodology",
    ["Daily TWR", "Modified Dietz"],
    horizontal=True,
)

methodology = {
    "Daily TWR": "daily_twr",
    "Modified Dietz": "modified_dietz",
}[method_label]

report = load_report()

selected = report.loc[
    report["methodology"] == methodology
]

latest_date = selected["as_of_date"].max()

st.caption(f"Latest available NAV: {latest_date}")

st.subheader("Cumulative Returns")

st.dataframe(
    style_performance_matrix(
        build_matrix(
            report,
            methodology,
            "cumulative_return",
            HORIZONS,
        )
    ),
    width="stretch",
)

st.subheader("Annualized Returns")

st.dataframe(
    style_performance_matrix(
        build_matrix(
            report,
            methodology,
            "annualized_return",
            TRAILING_HORIZONS,
        )
    ),
    width="stretch",
)

st.caption(
    "† Partial historical valuation coverage. "
    "Benchmark returns reflect dividend-adjusted closing prices."
)

with st.expander("Performance methodology"):
    st.write(
        "**Daily TWR:** Geometrically linked daily portfolio "
        "returns, adjusted for external cash flows using a "
        "beginning-of-day flow convention. Trailing periods "
        "end on the latest available NAV date."
    )

    st.write(
        "**Modified Dietz:** Geometrically linked monthly "
        "returns using time-weighted external cash flows. "
        "MTD, QTD, and YTD include the current partial month. "
        "Trailing periods use the latest completed calendar month."
    )

    st.write(
        "Annualized returns are displayed only for trailing "
        "12-, 24-, 36-, and 60-month periods. "
        "Historical valuation limitations are flagged."
    )
