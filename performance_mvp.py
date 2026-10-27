
"""Performance Detail — Streamlit dashboard."""

from datetime import date

import pandas as pd
import streamlit as st

from investment_analytics.data_access.performance import (
    get_portfolio_daily_returns,
    get_portfolio_monthly_returns,
    get_benchmark_daily_prices,
    get_security_daily_returns,
    get_security_monthly_returns,
    get_current_holdings,
)
from investment_analytics.performance.reporting import (
    build_unified_performance_report,
)


INCEPTION_DATE = date(2020, 7, 6)

HORIZONS = [
    "MTD", "QTD", "YTD",
    "T12M", "T24M", "T36M", "T60M",
]

TRAILING_HORIZONS = HORIZONS[3:]

HORIZON_LABELS = {
    "T12M": "TTM",
    "T24M": "2Y",
    "T36M": "3Y",
    "T60M": "5Y",
}

BENCHMARKS = ["SPY", "QQQ", "XLK", "XLY", "EEM", "IXUS"]

RETURN_BASES = {
    "Gross": "gross",
    "Net of ADR Fees": "net_fee",
    "After Foreign Withholding": "after_withholding",
}


@st.cache_data(ttl=300)
def load_report() -> pd.DataFrame:
    """Load and assemble unified portfolio performance."""

    portfolio_daily = get_portfolio_daily_returns()
    portfolio_monthly = get_portfolio_monthly_returns()

    if portfolio_daily.empty:
        raise ValueError("No portfolio daily returns available.")

    as_of = pd.to_datetime(
        portfolio_daily["nav_date"]
    ).max().date()

    # Use the previous completed calendar month for
    # Modified Dietz trailing horizons.
    last_completed_month = (
        pd.Timestamp(as_of).replace(day=1)
        - pd.DateOffset(months=1)
    ).date()

    return build_unified_performance_report(
        portfolio_monthly_returns=portfolio_monthly,
        portfolio_daily_returns=portfolio_daily,
        security_monthly_returns=get_security_monthly_returns(),
        security_daily_returns=get_security_daily_returns(),
        current_holdings=get_current_holdings(),
        inception_date=INCEPTION_DATE,
        as_of_date=as_of,
        last_completed_month=last_completed_month,
        benchmark_prices=get_benchmark_daily_prices(),
    )


def build_matrix(
    report: pd.DataFrame,
    methodology: str,
    return_basis: str,
    metric: str,
    horizons: list[str],
) -> pd.DataFrame:
    """Construct holdings-first performance matrix."""

    method_rows = report.loc[
        report["methodology"] == methodology
    ]

    # The selected return basis affects securities only.
    # Portfolio and benchmarks retain standard conventions.
    selected = method_rows.loc[
        (
            method_rows["asset_group"].eq("security")
            & method_rows["return_basis"].eq(return_basis)
        )
        | (
            method_rows["asset_group"].ne("security")
            & method_rows["return_basis"].eq("standard")
        )
    ].copy()

    # Rank current holdings by economic security value.
    securities = (
        selected.loc[
            selected["asset_group"].eq("security"),
            ["asset", "current_value"],
        ]
        .drop_duplicates("asset")
        .sort_values("current_value", ascending=False)
        ["asset"]
        .tolist()
    )

    assets = [*securities, "Portfolio", *BENCHMARKS]
    security_set = set(securities)

    observations = selected.set_index(["asset", "horizon"])

    rows = []

    for asset in assets:
        row = {
            "Asset": (
                "Total Portfolio"
                if asset == "Portfolio"
                else asset
            )
        }

        for horizon in horizons:
            key = (asset, horizon)

            if key not in observations.index:
                raise ValueError(
                    f"Missing performance observation: {key}"
                )

            observation = observations.loc[key]
            value = observation[metric]

            if pd.isna(value):
                row[horizon] = "—"
                continue

            # Only individual securities have ownership-
            # history classification in the unified report.
            partial = (
                asset in security_set
                and (
                    bool(observation["is_partial_period"])
                    or bool(observation["has_ownership_gap"])
                )
            )

            marker = " †" if partial else ""
            row[horizon] = f"{value:+.2%}{marker}"

        rows.append(row)

    return (
        pd.DataFrame(rows)
        .set_index("Asset")
        .rename(columns=HORIZON_LABELS)
    )


def style_performance_matrix(matrix: pd.DataFrame):
    """Color returns and emphasize the portfolio row."""

    def color_return(value):
        if isinstance(value, str):
            if value.startswith("+"):
                return "color: #16a34a"
            if value.startswith("-"):
                return "color: #dc2626"
        return ""

    def highlight_portfolio(row):
        if row.name == "Total Portfolio":
            return [
                "background-color: rgba(100, 116, 139, 0.15); "
                "font-weight: bold"
            ] * len(row)

        return [""] * len(row)

    
    return (
        matrix.style
        .set_properties(
            **{"background-color": "#F8FAFC"}
        )
        .map(color_return)
        .apply(highlight_portfolio, axis=1)
    )



def control_label(text: str):
    st.html(
        f"""
        <div style="
            background-color: #F1F5F9;
            color: #334155;
            padding: 7px 10px;
            border-radius: 6px;
            font-size: 0.85rem;
            font-weight: 600;
        ">
            {text}
        </div>
        """
    )

# --------------------------------------------------
# Streamlit UI
# --------------------------------------------------

st.set_page_config(
    page_title="Performance Detail",
    layout="wide",
)

report = load_report()

latest_date = pd.to_datetime(
    report["as_of_date"]
).max().date()

heading, date_column = st.columns([4, 1])

with heading:
    st.title("Performance Detail")

with date_column:
    st.markdown(
        f"**As of {latest_date:%B %d, %Y}**"
    )


# Compact, left-aligned reporting controls
method_col, basis_col, _ = st.columns(
    [1, 1, 4],
    gap="small",
)


with method_col:
    control_label("Performance methodology")

    method_label = st.segmented_control(
        "Performance methodology",
        ["Daily TWR", "Modified Dietz"],
        default="Daily TWR",
        width="stretch",
        label_visibility="collapsed",
    )

methodology = {
    "Daily TWR": "daily_twr",
    "Modified Dietz": "modified_dietz",
}[method_label or "Daily TWR"]

with basis_col:
    control_label("Individual security return basis")

    basis_label = st.selectbox(
        "Individual security return basis",
        list(RETURN_BASES),
        index=0,
        label_visibility="collapsed",
    )

return_basis = RETURN_BASES[basis_label]


return_basis = RETURN_BASES[basis_label]
# Methodology description
if methodology == "modified_dietz":
    trailing_date = report.loc[
        report["asset"].eq("Portfolio")
        & report["methodology"].eq("modified_dietz")
        & report["horizon"].eq("T12M"),
        "as_of_date",
    ].iloc[0]

    methodology_note = (
        "**Modified Dietz:** Calendar-period returns through "
        f"{latest_date:%B %d, %Y}. "
        "Trailing periods use completed months through "
        f"{pd.Timestamp(trailing_date):%B %d, %Y}."
    )
else:
    methodology_note = (
        "**Daily TWR:** Calendar and trailing returns through "
        f"{latest_date:%B %d, %Y}."
    )

# Reserve space for both descriptions
with st.container(height=96, border=False):
    st.caption(methodology_note)
    st.caption(
        "Return basis applies to individual securities only. "
        "Portfolio and benchmark conventions remain unchanged."
    )


st.subheader("Cumulative Performance")

cumulative = build_matrix(
    report=report,
    methodology=methodology,
    return_basis=return_basis,
    metric="cumulative_return",
    horizons=HORIZONS,
)
asset_width = max(
    110,
    max(len(str(asset)) for asset in cumulative.index) * 9 + 24
)

st.dataframe(
    style_performance_matrix(cumulative),
    width="stretch",
    height="content",
    column_config={
        "_index": st.column_config.TextColumn(
            "Asset",
            width=asset_width,
        )
    },
)


st.subheader("Annualized Performance")

annualized = build_matrix(
    report=report,
    methodology=methodology,
    return_basis=return_basis,
    metric="annualized_return",
    horizons=TRAILING_HORIZONS,
)

st.dataframe(
    style_performance_matrix(annualized),
    width="stretch",
    height="content",
    column_config={
        "_index": st.column_config.TextColumn(
            "Asset",
            width=asset_width,
        )
    },
)

st.caption(
    "† Partial investment history or intermittent ownership. "
    "Cumulative returns reflect available invested periods; "
    "annualized returns are omitted without full-period "
    "coverage. Benchmarks use dividend-adjusted closing prices."
)

with st.expander("Performance methodology"):
    st.write(
        "**Daily TWR:** Geometrically linked daily-valued "
        "returns using the established cash-flow timing "
        "convention. Trailing returns extend through the "
        "latest available valuation date."
    )

    st.write(
        "**Modified Dietz:** Geometrically linked monthly "
        "Modified Dietz returns. MTD, QTD, and YTD include "
        "the current partial month. Trailing periods use "
        "completed calendar months."
    )

    st.write(
        "**Individual securities:** Gross, net-of-ADR-fees, "
        "and after-foreign-withholding return bases are "
        "available. Partial ownership histories are flagged."
    )

    st.write(
        "**Annualization:** Trailing 12-, 24-, 36-, and "
        "60-month returns are annualized only when the "
        "security has complete investment history for "
        "the respective horizon."
    )
