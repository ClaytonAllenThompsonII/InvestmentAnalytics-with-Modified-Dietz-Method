
"""Risk Detail MVP — security and portfolio risk reporting."""

import pandas as pd
import streamlit as st

from investment_analytics.risk.risk_metrics import (
    load_risk_report,
)


# ---------------------------------------------------------------------
# PAGE CONFIGURATION
# ---------------------------------------------------------------------

st.set_page_config(
    page_title="Risk Detail | Investment Analytics",
    layout="wide",
)

LOOKBACK_OPTIONS = [
    "6 Months",
    "1 Year",
    "2 Years",
    "3 Years",
]

BENCHMARK_OPTIONS = [
    "SPY",
    "QQQ",
    "XLK",
    "XLY",
    "EEM",
    "IXUS",
]


# ---------------------------------------------------------------------
# STYLING
# ---------------------------------------------------------------------

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }

        div[data-testid="stMetric"] {
            border: 1px solid #e5e7eb;
            border-radius: 5px;
            padding: 0.75rem 1rem;
        }

        div[data-testid="stMetricLabel"] {
            font-size: 0.85rem;
        }

        div[data-testid="stDataFrame"] * {
            font-family: inherit;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def get_cached_risk_report(
    benchmark: str,
    lookback: str,
) -> dict:
    """Cache report calculations for five minutes."""
    return load_risk_report(
        benchmark=benchmark,
        lookback=lookback,
    )


# ---------------------------------------------------------------------
# DISPLAY HELPERS
# ---------------------------------------------------------------------


def control_label(text: str):
    """Display a compact reporting-control label."""

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



def display_security_matrix(matrix: pd.DataFrame) -> None:
    """Render the security-level risk table with equity totals."""

    columns = {
        "ticker": "Asset",
        "nmv": "NMV ($)",
        "weight": "Weight",
        "beta": "Beta",
        "idio_vol_daily": "Daily Idio Vol.",
        "dollar_beta": "Dollar Beta ($)",
        "observations": "Obs.",
        "coverage": "Coverage",
        "status": "History Status",
    }

    display = (
        matrix[list(columns)]
        .rename(columns=columns)
        .copy()
    )

    # -------------------------------------------------------------
    # TOTAL EQUITY ROW
    # -------------------------------------------------------------

    total_nmv = float(matrix["nmv"].sum())
    all_betas_available = matrix["beta"].notna().all()

    total_dollar_beta = (
        float(matrix["dollar_beta"].sum())
        if all_betas_available else float("nan")
    )

    portfolio_beta = (
        total_dollar_beta / total_nmv
        if total_nmv > 0 and all_betas_available
        else float("nan")
    )

    total_row = pd.DataFrame([{
        "Asset": "Total Equity",
        "NMV ($)": total_nmv,
        "Weight": float(matrix["weight"].sum()),
        "Beta": portfolio_beta,
        "Daily Idio Vol.": float("nan"),
        "Dollar Beta ($)": total_dollar_beta,
        "Obs.": float("nan"),
        "Coverage": float("nan"),
        "History Status": "—",
    }])

    display = pd.concat(
        [display, total_row],
        ignore_index=True,
    )

    # -------------------------------------------------------------
    # TABLE STYLING
    # -------------------------------------------------------------

    def highlight_total(row):
        if row["Asset"] == "Total Equity":
            return [
                "background-color: rgba(100, 116, 139, 0.15); "
                "font-weight: bold"
            ] * len(row)

        return [""] * len(row)

    styled = (
        display.style
        .set_properties(
            **{"background-color": "#F8FAFC"}
        )
        .format(
            {
                "NMV ($)": "${:,.0f}",
                "Weight": "{:.2%}",
                "Beta": "{:.3f}",
                "Daily Idio Vol.": "{:.2%}",
                "Dollar Beta ($)": "${:,.0f}",
                "Obs.": "{:,.0f}",
                "Coverage": "{:.1%}",
            },
            na_rep="—",
        )
        .apply(highlight_total, axis=1)
    )

    st.dataframe(
        styled,
        width="stretch",
        height="content",
        hide_index=True,
        column_config={
            "Asset": st.column_config.TextColumn(
                "Asset", width=110
            ),
            "NMV ($)": st.column_config.NumberColumn(
                "NMV ($)", width=135
            ),
            "Weight": st.column_config.NumberColumn(
                "Weight", width=105
            ),
            "Beta": st.column_config.NumberColumn(
                "Beta", width=95
            ),
            "Daily Idio Vol.": st.column_config.NumberColumn(
                "Daily Idio Vol.", width=135
            ),
            "Dollar Beta ($)": st.column_config.NumberColumn(
                "Dollar Beta ($)", width=145
            ),
            "Obs.": st.column_config.NumberColumn(
                "Obs.", width=85
            ),
            "Coverage": st.column_config.NumberColumn(
                "Coverage", width=105
            ),
            "History Status": st.column_config.TextColumn(
                "History Status", width=150
            ),
        },
    )

    st.caption(
        "Idiosyncratic volatility is the daily standard deviation "
        "of OLS regression residuals. "
        "NMV and weights represent currently held long equities. "
        "Portfolio-level idiosyncratic volatility is not the sum "
        "of individual security volatilities."
    )


#
def display_methodology(
    report: dict,
) -> None:
    """Compare alternative portfolio-risk estimators."""

    summary = report["summary"]
    common = report["common_window"]

    methods = pd.DataFrame(
        [
            {
                "Method": "A · Available-history independent",
                "Beta": summary["portfolio_beta"],
                "Market Vol.": summary["portfolio_market_vol_ann"],
                "Idio Vol.": summary[
                    "portfolio_idio_vol_ann_available_history"
                ],
                "Total Vol.": summary[
                    "portfolio_total_vol_ann_available_history"
                ],
                "Estimation": "Selected lookback / per security",
            },
            {
                "Method": "B · Residual covariance",
                "Beta": summary["portfolio_beta"],
                "Market Vol.": summary["portfolio_market_vol_ann"],
                "Idio Vol.": summary[
                    "portfolio_idio_vol_ann_covariance"
                ],
                "Total Vol.": summary[
                    "portfolio_total_vol_ann"
                ],
                "Estimation": (
                    f"Residuals: {summary['common_observations']} "
                    "common days"
                ),
            },
            {
                "Method": "C · Common-window OLS",
                "Beta": common["portfolio_beta"],
                "Market Vol.": common["portfolio_market_vol_ann"],
                "Idio Vol.": common["portfolio_idio_vol_ann"],
                "Total Vol.": common["portfolio_total_vol_ann"],
                "Estimation": (
                    f"{common['observations']} common days"
                ),
            },
        ]
    )

    styled = methods.style.format(
        {
            "Beta": "{:.3f}",
            "Market Vol.": "{:.2%}",
            "Idio Vol.": "{:.2%}",
            "Total Vol.": "{:.2%}",
        }
    )

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height="content",
    )

    st.markdown(
        """
        **Model A — Available-history independent residuals**

        Security betas and residual volatilities use the selected
        lookback, subject to available market history. Portfolio
        residual risk assumes zero cross-security residual covariance.

        **Model B — Mixed-window residual covariance**

        Uses the same security beta estimates and benchmark volatility
        as Model A, but incorporates residual covariances estimated on
        dates shared by all modeled equities.

        **Model C — Common-window OLS**

        Re-estimates the entire synthetic portfolio regression on the
        common historical sample. Its total volatility reconciles to
        the standard deviation of daily constant-weight portfolio returns.
        """
    )

    st.caption(
        "All portfolio volatility measures are annualized using "
        "252 trading days. The models are historical estimates, "
        "not forecasts or actual portfolio accounting returns."
    )

    st.caption(
        f"Common estimation window: "
        f"{common['start_date']} to {common['end_date']} "
        f"({common['observations']} observations)."
    )


# ---------------------------------------------------------------------
# MAIN APPLICATION
# ---------------------------------------------------------------------

def main() -> None:

    heading, date_column = st.columns([4, 1])

    with heading:
        st.title("Risk Detail")

    
    # -------------------------------------------------------------
    # REPORT CONTROLS
    # -------------------------------------------------------------

    ctrl1, ctrl2, ctrl3, _ = st.columns(
        [1.15, 1.15, 1.2, 2.5],
        gap="small",
    )

    with ctrl1:
        control_label("Estimation Window")

        lookback = st.selectbox(
            "Estimation Window",
            options=LOOKBACK_OPTIONS,
            index=2,
            label_visibility="collapsed",
        )

    with ctrl2:
        control_label("Market Benchmark")

        benchmark = st.selectbox(
            "Market Benchmark",
            options=BENCHMARK_OPTIONS,
            index=0,
            label_visibility="collapsed",
        )


    try:
        with st.spinner("Calculating portfolio risk..."):
            report = get_cached_risk_report(
                benchmark=benchmark,
                lookback=lookback,
            )

    except Exception as exc:
        st.error(f"Unable to load Risk Detail: {exc}")
        st.stop()

    matrix = report["matrix"]
    summary = report["summary"]

    # Display reporting date in the top-right header
    with date_column:
        st.markdown(
            f"**As of {report['end_date']:%B %d, %Y}**"
        )

    with ctrl3:
        st.metric(
            f"Daily Market Vol. ({benchmark})",
            f"{summary['market_vol_daily']:.2%}",
            help="Daily standard deviation of benchmark returns.",
        )

    st.caption(
        f"Estimation: {report['start_date']} to "
        f"{report['end_date']} · "
        "Long-equity exposures only"
    )

    # -------------------------------------------------------------
    # PORTFOLIO RISK OVERVIEW
    # -------------------------------------------------------------

    st.subheader("Portfolio Risk Overview")

    beta_col, dollar_beta_col = st.columns(2, gap="medium")

    with beta_col:
        st.metric(
            "Portfolio Beta",
            f"{summary['portfolio_beta']:.4f}",
            help="Current-equity-market-value-weighted security beta.",
        )

    with dollar_beta_col:
        st.metric(
            "Dollar Beta",
            f"${summary['dollar_beta']:,.0f}",
            help="Sum of security equity market value multiplied by beta.",
        )

    market_col, idio_col, total_col = st.columns(
        3, gap="medium"
    )

    with market_col:
        st.metric(
            "Portfolio Market Vol. (Ann.)",
            f"{summary['portfolio_market_vol_ann']:.2%}",
        )

    with idio_col:
        st.metric(
            "Portfolio Idio Vol. (Ann.)",
            f"{summary['portfolio_idio_vol_ann_available_history']:.2%}",
        )

    with total_col:
        st.metric(
            "Portfolio Total Vol. (Ann.)",
            f"{summary['portfolio_total_vol_ann_available_history']:.2%}",
        )

    st.caption(
        "Default: available-history independent-residual model. "
        "Portfolio volatility is holdings-implied and annualized. "
        "Equity-only calculations exclude cash and options."
    )

    # -------------------------------------------------------------
    # SECURITY RISK EXPOSURE
    # -------------------------------------------------------------

    st.subheader("Security Risk Exposure")

    display_security_matrix(matrix)

    partial = matrix.loc[
        matrix["status"].ne("OK"), "ticker"
    ].tolist()

    if partial:
        st.caption(
            "Partial or insufficient estimation history: "
            + ", ".join(partial)
            + ". See observation counts and coverage above."
        )

    # -------------------------------------------------------------
    # METHODOLOGY AND MODEL COMPARISON
    # -------------------------------------------------------------

    with st.expander(
        "Risk methodology and estimation comparison",
        expanded=False,
    ):
        display_methodology(report)


if __name__ == "__main__":
    main()
