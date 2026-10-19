import json
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


DEFAULT_CHART_COLORS = {
    "primary": "#806CEB",
    "secondary": "#B5A8F5",
}


COLOR_CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "assets"
    / "company_chart_colors.local.json"
)


def _load_company_chart_colors() -> dict:
    if not COLOR_CONFIG_PATH.exists():
        return {}

    with COLOR_CONFIG_PATH.open("r") as file:
        return json.load(file)


COMPANY_CHART_COLORS = _load_company_chart_colors()


def _get_revenue_scale(values: pd.Series) -> tuple[float, str]:
    """Return a display scale and suffix for financial values."""

    max_value = values.abs().max()

    if max_value >= 1_000_000_000_000:
        return 1_000_000_000_000, "T"

    if max_value >= 1_000_000_000:
        return 1_000_000_000, "B"

    if max_value >= 1_000_000:
        return 1_000_000, "M"

    if max_value >= 1_000:
        return 1_000, "K"

    return 1, ""


def build_revenue_growth_chart(
    df: pd.DataFrame,
    currency: str,
    instrument: str,
    frequency: str = "annual",
    periods: int = 10,
    ttm_df: pd.DataFrame | None = None,
) -> go.Figure:
    """
    Build an annual revenue and YoY growth chart.

    Expects a prepared income-statement metrics DataFrame containing:
    fiscal_date, total_revenue, and revenue_growth.
    """

    chart_df = df.tail(periods).copy()

    colors = COMPANY_CHART_COLORS.get(
        instrument,
        DEFAULT_CHART_COLORS,
    )

    scale, unit = _get_revenue_scale(
        chart_df["total_revenue"]
    )

    chart_df["revenue_display"] = (
        chart_df["total_revenue"] / scale
    )

    if frequency == "quarterly" and ttm_df is not None:
        ttm_columns = ttm_df[
            [
                "fiscal_date",
                "total_revenue_ttm",
                "revenue_growth_ttm",
            ]
        ].copy()

        chart_df = chart_df.merge(
            ttm_columns,
            on="fiscal_date",
            how="left",
        )

        chart_df["ttm_revenue_display"] = (
            chart_df["total_revenue_ttm"] / scale
        )

    fiscal_dates = pd.to_datetime(
        chart_df["fiscal_date"]
    )

    if frequency == "quarterly":
        chart_df["period_label"] = (
            "Q"
            + fiscal_dates.dt.quarter.astype(str)
            + " '"
            + fiscal_dates.dt.strftime("%y")
        )
    else:
        chart_df["period_label"] = (
            fiscal_dates.dt.year.astype(str)
        )

    fig = make_subplots(
        specs=[[{"secondary_y": True}]]
    )

    revenue_customdata = chart_df[
        ["revenue_display"]
    ].to_numpy()

    revenue_hovertemplate = (
        f"Revenue: {currency} %{{customdata[0]:,.2f}}{unit}"
        "<extra></extra>"
    )

    fig.add_trace(
        go.Bar(
            x=chart_df["period_label"],
            y=chart_df["revenue_display"],
            customdata=revenue_customdata,
            name="Revenue",
            marker=dict(
                color=colors["primary"],
                opacity=0.48,
                line=dict(width=0),
            ),
            hovertemplate=revenue_hovertemplate,
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["period_label"],
            y=chart_df["revenue_growth"],
            name="YoY Growth",
            mode="lines+markers",
            line=dict(
                color=colors["secondary"],
                width=2.5,
            ),
            marker=dict(
                color=colors["secondary"],
                size=7,
            ),
            hovertemplate=(
                "YoY Growth: %{y:.1%}"
                "<extra></extra>"
            ),
        ),
        secondary_y=True,
    )

    if frequency == "quarterly" and ttm_df is not None:
        chart_df["ttm_revenue_text"] = chart_df[
            "ttm_revenue_display"
        ].apply(
            lambda value: (
                "—"
                if pd.isna(value)
                else f"{currency} {value:,.2f}{unit}"
            )
        )

        chart_df["ttm_growth_text"] = chart_df[
            "revenue_growth_ttm"
        ].apply(
            lambda value: (
                "—"
                if pd.isna(value)
                else f"{value:.1%}"
            )
        )

        ttm_customdata = chart_df[
            [
                "ttm_revenue_text",
                "ttm_growth_text",
            ]
        ].to_numpy()

        fig.add_trace(
            go.Scatter(
                x=chart_df["period_label"],
                y=chart_df["revenue_growth"],
                customdata=ttm_customdata,
                mode="markers",
                marker=dict(
                    size=0,
                    opacity=0,
                ),
                name="",
                showlegend=False,
                hovertemplate=(
                    "<br>"
                    "TTM Revenue: %{customdata[0]}"
                    "<br>"
                    "TTM YoY Growth: %{customdata[1]}"
                    "<br>&nbsp;"
                    "<extra></extra>"
                ),
            ),
            secondary_y=True,
        )

    fig.update_layout(
        height=420,
        margin=dict(
            l=10,
            r=10,
            t=35,
            b=10,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            color="#C9CDD4",
        ),
        hovermode="x unified",
        hoverlabel=dict(
            align="left",
            font=dict(
                size=12,
            ),
        ),
        bargap=0.28,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11),
        ),
    )

    fig.update_xaxes(
        title=None,
        showgrid=False,
        showline=False,
        tickfont=dict(
            color="#8B919B",
        ),
    )

    fig.update_yaxes(
        title_text=f"Revenue ({currency}, {unit})",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.07)",
        zeroline=False,
        tickfont=dict(
            color="#8B919B",
        ),
        title_font=dict(
            color="#8B919B",
        ),
        secondary_y=False,
    )

    fig.update_yaxes(
        title_text="YoY Growth",
        tickformat=".0%",
        showgrid=False,
        zeroline=True,
        zerolinecolor="rgba(255,255,255,0.12)",
        tickfont=dict(
            color="#8B919B",
        ),
        title_font=dict(
            color="#8B919B",
        ),
        secondary_y=True,
    )

    return fig