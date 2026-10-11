import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


COMPANY_CHART_COLORS = {
    "NVO": {
        "primary": "#00008F",
        "secondary": "#6E8CFF",
    },
    "TOST": {
        "primary": "#EF4B24",
        "secondary": "#FF9A73",
    },
    "NU": {
        "primary": "#830AD1",
        "secondary": "#C084FC",
    },
    "CPNG": {
        "primary": "#3FACDB",
        "secondary": "#F9CB38",
    },
    "TSM": {
        "primary": "#E60012",
        "secondary": "#FF8A94",
    },
    "PONY": {
        "primary": "#73DBE5",
        "secondary": "#D2D7DC",
    },
    "SKHY": {
        "primary": "#ED6900",
        "secondary": "#D6001F",
    },
    "CBRS": {
        "primary": "#F25A29",
        "secondary": "#E7EDF6",
    },
    "SPCX": {
        "primary": "#AEB8C5",
        "secondary": "#F4F7FB",
    },
}

DEFAULT_CHART_COLORS = {
    "primary": "#806CEB",
    "secondary": "#B5A8F5",
}


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
    periods: int = 10,
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

    chart_df["year"] = (
        pd.to_datetime(chart_df["fiscal_date"])
        .dt.year
        .astype(str)
    )

    fig = make_subplots(
        specs=[[{"secondary_y": True}]]
    )

    fig.add_trace(
        go.Bar(
            x=chart_df["year"],
            y=chart_df["revenue_display"],
            customdata=chart_df["total_revenue"],
            name="Revenue",
            marker=dict(
                color=colors["primary"],
                opacity=0.48,
                line=dict(width=0),
            ),
            hovertemplate=(
                f"{currency} %{{customdata:,.0f}}"
                "<extra>Revenue</extra>"
            ),
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["year"],
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
                "%{y:.1%}"
                "<extra>YoY Growth</extra>"
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