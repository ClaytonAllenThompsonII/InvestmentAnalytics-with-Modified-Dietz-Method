import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def build_revenue_growth_chart(
    df: pd.DataFrame,
    currency: str,
    periods: int = 10,
) -> go.Figure:
    """
    Build an annual revenue and YoY growth chart.

    Expects a prepared income-statement metrics DataFrame containing:
    fiscal_date, total_revenue, and revenue_growth.
    """

    chart_df = df.tail(periods).copy()

    fig = make_subplots(
        specs=[[{"secondary_y": True}]]
    )

    fig.add_trace(
        go.Bar(
            x=chart_df["fiscal_date"],
            y=chart_df["total_revenue"],
            name="Revenue",
            marker_color="#6B8AFD",
            hovertemplate=(
                f"{currency} %{{y:,.0f}}"
                "<extra>Revenue</extra>"
            ),
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["fiscal_date"],
            y=chart_df["revenue_growth"],
            name="YoY Growth",
            mode="lines+markers",
            line=dict(
                color="#A8B8FF",
                width=2,
            ),
            marker=dict(size=6),
            hovertemplate=(
                "%{y:.1%}"
                "<extra>YoY Growth</extra>"
            ),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title="Revenue & Growth",
        height=420,
        margin=dict(
            l=20,
            r=20,
            t=60,
            b=20,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    fig.update_xaxes(
        title=None,
        showgrid=False,
        tickformat="%Y",
    )

    fig.update_yaxes(
        title_text=f"Revenue ({currency})",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.08)",
        secondary_y=False,
    )

    fig.update_yaxes(
        title_text="YoY Growth",
        tickformat=".0%",
        showgrid=False,
        secondary_y=True,
    )

    return fig