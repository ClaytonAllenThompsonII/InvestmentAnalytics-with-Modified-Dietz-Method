def render_income_statement_page():
    import os
    import streamlit as st
    import pandas as pd
    import psycopg2
    from dotenv import load_dotenv
    import plotly.express as px
    import numpy as np


    # McKinsey/Dieter Rams-inspired styling
    st.markdown("""
        <style>
            html, body {
                background-color: #FAFAFA;
                color: #333333;
                font-family: 'Segoe UI', sans-serif, 'Helvetica Neue';
            }
            section[data-testid="stSidebar"] {
                background-color: #F5F5F5;
                padding: 20px;
            }
            div[data-testid="metric-container"] {
                background-color: #FFFFFF;
                border: 1px solid #DDDDDD;
                border-radius: 8px;
                padding: 20px;
                margin: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            h1, h2, h3 {
                color: #002D72;
                font-weight: 600;
            }
            .stPlotlyChart {
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                padding: 10px;
                background-color: #FFFFFF;
            }
            .stButton > button {
                background-color: #002D72;
                color: white;
                border-radius: 6px;
                padding: 6px 12px;
            }
            .stSelectbox, .stRadio {
                background-color: #FFFFFF;
            }
            .stDataFrame {
                border: 1px solid #DDD;
                border-radius: 4px;
            }
            .chart-wrapper {
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                background-color: #FFFFFF;
                padding: 10px;
                height: 310px; /* Matches stPlotlyChart height + padding/margin visually */
                display: flex;
                justify-content: center;
                align-items: center;
                margin-top: 10px;
                margin-bottom: 10px;
            }
            .warning {
                color: #C62828;
                font-style: italic;
                text-align: center;
            }
        </style>
    """, unsafe_allow_html=True)

    # Load environment variables
    load_dotenv()

    def get_connection():
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

    def load_data():
        with get_connection() as conn:
            return pd.read_sql("SELECT * FROM income_statements", conn)

    def render_chart(df, col, label, freq):
        st.subheader(label)
        if df[col].isna().all() or df[col].empty:
            st.markdown(
                f'''
                <div class="chart-wrapper">
                    <p class="warning">🚫 No valid data available for <strong>{label}</strong>.</p>
                </div>
                ''',
                unsafe_allow_html=True
            )
            return

        plot_data = df[[col, 'fiscal_date']].copy().fillna(0)
        fig = px.line(
            plot_data,
            x='fiscal_date',
            y=col,
            title=f"{label} ({freq.title()})",
            markers=True
        )
        fig.update_layout(
            xaxis_title="Fiscal Date",
            yaxis_title="USD",
            font_color="#4D4D4D",
            title_font_color="#002D72",
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            xaxis_tickangle=45,
            height=300,
            margin=dict(l=30, r=20, t=50, b=40)
        )
        fig.update_traces(line_color="#002D72", marker=dict(size=6))

        if plot_data[col].notna().sum() >= 2:
            x = np.arange(len(plot_data['fiscal_date']))
            z = np.polyfit(x, plot_data[col], 1)
            trend = np.poly1d(z)(x)
            fig.add_scatter(x=plot_data['fiscal_date'], y=trend, mode='lines', name='Trend', line=dict(color='#B22222', dash='dash'))

        st.plotly_chart(fig, use_container_width=True)

    # Main app
    st.title("\U0001F4CA Income Statement Dashboard")

    data = load_data()

    # Sidebar controls
    with st.sidebar:
        st.title("Filters")
        instruments = sorted(data['instrument'].unique())
        instrument = st.selectbox("Select a holding:", instruments)
        freq = st.radio("Select frequency:", ["annual", "quarterly"], horizontal=True)

    # Filter base data
    filtered = data[(data['instrument'] == instrument) & (data['frequency'] == freq)].copy()
    filtered['fiscal_date'] = pd.to_datetime(filtered['fiscal_date'], errors='coerce')
    filtered = filtered.sort_values("fiscal_date")

    # Unify depreciation
    filtered["unified_depreciation"] = filtered["depreciation"]
    mask = filtered["unified_depreciation"].isna() & filtered["depreciation_and_amortization"].notna()
    filtered.loc[mask, "unified_depreciation"] = filtered.loc[mask, "depreciation_and_amortization"]

    # Picker logic
    show_all = st.sidebar.checkbox("Show all data")
    period_limit = 10 if freq == "annual" else 40

    if freq == "annual":
        filtered['fiscal_year'] = filtered['fiscal_date'].dt.year
        available_years = sorted(filtered['fiscal_year'].dropna().unique())

        if show_all or len(available_years) <= period_limit:
            selected_start = available_years[0]
            selected_end = available_years[-1]
        else:
            selected_start = st.sidebar.selectbox("Start fiscal year:", options=available_years, index=max(0, len(available_years) - period_limit))
            selected_end = st.sidebar.selectbox("End fiscal year:", options=available_years, index=len(available_years) - 1)

        filtered = filtered[(filtered['fiscal_year'] >= selected_start) & (filtered['fiscal_year'] <= selected_end)]

    else:
        filtered['fiscal_quarter'] = filtered['fiscal_date'].dt.to_period("Q").astype(str)
        available_quarters = sorted(filtered['fiscal_quarter'].dropna().unique())

        if show_all or len(available_quarters) <= period_limit:
            selected_start = available_quarters[0]
            selected_end = available_quarters[-1]
        else:
            selected_start = st.sidebar.selectbox("Start fiscal quarter:", options=available_quarters, index=max(0, len(available_quarters) - period_limit))
            selected_end = st.sidebar.selectbox("End fiscal quarter:", options=available_quarters, index=len(available_quarters) - 1)

        qmask = (filtered['fiscal_quarter'] >= selected_start) & (filtered['fiscal_quarter'] <= selected_end)
        filtered = filtered[qmask]

    # Executive Summary
    with st.expander("Executive Summary", expanded=True):
        cols = st.columns(4)
        for i, (label, col) in enumerate([
            ("Latest Revenue", "total_revenue"),
            ("Latest Net Income", "net_income"),
            ("Latest EBITDA", "ebitda"),
            ("Latest Operating Income", "operating_income")
        ]):
            value = filtered[col].iloc[-1] if not filtered[col].empty and not pd.isna(filtered[col].iloc[-1]) else "N/A"
            cols[i].metric(label, f"${value:,.0f}" if isinstance(value, (int, float)) else value)

    # Financial Metrics Section
    st.markdown("---")
    st.subheader("Financial Metrics")

    line_metrics = [
        ("Total Revenue", "total_revenue"),
        ("Cost of Revenue", "cost_of_revenue"),
        ("COGS", "cost_of_goods_and_services_sold"),
        ("Gross Profit", "gross_profit"),
        ("Research & Development", "research_and_development"),
        ("SG&A", "selling_general_and_administrative"),
        ("Depreciation / Amortization", "unified_depreciation"),
        ("Operating Expenses", "operating_expenses"),
        ("Operating Income", "operating_income"),
        ("EBIT", "ebit"),
        ("EBITDA", "ebitda"),
        ("Income Before Tax", "income_before_tax"),
        ("Income Tax Expense", "income_tax_expense"),
        ("Net Income", "net_income")
    ]

    col1, col2 = st.columns(2)
    for i, (label, col_name) in enumerate(line_metrics):
        with col1 if i % 2 == 0 else col2:
            render_chart(filtered, col_name, label, freq)

    # Data preview
    st.markdown("---")
    st.subheader("Raw Data Preview")
    st.dataframe(filtered.reset_index(drop=True))

    # To run the Streamlit app, use the following command in your terminal:
    # streamlit run "fundamentals/income_statement.py"