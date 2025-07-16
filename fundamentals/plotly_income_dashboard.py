import os
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# DB connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def load_data():
    query = """
        SELECT *
        FROM income_statements
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)

st.set_page_config(page_title="Income Statement Dashboard (Plotly)", layout="wide")
st.title("📈 Income Statement Dashboard (Plotly)")

# Load data
data = load_data()

# Instruments dropdown
instruments = sorted(data['instrument'].unique())
instrument = st.selectbox("Select a holding:", instruments)

# Frequency selection
freq = st.radio("Select frequency:", ["annual", "quarterly"], horizontal=True)

# Date range selector
year_filter = st.selectbox("Select date range:", ["All", "Last 10 Years", "Last 5 Years"])

# Chart type selector
chart_type = st.radio("Select chart type:", ["Line", "Bar"], horizontal=True)

filtered = data[(data['instrument'] == instrument) & (data['frequency'] == freq)].copy()
filtered = filtered.sort_values("fiscal_date")

if year_filter != "All":
    years = int(year_filter.split()[1])
    min_date = pd.Timestamp.now() - pd.DateOffset(years=years)
    filtered = filtered[filtered['fiscal_date'] >= min_date]

# Metric groups to plot
metrics = [
    ("Total Revenue", "total_revenue"),
    ("Gross Profit", "gross_profit"),
    ("Operating Income", "operating_income"),
    ("Net Income", "net_income"),
    ("EBITDA", "ebitda"),
    ("EBIT", "ebit"),
    ("Operating Expenses", "operating_expenses"),
    ("R&D", "research_and_development"),
    ("SG&A", "selling_general_and_administrative"),
    ("Depreciation", "depreciation"),
    ("Depreciation & Amortization", "depreciation_and_amortization"),
    ("Cost of Revenue", "cost_of_revenue"),
    ("COGS", "cost_of_goods_and_services_sold"),
    ("Income Before Tax", "income_before_tax"),
    ("Income Tax Expense", "income_tax_expense")
]

# Plotly chart loop
for label, col in metrics:
    st.subheader(label)

    if chart_type == "Line":
        fig = px.line(
            filtered,
            x="fiscal_date",
            y=col,
            markers=True,
            labels={"fiscal_date": "Fiscal Date", col: "USD"},
            title=f"{label} ({freq.title()})"
        )
        fig.update_traces(line=dict(width=2), marker=dict(size=6))
    else:
        fig = px.bar(
            filtered,
            x="fiscal_date",
            y=col,
            labels={"fiscal_date": "Fiscal Date", col: "USD"},
            title=f"{label} ({freq.title()})"
        )

    fig.update_yaxes(tickformat="$.2s")
    fig.update_layout(
        height=300,
        margin=dict(t=40, b=40, l=40, r=40),
        font=dict(size=13),
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

# Data preview at bottom
st.markdown("---")
st.subheader("Raw Data Preview")
st.dataframe(filtered.reset_index(drop=True))

# To run the Streamlit app:
# streamlit run "fundamentals/plotly_income_dashboard.py"