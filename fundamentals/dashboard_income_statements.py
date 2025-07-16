import os
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter

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

st.set_page_config(page_title="Income Statement Dashboard", layout="wide")
st.title("📊 Income Statement Dashboard")

# Load and cache data
data = load_data()

# Instruments dropdown
instruments = sorted(data['instrument'].unique())
instrument = st.selectbox("Select a holding:", instruments)

filtered = data[data['instrument'] == instrument].copy()

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

# Frequency selection
freq = st.radio("Select frequency:", ["annual", "quarterly"], horizontal=True)
filtered = filtered[filtered['frequency'] == freq].sort_values("fiscal_date")

# Helper: Format Y-axis in billions or millions
def billions(x, pos):
    if x >= 1e9:
        return f'${x*1e-9:.1f}B'
    elif x >= 1e6:
        return f'${x*1e-6:.0f}M'
    else:
        return f'${x:,.0f}'

# Plot each metric section
for label, col in metrics:
    st.subheader(label)
    fig, ax = plt.subplots(figsize=(10, 2.2))  # ⬅️ Less vertical height

    # Optional: Reduce marker size to avoid visual clutter
    ax.plot(filtered['fiscal_date'], filtered[col], marker='o', markersize=4, linewidth=2)

    # Optional: More dense X-axis labels (especially for long date spans)
    ax.xaxis.set_major_locator(plt.MaxNLocator(10))  # Show fewer x-axis labels
    
    # Plot line with markers
    ax.plot(filtered['fiscal_date'], filtered[col], marker='o', linewidth=2)
    
    # Title and axis labels
    ax.set_title(f"{label} ({freq.title()})", fontsize=14, pad=10)
    ax.set_xlabel("Fiscal Date", fontsize=12)
    ax.set_ylabel("USD", fontsize=12)

    # Apply custom formatter for Y-axis
    ax.yaxis.set_major_formatter(FuncFormatter(billions))

    # Minimalist design
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # Rotate dates slightly for better fit
    plt.xticks(rotation=45)

    st.pyplot(fig)

# Data preview at bottom
st.markdown("---")
st.subheader("Raw Data Preview")
st.dataframe(filtered.reset_index(drop=True))


# To run the Streamlit app, use the following command in your terminal:
# streamlit run "fundamentals/dashboard_income_statements.py"