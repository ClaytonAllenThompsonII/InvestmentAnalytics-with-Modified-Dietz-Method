import os
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# CONFIGURATION: LOAD DATABASE CREDENTIALS
# ------------------------------------------------------------------------------
load_dotenv()  # Load DB credentials from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ------------------------------------------------------------------------------
# CUSTOM CSS FOR A MINIMAL DESIGN (Inspired by Dieter Rams)
# ------------------------------------------------------------------------------
CUSTOM_CSS = """
<style>
/* Minimal background and neutral text color */
body {
    background-color: #F9F9F9;
    color: #333;
    font-family: "Helvetica Neue", Arial, sans-serif;
}

/* Make main title (h1) bigger & bolder */
h1 {
    font-size: 2.0rem !important;
    font-weight: 700 !important;
    margin-bottom: 0.5rem !important;
}

/* Subheader or section titles a bit larger, minimal margin */
h2, .stMarkdown h2 {
    font-size: 1.4rem !important;
    font-weight: 600 !important;
    margin-top: 1.0rem !important;
    margin-bottom: 0.6rem !important;
}

/* DataFrame table styling */
table {
    background-color: #FFF;
    border-collapse: collapse;
    width: 100%;
}
thead tr {
    background-color: #ECECEC;
}
tbody tr:nth-child(even) {
    background-color: #F3F3F3;
}
td, th {
    padding: 8px 12px;
    border: 1px solid #DDD;
}

/* Streamlit main container spacing */
.block-container {
    padding: 1rem 2rem !important;
}

/* Buttons / widget styling */
.stButton button {
    background-color: #666 !important;
    color: #FFF !important;
    border-radius: 4px !important;
    border: none !important;
    padding: 0.4rem 1rem !important;
}
</style>
"""

# ------------------------------------------------------------------------------
# HELPER: DATABASE CONNECTION
# ------------------------------------------------------------------------------
def get_connection():
    """Establish and return a connection to the database."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except psycopg2.Error as e:
        st.error(f"Database connection failed: {e}")
        return None

# ------------------------------------------------------------------------------
# 1) FETCH INSTRUMENT-LEVEL MONTHLY DATA (OPEN POSITIONS ONLY)
# ------------------------------------------------------------------------------
def fetch_open_instrument_monthly_data():
    """
    Pull monthly asset value data for instruments with open positions.
    This query uses asset_value_view joined with a CTE that finds, for each 
    instrument, the earliest lot_open_date (from fifo_equity_lots) when shares 
    were opened. Only monthly rows on or after that first open date are returned.
    """
    query = """
        WITH open_instruments AS (
            SELECT
                instrument,
                MIN(lot_open_date) AS first_open_date
            FROM fifo_equity_lots
            WHERE open_quantity > 0
            GROUP BY instrument
        )
        SELECT av.*
        FROM asset_value_view av
        JOIN open_instruments oi
          ON av.instrument = oi.instrument
        WHERE av.period_end_date >= oi.first_open_date
        ORDER BY av.instrument, av.period_end_date;
    """
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(query, conn)
        conn.close()
        df["period_end_date"] = pd.to_datetime(df["period_end_date"])
        return df
    except Exception as e:
        st.error(f"Error fetching open instrument monthly data: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------------------------
# 2) (OPTIONAL) FETCH PORTFOLIO-LEVEL MONTHLY DATA
# ------------------------------------------------------------------------------
def fetch_portfolio_monthly():
    """
    Dynamically compute portfolio-level monthly data by aggregating from asset_value_view.
    Sums BOM/EOM NAV, net cash flow, and weighted cash flow across all instruments
    for each month, then calculates a portfolio-level Modified Dietz return.
    """
    query = """
        WITH portfolio_monthly AS (
            SELECT
                av.period_end_date,
                SUM(av.nav_bom) AS portfolio_bom,
                SUM(av.nav_eom) AS portfolio_eom,
                SUM(av.net_cash_flow) AS total_net_flow,
                SUM(av.weighted_cash_flow) AS total_weighted_flow
            FROM asset_value_view av
            GROUP BY av.period_end_date
        )
        SELECT
            pm.period_end_date,
            pm.portfolio_bom,
            pm.portfolio_eom,
            pm.total_net_flow,
            CASE
                WHEN (pm.portfolio_bom + pm.total_weighted_flow) <> 0
                THEN ROUND(
                    (
                        (pm.portfolio_eom - pm.portfolio_bom - pm.total_net_flow)
                        / (pm.portfolio_bom + pm.total_weighted_flow)
                    )::numeric,
                    6
                )
                ELSE NULL
            END AS md_return
        FROM portfolio_monthly pm
        ORDER BY pm.period_end_date
    """

    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        df_port = pd.read_sql_query(query, conn)
        conn.close()
        df_port["period_end_date"] = pd.to_datetime(df_port["period_end_date"])
        return df_port
    except Exception as e:
        st.warning(f"Error fetching portfolio data: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------------------------
# 3) HELPER: CALCULATE TIME-WEIGHTED RETURN (TWR)
# ------------------------------------------------------------------------------
def calculate_twr_from_monthly(df, monthly_return_col="md_return"):
    """
    Compute the geometric link of monthly returns:
      TWR = Π(1 + monthly_return) - 1
    Replaces NaN with 0.
    """
    if df.empty:
        return None
    product_factor = (1 + df[monthly_return_col].fillna(0)).prod()
    return product_factor - 1

def calculate_trailing_return(df, months, date_col="period_end_date", monthly_return_col="md_return"):
    """
    Restrict DataFrame to the last 'months' months and compute TWR.
    For T2Y (24 months), return None if fewer than 24 records.
    """
    if df.empty:
        return None
    max_date = df[date_col].max()
    if pd.isnull(max_date):
        return None

    cutoff_date = max_date - pd.DateOffset(months=months)
    slice_df = df[df[date_col] > cutoff_date]

    if monthly_return_col == "md_return" and months == 24 and len(slice_df) < 24:
        return None

    return calculate_twr_from_monthly(slice_df, monthly_return_col=monthly_return_col)

# ------------------------------------------------------------------------------
# 4) BUILD INSTRUMENT-LEVEL SUMMARY
# ------------------------------------------------------------------------------
def build_instrument_summary(df):
    """
    Build a summary DataFrame at the instrument level from monthly data.
    Computes MTD, QTD, T3M, YTD, TTM, T2Y, LTD returns, plus latest open shares.
    """
    summary_rows = []

    for instrument, grp in df.groupby("instrument"):
        grp = grp.sort_values("period_end_date")
        if grp.empty:
            continue

        latest_date = grp["period_end_date"].max()

        # MTD: from the first day of the latest month
        mtd_start = latest_date.replace(day=1)
        mtd_slice = grp[grp["period_end_date"] >= mtd_start]

        # QTD: from start of current quarter
        current_month = latest_date.month
        months_into_quarter = (current_month - 1) % 3
        qtd_start = latest_date.replace(day=1) - pd.DateOffset(months=months_into_quarter)
        qtd_slice = grp[grp["period_end_date"] >= qtd_start]

        # T3M
        t3m = calculate_trailing_return(grp, 3)
        # YTD
        ytd_start = latest_date.replace(month=1, day=1)
        ytd_slice = grp[grp["period_end_date"] >= ytd_start]
        # TTM
        ttm = calculate_trailing_return(grp, 12)
        # T2Y
        t2y = calculate_trailing_return(grp, 24) if len(grp) >= 24 else None
        # LTD
        ltd = calculate_twr_from_monthly(grp)

        # If eom_shares_cumulative is present, get the latest
        total_shares = None
        if "eom_shares_cumulative" in grp.columns:
            total_shares = grp.iloc[-1]["eom_shares_cumulative"]

        summary_rows.append({
            "Instrument": instrument,
            "MTD": calculate_twr_from_monthly(mtd_slice),
            "QTD": calculate_twr_from_monthly(qtd_slice),
            "T3M": t3m,
            "YTD": calculate_twr_from_monthly(ytd_slice),
            "TTM": ttm,
            "T2Y": t2y,
            "LTD": ltd,
            "Total Shares": total_shares
        })

    return pd.DataFrame(summary_rows)

# ------------------------------------------------------------------------------
# 5) FETCH COST-BASED RETURNS
# ------------------------------------------------------------------------------
def fetch_cost_based_returns():
    """
    Aggregates total cost from fifo_equity_lots and retrieves
    the latest nav_eom from asset_value_view_open_positions,
    then computes total_pnl and cost_based_return.
    """
    query = """
        WITH total_costs AS (
          SELECT 
            instrument,
            SUM(total_cost) AS total_cost
          FROM fifo_equity_lots
          GROUP BY instrument
        ),
        latest_nav AS (
          SELECT av.instrument, av.nav_eom
          FROM asset_value_view_open_positions av
          JOIN (
              SELECT instrument, MAX(period_end_date) AS latest_date
              FROM asset_value_view_open_positions
              GROUP BY instrument
          ) ln ON av.instrument = ln.instrument 
             AND av.period_end_date = ln.latest_date
        )
        SELECT 
          tc.instrument,
          tc.total_cost,
          (ln.nav_eom - tc.total_cost) AS total_pnl,
          CASE 
            WHEN tc.total_cost <> 0 THEN (ln.nav_eom - tc.total_cost) / tc.total_cost
            ELSE NULL 
          END AS cost_based_return
        FROM total_costs tc
        JOIN latest_nav ln 
          ON tc.instrument = ln.instrument;
    """
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching cost-based returns: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------------------------
# 6) FORMAT RETURNS AS PERCENTAGES
# ------------------------------------------------------------------------------
def format_as_percentage(df, columns=None):
    """
    Convert decimal returns (e.g. 0.05) to percentage strings (e.g. '5.00%').
    """
    if columns is None:
        columns = ["MTD", "QTD", "T3M", "YTD", "TTM", "T2Y", "LTD", "cost_based_return"]

    df_formatted = df.copy()
    for col in columns:
        if col in df_formatted.columns:
            df_formatted[col] = df_formatted[col].apply(
                lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "N/A"
            )
    return df_formatted

# ------------------------------------------------------------------------------
# 7) (OPTIONAL) BUILD PORTFOLIO SUMMARY
# ------------------------------------------------------------------------------
def build_portfolio_summary(df_port):
    """
    Creates a single-row summary (PORTFOLIO) from the monthly portfolio data,
    computing MTD, QTD, T3M, YTD, TTM, T2Y, LTD, etc.
    """
    if df_port.empty:
        return pd.DataFrame()

    df_port = df_port.sort_values("period_end_date")
    latest_date = df_port["period_end_date"].max()

    # MTD
    mtd_start = latest_date.replace(day=1)
    mtd_slice = df_port[df_port["period_end_date"] >= mtd_start]

    # QTD
    current_month = latest_date.month
    months_into_quarter = (current_month - 1) % 3
    qtd_start = latest_date.replace(day=1) - pd.DateOffset(months=months_into_quarter)
    qtd_slice = df_port[df_port["period_end_date"] >= qtd_start]

    # T3M
    t3m = calculate_trailing_return(df_port, 3)
    # YTD
    ytd_start = latest_date.replace(month=1, day=1)
    ytd_slice = df_port[df_port["period_end_date"] >= ytd_start]
    # TTM
    ttm = calculate_trailing_return(df_port, 12)
    # T2Y
    t2y = calculate_trailing_return(df_port, 24) if len(df_port) >= 24 else None
    # LTD
    ltd = calculate_twr_from_monthly(df_port)

    row = {
        "Instrument": "PORTFOLIO",
        "MTD": calculate_twr_from_monthly(mtd_slice),
        "QTD": calculate_twr_from_monthly(qtd_slice),
        "T3M": t3m,
        "YTD": calculate_twr_from_monthly(ytd_slice),
        "TTM": ttm,
        "T2Y": t2y,
        "LTD": ltd,
        "Total Shares": None
    }
    return pd.DataFrame([row])

# ------------------------------------------------------------------------------
# 8) AREA CHART FOR PORTFOLIO EOM NAV
# ------------------------------------------------------------------------------
def plot_portfolio_nav_area(df_port):
    """
    Plot an area chart showing the portfolio's EOM NAV over time.
    """
    if df_port.empty:
        st.warning("No portfolio data to plot for NAV EOM.")
        return

    fig_nav = go.Figure()
    fig_nav.add_trace(go.Scatter(
        x=df_port["period_end_date"],
        y=df_port["portfolio_eom"],
        fill='tozeroy',
        mode='lines',
        name='Portfolio EOM NAV',
        line_color='blue'
    ))
    fig_nav.update_layout(
        title="Portfolio End-of-Month NAV Over Time",
        xaxis_title="Period End Date",
        yaxis_title="EOM NAV",
        template="plotly_white"
    )
    st.plotly_chart(fig_nav, use_container_width=True)

# ------------------------------------------------------------------------------
# 9) STREAMLIT DASHBOARD
# ------------------------------------------------------------------------------
def main():
    # Set page config to wide mode, plus a custom title.
    st.set_page_config(page_title="Investment Returns Dashboard", layout="wide")
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    st.title("Investment Returns Dashboard")

    # 1) Fetch monthly data for open instruments
    st.subheader("1) Fetching Monthly Data for Open Instruments")
    df_open = fetch_open_instrument_monthly_data()
    if df_open.empty:
        st.error("No open-instrument data found. Exiting.")
        return
    st.write("Monthly data for open instruments (snippet):")
    st.dataframe(df_open.head(10))  # Show a snippet

    # 2) Build instrument-level summary
    st.subheader("2) Instrument-Level Returns Summary")
    instr_summary_df = build_instrument_summary(df_open)

    # 3) Fetch cost-based returns, merge into summary
    cost_returns_df = fetch_cost_based_returns()
    final_df = pd.merge(instr_summary_df, cost_returns_df, left_on="Instrument", right_on="instrument", how="left")
    if "instrument" in final_df.columns:
        final_df.drop(columns=["instrument"], inplace=True)

    # 4) Fetch portfolio-level data
    df_portfolio = fetch_portfolio_monthly()
    if not df_portfolio.empty:
        st.subheader("Portfolio-Level Data Found")
        # 4A) Build single-row summary for 'PORTFOLIO'
        portfolio_row = build_portfolio_summary(df_portfolio)
        final_df = pd.concat([final_df, portfolio_row], ignore_index=True)

        # 4B) Place the portfolio row last
        final_df["is_portfolio"] = (final_df["Instrument"] == "PORTFOLIO")
        final_df.sort_values(["is_portfolio", "LTD"], ascending=[True, False], inplace=True)
        final_df.drop(columns=["is_portfolio"], inplace=True)

        # 4C) Show an area chart for EOM NAV
        st.subheader("Portfolio EOM NAV Over Time")
        plot_portfolio_nav_area(df_portfolio)

    else:
        st.info("No portfolio-level data found; skipping portfolio summary.")

    # 5) Convert the final DataFrame to percentage columns & show a single table
    st.subheader("3) Instrument + Portfolio Returns (Percentage)")
    final_df_pct = format_as_percentage(final_df)
    st.dataframe(final_df_pct)

    # 6) (Optional) Quick Plotly chart example: TTM
    st.subheader("4) Sample Plotly Chart (TTM Returns)")
    fig = make_subplots(rows=1, cols=1, shared_xaxes=True)

    def parse_percent_to_float(s):
        if isinstance(s, str) and s.endswith("%"):
            return float(s[:-1])
        return None

    ttm_values = final_df_pct["TTM"].apply(parse_percent_to_float)
    fig.add_trace(go.Bar(
        x=final_df_pct["Instrument"],
        y=ttm_values,
        name="TTM Return (%)",
        marker_color="blue"
    ))
    fig.update_layout(
        title="TTM Returns by Instrument",
        template="plotly_white",
        yaxis=dict(title="TTM (%)")
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()


    # streamlit run "analytics/performance/investment_returns.py"