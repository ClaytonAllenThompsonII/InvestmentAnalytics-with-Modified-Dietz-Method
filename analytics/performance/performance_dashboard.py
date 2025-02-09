import os
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection():
    """Establish a connection to the database."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        return None


def fetch_active_positions():
    """
    Fetch only active positions (instruments with last eom_shares_cumulative > 0)
    from the asset_value_view. We then return ALL monthly rows for those
    instruments, so we can calculate longer-term returns.
    """
    query = """
        WITH latest_position AS (
            SELECT
                instrument,
                MAX(period_end_date) AS latest_period
            FROM asset_value_view
            GROUP BY instrument
        ),
        active_instruments AS (
            SELECT
                av.instrument
            FROM asset_value_view av
            JOIN latest_position lp
              ON av.instrument = lp.instrument
             AND av.period_end_date = lp.latest_period
            WHERE av.eom_shares_cumulative > 0
        )
        SELECT av.*
        FROM asset_value_view av
        JOIN active_instruments ai
          ON av.instrument = ai.instrument
        ORDER BY av.instrument, av.period_end_date;
    """
    try:
        conn = get_connection()
        if conn is None:
            return None
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def calculate_twr(data):
    """
    Calculate the Time-Weighted Return (TWR) using geometric linking
    based on the monthly 'md_return' values from asset_value_view.
    Each row's md_return is a decimal (e.g. 0.02 for 2%).
    """
    if len(data) == 0:
        return None
    # TWR = Π(1 + monthly_return) - 1
    returns = (1 + data["md_return"]).prod(skipna=True)
    # Subtract 1 to convert from growth factor back to percentage
    return returns - 1


def calculate_trailing_return(data, months):
    """Calculate trailing return for a specific number of months."""
    if len(data) == 0:
        return None
    # Filter for the trailing period
    max_date = data["period_end_date"].max()
    if pd.isnull(max_date):
        return None
    cutoff_date = max_date - pd.DateOffset(months=months)
    trailing_data = data[data["period_end_date"] > cutoff_date]
    return calculate_twr(trailing_data)


def calculate_linked_returns(df):
    """Calculate MTD, QTD, YTD, TTM, T2Y, and LTD returns, 
       both per-instrument AND a final 'PORTFOLIO' row."""
    summary = []

    # ---------------------------
    # 1) Per-Instrument grouping
    # ---------------------------
    for instrument, group in df.groupby("instrument"):
        group = group.sort_values("period_end_date")
        group["period_end_date"] = pd.to_datetime(group["period_end_date"])

        if group.empty:
            continue

        # Latest month for *this* instrument
        latest_month = group["period_end_date"].max()

        # MTD: from the 1st day of that instrument's latest_month
        mtd_start = latest_month.replace(day=1)
        mtd_data = group[group["period_end_date"] >= mtd_start]

        # QTD: figure out how many months into quarter
        current_month = latest_month.month
        months_into_quarter = (current_month - 1) % 3
        qtd_start = latest_month.replace(day=1) - pd.DateOffset(months=months_into_quarter)
        qtd_data = group[group["period_end_date"] >= qtd_start]

        # YTD: from the 1st day of the current year
        ytd_start = latest_month.replace(month=1, day=1)
        ytd_data = group[group["period_end_date"] >= ytd_start]

        summary.append({
            "Instrument": instrument,
            "MTD": calculate_twr(mtd_data),
            "QTD": calculate_twr(qtd_data),
            "YTD": calculate_twr(ytd_data),
            "TTM": calculate_trailing_return(group, 12),
            "T2Y": calculate_trailing_return(group, 24),
            "LTD": calculate_twr(group)  # entire history for that instrument
        })

    summary_df = pd.DataFrame(summary)
    # Sort by LTD descending, except that "PORTFOLIO" 
    # might appear at bottom anyway unless you handle it specially
    summary_df = summary_df.sort_values(by="LTD", ascending=False)
    return summary_df

def format_as_percentage(df):
    """Convert decimal values to percentages and add a % sign."""
    df_percentage = df.copy()
    percentage_cols = ["MTD", "QTD", "YTD", "TTM", "T2Y", "LTD"]
    for col in percentage_cols:
        df_percentage[col] = df_percentage[col].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notnull(x) else "N/A"
        )
    return df_percentage

def fetch_portfolio_monthly():
    """
    Fetch the aggregated monthly portfolio data from 'portfolio_monthly_agg'.
    Returns a DataFrame with columns:
        period_end_date, portfolio_bom, portfolio_eom, total_net_flow,
        total_weighted_flow, md_return
    Each row is one month's portfolio-level data.
    """
    query = """
        SELECT
            period_end_date,
            portfolio_bom,
            portfolio_eom,
            total_net_flow,
            total_weighted_flow,
            md_return
        FROM portfolio_monthly_agg
        ORDER BY period_end_date
    """
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        df_port = pd.read_sql_query(query, conn)
        conn.close()
        # Convert period_end_date to datetime
        df_port['period_end_date'] = pd.to_datetime(df_port['period_end_date'])
        return df_port
    except Exception as e:
        print(f"Error fetching portfolio monthly data: {e}")
        return pd.DataFrame()
    
def calculate_portfolio_returns(df_port):
    """
    Given the portfolio_monthly_agg DataFrame (one row per month),
    compute MTD, QTD, YTD, TTM, T2Y, and LTD by geometric linking of md_return.
    """
    if df_port.empty:
        return {}

    df_port = df_port.sort_values("period_end_date")
    latest_month = df_port["period_end_date"].max()

    # Helper function: TWR on a sub-slice
    def portfolio_twr(data):
        # TWR = Π(1 + md_return) - 1, ignoring NaN
        if data.empty:
            return None
        return (1 + data["md_return"].fillna(0)).prod() - 1

    # MTD
    mtd_start = latest_month.replace(day=1)
    mtd_data = df_port[df_port["period_end_date"] >= mtd_start]

    # QTD
    current_month = latest_month.month
    months_into_quarter = (current_month - 1) % 3
    qtd_start = latest_month.replace(day=1) - pd.DateOffset(months=months_into_quarter)
    qtd_data = df_port[df_port["period_end_date"] >= qtd_start]

    # YTD
    ytd_start = latest_month.replace(month=1, day=1)
    ytd_data = df_port[df_port["period_end_date"] >= ytd_start]

    # TTM
    ttm_cutoff = latest_month - pd.DateOffset(months=12)
    ttm_data = df_port[df_port["period_end_date"] > ttm_cutoff]

    # T2Y
    t2y_cutoff = latest_month - pd.DateOffset(months=24)
    t2y_data = df_port[df_port["period_end_date"] > t2y_cutoff]

    # LTD = entire df
    ltd_data = df_port

    return {
        "Instrument": "PORTFOLIO",
        "MTD": portfolio_twr(mtd_data),
        "QTD": portfolio_twr(qtd_data),
        "YTD": portfolio_twr(ytd_data),
        "TTM": portfolio_twr(ttm_data),
        "T2Y": portfolio_twr(t2y_data),
        "LTD": portfolio_twr(ltd_data)
    }    

def main():
    # 1) Instrument-level data
    df_instr = fetch_active_positions()
    if df_instr is None or df_instr.empty:
        print("No instrument-level data or no active positions.")
        return

    instr_summary_df = calculate_linked_returns(df_instr)

    # 2) Portfolio-level data
    df_port = fetch_portfolio_monthly()
    if df_port.empty:
        print("No portfolio_monthly_agg data found.")
        # We can still show instrument summary, but no portfolio row
        combined_df = instr_summary_df
    else:
        port_row = calculate_portfolio_returns(df_port)  # single dict
        port_df  = pd.DataFrame([port_row])

        # 3) Combine them
        combined_df = pd.concat([instr_summary_df, port_df], ignore_index=True)

        # If you want the portfolio row at bottom, you can do:
        #   - add a flag is_portfolio = True/False, then sort
        combined_df["is_portfolio"] = (combined_df["Instrument"] == "PORTFOLIO")
        # sort first by is_portfolio (False=0 => top, True=1 => bottom),
        # then by LTD descending (like before)
        combined_df.sort_values(
            by=["is_portfolio", "LTD"], 
            ascending=[True, False], 
            inplace=True
        )
        combined_df.drop(columns=["is_portfolio"], inplace=True)

    # 4) Print the final results
    print("\nInstrument + Portfolio Returns (Decimal):")
    print(combined_df)

    combined_df_pct = format_as_percentage(combined_df)
    print("\nInstrument + Portfolio Returns (Percentage):")
    print(combined_df_pct)

if __name__ == "__main__":
    main()