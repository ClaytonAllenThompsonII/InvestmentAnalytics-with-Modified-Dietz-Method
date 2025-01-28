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
    """Calculate MTD, QTD, YTD, TTM, T2Y, and LTD returns."""
    summary = []

    for instrument, group in df.groupby("instrument"):
        group = group.sort_values("period_end_date")
        group["period_end_date"] = pd.to_datetime(group["period_end_date"])

        # Find the latest month in this instrument's dataset
        if group.empty:
            continue
        latest_month = group["period_end_date"].max()

        # MTD: from the 1st day of the latest_month
        mtd_start = latest_month.replace(day=1)
        mtd_data = group[group["period_end_date"] >= mtd_start]

        # QTD: from the 1st day of the current quarter
        current_month = latest_month.month
        # "quarter" start offset: months since quarter start
        months_into_quarter = (current_month - 1) % 3  
        qtd_start = latest_month.replace(day=1) - pd.DateOffset(months=months_into_quarter)
        qtd_data = group[group["period_end_date"] >= qtd_start]

        # YTD: from the 1st day of the current year
        ytd_start = latest_month.replace(month=1, day=1)
        ytd_data = group[group["period_end_date"] >= ytd_start]

        # Calculate returns
        summary.append({
            "Instrument": instrument,
            "MTD": calculate_twr(mtd_data),
            "QTD": calculate_twr(qtd_data),
            "YTD": calculate_twr(ytd_data),
            "TTM": calculate_trailing_return(group, 12),
            "T2Y": calculate_trailing_return(group, 24),
            "LTD": calculate_twr(group)  # Link all returns
        })

    summary_df = pd.DataFrame(summary)
    # Sort by LTD descending
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


def main():
    # Fetch data
    df = fetch_active_positions()
    if df is None or df.empty:
        print("No data available or no active positions.")
        return

    # Calculate summary table
    summary_df = calculate_linked_returns(df)

    # Print the summary table in decimal format
    print("\nDecimal Format:")
    print(summary_df)

    # Print the summary table in percentage format
    summary_df_percentage = format_as_percentage(summary_df)
    print("\nPercentage Format:")
    print(summary_df_percentage)


if __name__ == "__main__":
    main()