from datetime import datetime
import pandas as pd
import yfinance as yf
from fpdf import FPDF
import matplotlib.pyplot as plt
from io import BytesIO


def load_and_prepare_data(file_path, stock_symbol):
    """
    Loads and processes transaction data from a Robinhood `.csv` file for a given stock symbol.
    """
    df = pd.read_csv(file_path)
    df_stock = df[df['Instrument'].str.contains(stock_symbol, na=False)].copy()

    # Ensure consistent date parsing
    df_stock['Activity_Date'] = pd.to_datetime(
        df_stock['Activity Date'], errors='coerce'
    )
    df_stock = df_stock.dropna(subset=['Activity_Date'])

    # Clean and convert numerical columns
    df_stock['Amount'] = pd.to_numeric(
        df_stock['Amount'].replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True), errors='coerce'
    ).fillna(0)
    df_stock['Quantity'] = pd.to_numeric(df_stock['Quantity'], errors='coerce').fillna(0)
    df_stock['Price'] = pd.to_numeric(
        df_stock['Price'].replace({'\$': '', ',': ''}, regex=True), errors='coerce'
    ).fillna(0)

    # Categorize transactions and adjust cash flows
    df_stock['Event_Type'] = df_stock.apply(categorize_transaction, axis=1)
    df_stock['Adjusted_Amount'] = df_stock.apply(adjust_cash_flow_sign, axis=1)
    df_stock['Period'] = df_stock['Activity_Date'].dt.to_period('M')

    return df_stock


def categorize_transaction(row):
    """
    Categorizes transactions based on Robinhood's 'Trans Code' column.
    """
    transaction_map = {'Buy': 'Buy', 'Sell': 'Sell', 'CDIV': 'Dividend', 'SPL': 'Split'}
    return transaction_map.get(row['Trans Code'], 'Other')


def adjust_cash_flow_sign(row):
    """
    Adjusts the cash flow sign based on transaction type.
    """
    if row['Event_Type'] == 'Buy':
        return abs(row['Amount'])
    elif row['Event_Type'] == 'Sell':
        return -abs(row['Amount'])
    return row['Amount']


def calculate_cumulative_shares(data):
    """
    Calculates the cumulative number of shares held over time.
    """
    total_shares = 0
    cumulative_shares = []
    for _, row in data.iterrows():
        if row['Event_Type'] == 'Buy':
            total_shares += row['Quantity']
        elif row['Event_Type'] == 'Sell':
            total_shares -= row['Quantity']
        elif row['Event_Type'] == 'Split' and row['Quantity'] > 0:
            total_shares += row['Quantity']
        cumulative_shares.append(total_shares)
    data.loc[:, 'Cumulative_Shares'] = cumulative_shares
    return data


def fetch_historical_prices(stock_symbol, start_date):
    """
    Fetches historical stock prices using yfinance.
    """
    ticker = yf.Ticker(stock_symbol)
    hist = ticker.history(start=start_date, end=datetime.today())
    hist['As_of_Date'] = hist.index.to_period('M')
    return hist


def calculate_wcf_table(transactions, periods):
    """
    Calculates the Weighted Cash Flow table.
    """
    wcf_rows = []
    for period in periods:
        period_start = pd.Timestamp(period.start_time)
        period_end = pd.Timestamp(period.end_time)
        T = (period_end - period_start).days + 1
        period_transactions = transactions[transactions['Period'] == period]

        for _, row in period_transactions.iterrows():
            t_i = (row['Activity_Date'] - period_start).days + 1
            W_i = (T - t_i + 1) / T
            weighted_contribution = W_i * row['Adjusted_Amount']
            wcf_rows.append({
                'As_of_Date': period,
                'Activity_Date': row['Activity_Date'],
                'Event_Type': row['Event_Type'],
                'Amount': row['Adjusted_Amount'],
                'T (Days in Period)': T,
                't_i (Days Before Transaction)': t_i,
                'W_i (Weight)': W_i,
                'W_i * F_i (Weighted Contribution)': weighted_contribution
            })
    return pd.DataFrame(wcf_rows)


def calculate_equity_nav(df_equities, hist, all_periods):
    """
    Calculates the equity NAV (Net Asset Value) for each period.
    """
    ncf = df_equities[df_equities['Event_Type'].isin(['Buy', 'Sell'])].groupby('Period').agg(
        Net_CF=('Adjusted_Amount', 'sum')
    ).reset_index()

    equities_nav = pd.merge(all_periods, df_equities.groupby('Period').agg(
        Shares_EOM=('Cumulative_Shares', 'last')
    ).reset_index(), left_on='As_of_Date', right_on='Period', how='left')

    equities_nav['Shares_EOM'] = equities_nav['Shares_EOM'].fillna(method='ffill').fillna(0)
    equities_nav['Shares_BOM'] = equities_nav['Shares_EOM'].shift(1, fill_value=0)
    equities_nav['Shares'] = (equities_nav['Shares_BOM'] + equities_nav['Shares_EOM']) / 2

    equities_nav = pd.merge(equities_nav, hist.groupby('As_of_Date').agg(
        BOM_Price=('Close', 'first'),
        EOM_Price=('Close', 'last')
    ).reset_index(), on='As_of_Date', how='left')

    equities_nav['Equity_NAV_BOM'] = equities_nav['BOM_Price'] * equities_nav['Shares_BOM']
    equities_nav['Equity_NAV_EOM'] = equities_nav['EOM_Price'] * equities_nav['Shares_EOM']

    equities_nav = pd.merge(equities_nav, ncf, left_on='As_of_Date', right_on='Period', how='left')
    equities_nav['Net_CF'] = equities_nav['Net_CF'].fillna(0)
    return equities_nav


def calculate_metrics(equities_nav, wcf_table):
    """
    Calculates financial metrics including P&L, Average Capital, and Modified Dietz Return.
    """
    wcf_aggregated = wcf_table.groupby('As_of_Date').agg(
        WCF=('W_i * F_i (Weighted Contribution)', 'sum')
    ).reset_index()

    equities_nav = pd.merge(equities_nav, wcf_aggregated, on='As_of_Date', how='left')
    equities_nav['WCF'] = equities_nav['WCF'].fillna(0)
    equities_nav['P&L'] = equities_nav['Equity_NAV_EOM'] - equities_nav['Equity_NAV_BOM'] - equities_nav['Net_CF']
    equities_nav['Average_Capital'] = equities_nav['Equity_NAV_BOM'] + equities_nav['WCF']
    equities_nav['Modified_Dietz_Return'] = (
        equities_nav['P&L'] / equities_nav['Average_Capital']
    ).replace([float('inf'), -float('inf')], None)
    return equities_nav


def calculate_geometric_returns(equities_nav):
    """
    Adds a column for geometrically linked returns to the Asset Value Table.
    """
    geometric_returns = []
    cumulative_return = 1

    for md_return in equities_nav['Modified_Dietz_Return']:
        if pd.notnull(md_return):
            cumulative_return *= (1 + md_return)
        geometric_returns.append(cumulative_return - 1)

    equities_nav['Geometric_Linked_Return'] = geometric_returns
    return equities_nav



def generate_nav_chart(equities_nav):
    """
    Generates an area chart for NAV over time with cash flows represented as bars.

    Args:
        equities_nav (pd.DataFrame): The Equity NAV table containing NAV, cash flow, and date data.

    Returns:
        matplotlib.figure.Figure: The generated area chart with cash flows.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    # Area chart for NAV
    ax.fill_between(equities_nav['As_of_Date'].astype(str), equities_nav['Equity_NAV_EOM'], color="#00356B", alpha=0.7)
    ax.plot(equities_nav['As_of_Date'].astype(str), equities_nav['Equity_NAV_EOM'], color="#002E50", linewidth=2, label="NAV ($)")

    # Bar chart for Net Cash Flow
    ax.bar(equities_nav['As_of_Date'].astype(str), equities_nav['Net_CF'], color="#0072CE", alpha=0.6, label="Net Cash Flow ($)")

    # Formatting
    ax.set_title("NAV and Cash Flow Over Time", fontsize=14, color="#00356B")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Value ($)", fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.6)

    # Improve date readability: Limit the number of x-axis labels
    step = max(1, len(equities_nav) // 10)  # Show every 10th date for large datasets
    ax.set_xticks(equities_nav['As_of_Date'].astype(str)[::step])

    plt.tight_layout()
    return fig

def generate_geo_returns_chart(equities_nav):
    """
    Generates a line chart for geometrically linked returns over time.

    Args:
        equities_nav (pd.DataFrame): The Equity NAV table containing geometric linked returns and date data.

    Returns:
        matplotlib.figure.Figure: The generated chart for LTD returns.
    """
    fig, ax = plt.subplots(figsize=(12, 4))

    # Line chart for Geometric Linked Returns
    ax.plot(
        equities_nav['As_of_Date'].astype(str),
        equities_nav['Geometric_Linked_Return'],
        color="#00356B",
        linewidth=2,
        label="LTD Return"
    )

    # Formatting
    ax.set_title("LTD Return Over Time", fontsize=14, color="#00356B")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Cumulative Return", fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()

    # Improve date readability: Limit the number of x-axis labels
    step = max(1, len(equities_nav) // 10)  # Show every 10th date for large datasets
    ax.set_xticks(equities_nav['As_of_Date'].astype(str)[::step])

    plt.tight_layout()
    return fig




class PDFReport(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 12)
        self.cell(0, 10, 'Equity Analysis Report', align='C', new_x='LMARGIN', new_y='NEXT')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')


def generate_pdf_report(stock_symbol, equities_nav, wcf_table, nav_chart, geo_returns_chart):
    pdf = PDFReport()
    pdf.add_page()

    # Title Section
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, f"Investment Analysis Report for {stock_symbol}", align='C', ln=1)
    pdf.ln(10)

    # Summary Section
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, "Summary Metrics", ln=1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 10, f"Most Recent NAV: ${equities_nav['Equity_NAV_EOM'].iloc[-1]:.2f}", ln=1)
    pdf.cell(0, 10, f"Shares (EOM): {equities_nav['Shares_EOM'].iloc[-1]:.2f}", ln=1)
    pdf.cell(0, 10, f"EOM Price: ${equities_nav['EOM_Price'].iloc[-1]:.2f}", ln=1)
    pdf.cell(0, 10, f"As of Date: {equities_nav['As_of_Date'].iloc[-1]}", ln=1)
    pdf.cell(0, 10, f"LTD Return: {equities_nav['Geometric_Linked_Return'].iloc[-1] * 100:.2f}%", ln=1)
    pdf.ln(10)

    # NAV and Cash Flow Chart
    pdf.cell(0, 10, "NAV and Cash Flow Chart", ln=1)
    nav_image = BytesIO()
    nav_chart.savefig(nav_image, format='png')
    nav_image.seek(0)
    pdf.image(nav_image, x=10, y=None, w=190)
    pdf.ln(10)

    # LTD Returns Chart
    pdf.cell(0, 10, "LTD Returns Chart", ln=1)
    geo_image = BytesIO()
    geo_returns_chart.savefig(geo_image, format='png')
    geo_image.seek(0)
    pdf.image(geo_image, x=10, y=None, w=190)
    pdf.ln(10)

    # Save PDF
    pdf.output(f"/Users/claytonthompson/Desktop/Investment_Report_{stock_symbol}.pdf")
    print(f"PDF report generated: /Users/claytonthompson/Desktop/Investment_Report_{stock_symbol}.pdf")


def main(file_path, stock_symbol):
    df_stock = load_and_prepare_data(file_path, stock_symbol)
    df_equities = calculate_cumulative_shares(df_stock[df_stock['Event_Type'].isin(['Buy', 'Sell', 'Split'])])
    hist = fetch_historical_prices(stock_symbol, df_stock['Activity_Date'].min())
    all_periods = pd.DataFrame({'As_of_Date': hist['As_of_Date'].unique()})
    wcf_table = calculate_wcf_table(df_equities, all_periods['As_of_Date'])
    equities_nav = calculate_equity_nav(df_equities, hist, all_periods)
    equities_nav = calculate_metrics(equities_nav, wcf_table)
    equities_nav = calculate_geometric_returns(equities_nav)

    # Output results
    print("\nWeighted Cash Flow Table:")
    print(wcf_table)
    print(f"\nTransactions Table for {stock_symbol} (Equities):")
    print(df_equities[['Activity_Date', 'Event_Type', 'Quantity', 'Adjusted_Amount', 'Cumulative_Shares']])
    print(f"\nAsset Value Table for {stock_symbol} (Equities):")
    print(equities_nav[['As_of_Date', 'BOM_Price', 'EOM_Price', 'Shares_BOM', 'Shares_EOM',
                        'Equity_NAV_BOM', 'Equity_NAV_EOM', 'Net_CF', 'WCF',
                        'P&L', 'Average_Capital', 'Modified_Dietz_Return',
                        'Geometric_Linked_Return']])

    # Generate Charts
    nav_chart = generate_nav_chart(equities_nav)
    geo_returns_chart = generate_geo_returns_chart(equities_nav)

    # Generate PDF Report
    generate_pdf_report(stock_symbol, equities_nav, wcf_table, nav_chart, geo_returns_chart)

    


# Run the script
main('/Users/claytonthompson/Desktop/portfolio_tx.csv', 'AAPL')