import yfinance as yf
import pandas as pd
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging

# Configure logging (if not already configured in your project)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def fetch_daily_returns(ticker: str, start_date: datetime, end_date: datetime) -> pd.Series:
    """
    Downloads adjusted daily close prices for the given ticker from Yahoo Finance
    and returns a Series of daily percentage returns computed from the 'Close' column.
    
    Note:
        With auto_adjust=True (the default), the 'Close' column returned by yfinance 
        already reflects adjusted prices.
    
    Parameters:
        ticker (str): The stock ticker symbol (e.g., "AAPL").
        start_date (datetime): The start date for historical data.
        end_date (datetime): The end date for historical data.
    
    Returns:
        pd.Series: A series of daily percentage returns. If no data is returned,
                   an empty series is returned.
    """
    try:
        # Download historical data. auto_adjust is True by default.
        df = yf.download(ticker, start=start_date, end=end_date)
    except Exception as e:
        logging.error("Error downloading data for %s: %s", ticker, e)
        return pd.Series(dtype=float)
    
    if df.empty:
        logging.warning("No price data returned for ticker: %s", ticker)
        return pd.Series(dtype=float)
    
    # Compute daily percentage returns using the adjusted 'Close' column
    returns = df['Close'].pct_change().dropna()
    return returns


def run_regression_and_plot(df: pd.DataFrame, x_col: str, y_col: str,
                            start_date: datetime, end_date: datetime) -> None:
    """
    Runs a linear OLS regression y = alpha + beta * x on the specified columns of df
    and then plots the scatter and regression line in a minimalist style.
    """
    if df.empty or len(df) < 2:
        print("[Error] Not enough data for regression.")
        return

    # Prepare regression variables
    X = sm.add_constant(df[x_col])  # add a column of 1s (intercept)
    y = df[y_col]

    # Fit OLS model
    model = sm.OLS(y, X).fit()
    alpha = model.params['const']
    beta = model.params[x_col]

    print("\nRegression Results:")
    print(f"  Alpha (Intercept): {alpha:.6f}")
    print(f"  Beta (Slope):      {beta:.6f}")
    print(model.summary())  # Optional: see more details

    # ---- Minimalist / Dieter Rams–Inspired Plotting Style ----
    plt.rcParams['figure.figsize'] = (8, 6)
    plt.rcParams['figure.dpi'] = 100
    plt.rcParams['axes.facecolor'] = '#fafafa'  # Light background
    plt.rcParams['axes.edgecolor'] = '#333333'
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.color'] = '#cccccc'
    plt.rcParams['grid.linestyle'] = ':'
    plt.rcParams['grid.alpha'] = 0.8
    plt.rcParams['axes.spines.top'] = False
    plt.rcParams['axes.spines.right'] = False
    plt.rcParams['axes.spines.left'] = True
    plt.rcParams['axes.spines.bottom'] = True
    plt.rcParams['legend.frameon'] = False
    plt.rcParams['font.size'] = 11
    plt.rcParams['axes.titleweight'] = 'bold'

    # Create figure & axis
    fig, ax = plt.subplots()

    # Scatter plot of the returns
    ax.scatter(df[x_col], df[y_col],
               color='steelblue', alpha=0.5,
               label='Daily Observations')

    # Regression line
    x_vals = np.linspace(df[x_col].min(), df[x_col].max(), 100)
    y_vals = alpha + beta * x_vals
    ax.plot(x_vals, y_vals,
            color='darkorange',
            linewidth=2,
            label='Regression Line')

    # Title / Axis Labels
    title_str = (
        f"Linear Regression of Toast (TOST) vs. SPY Daily Returns\n"
        f"({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})"
    )
    ax.set_title(title_str, fontsize=12)
    ax.set_xlabel("SPY Daily Return")
    ax.set_ylabel("TOST Daily Return")
    ax.legend(loc='best')

    plt.show()


def main():
    # 1. Define our start and end dates for ~2 years
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365 * 2)

    # 2. Fetch daily returns for TOST and SPY
    tost_returns = fetch_daily_returns("TOST", start_date, end_date)
    spy_returns = fetch_daily_returns("SPY", start_date, end_date)

    # 3. Merge into a single DataFrame
    df = pd.concat([tost_returns, spy_returns], axis=1)
    df.columns = ['R_TOST', 'R_SPY']
    df = df.dropna()

    # Quick check on data
    print("Merged DataFrame (first 5 rows):")
    print(df.head())
    print(f"Total rows after merging: {len(df)}")

    if len(df) < 2:
        print("[Error] Not enough merged data for regression.")
        return

    # 4. Run Regression (R_TOST = alpha + beta * R_SPY) and Plot
    run_regression_and_plot(df, x_col='R_SPY', y_col='R_TOST',
                            start_date=start_date, end_date=end_date)


# If running this file directly, execute main()
if __name__ == "__main__":
    main()