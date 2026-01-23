#!/usr/bin/env python3

import os
import logging
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import statsmodels.api as sm
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple
import matplotlib.pyplot as plt

# -----------------------------------------------------------------------
# 1. Logging & Env Setup
# -----------------------------------------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Load environment variables from .env
load_dotenv()

# Read DB credentials from environment
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    """
    Create a psycopg2 connection to the Postgres database.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        raise


# -----------------------------------------------------------------------
# 2. Database Query to Get Open Positions
# -----------------------------------------------------------------------

def get_open_positions():
    """
    Fetches open positions from fifo_equity_lots, grouped by instrument.
    Returns a DataFrame with columns: [instrument, total_open_quantity, total_cost].
    """
    query = """
        SELECT
            instrument,
            SUM(open_quantity) AS total_open_quantity,
            SUM(total_cost)    AS total_cost
        FROM fifo_equity_lots
        WHERE open_quantity != 0
        GROUP BY instrument
        ORDER BY instrument;
    """

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        return df

    except Exception as e:
        logging.error("Error fetching open positions: %s", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()

# -----------------------------------------------------------------------
# 3. Market Data & Regression Helpers
# -----------------------------------------------------------------------

def fetch_daily_returns_db(symbol: str, start_date: datetime, end_date: datetime) -> pd.Series:
    query = """
        SELECT price_date, close_price FROM market_data
        WHERE instrument = %s AND price_date BETWEEN %s AND %s
        ORDER BY price_date;
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))
    df.set_index('price_date', inplace=True)
    return df['close_price'].pct_change().dropna()


def compute_instrument_beta_idio_vol(instrument_returns: pd.Series,
                                     market_returns: pd.Series) -> Tuple[float, float]:
    """
    Computes (beta, daily_idio_vol) by regressing instrument_returns on market_returns.
    daily_idio_vol is the standard deviation of residuals from the OLS regression.
    """
    data = pd.concat([instrument_returns, market_returns], axis=1).dropna()
    data.columns = ['r_instrument', 'r_market']

    if len(data) < 2:
        logging.warning("Not enough overlapping data for regression.")
        return np.nan, np.nan

    X = sm.add_constant(data['r_market'])
    y = data['r_instrument']
    model = sm.OLS(y, X).fit()

    beta = model.params['r_market']
    resid_std = model.resid.std()  # daily idio vol

    return beta, resid_std


def fetch_current_price_db(symbol: str) -> float:
    query = """
        SELECT close_price FROM market_data
        WHERE instrument = %s ORDER BY price_date DESC LIMIT 1;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            result = cur.fetchone()
    return float(result[0]) if result else np.nan


def get_portfolio_performance_data():
    """
    Fetch monthly portfolio-level performance from the view `portfolio_performance_view`,
    ordered by period_end_date.
    """
    query = """
        SELECT *
        FROM portfolio_performance_view
        ORDER BY period_end_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
        df["period_end_date"] = pd.to_datetime(df["period_end_date"])
        return df

def fetch_risk_free_rate():
    query = """
        SELECT yield_date, yield_percent
        FROM treasury_yields
        WHERE maturity = '3month' AND interval = 'monthly'
        ORDER BY yield_date
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
        df["yield_date"] = pd.to_datetime(df["yield_date"])
        df["monthly_rf"] = df["yield_percent"] / 100 / 12
        return df[["yield_date", "monthly_rf"]]
    

def calculate_sharpe_ratio(portfolio_returns_df: pd.DataFrame, rf_df: pd.DataFrame) -> float:
    df = pd.merge(
        portfolio_returns_df,
        rf_df,
        left_on="date",
        right_on="yield_date",
        how="inner"
    )
    df["excess_return"] = df["portfolio_return"] - df["monthly_rf"]

    annualized_return = df["excess_return"].mean() * 12
    annualized_vol = df["excess_return"].std() * np.sqrt(12)
    
    sharpe_ratio = annualized_return / annualized_vol if annualized_vol > 0 else np.nan
    return round(sharpe_ratio, 2)

# -----------------------------------------------------------------------
# 4. Main Calculation Logic
# -----------------------------------------------------------------------

def main():
    # Minimalist Matplotlib styling (Dieter Rams–inspired)
    plt.rcParams['figure.figsize'] = (10, 5)
    plt.rcParams['axes.facecolor'] = '#fafafa'
    plt.rcParams['axes.edgecolor'] = '#333333'
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.color'] = '#cccccc'
    plt.rcParams['grid.linestyle'] = ':'
    plt.rcParams['axes.spines.top'] = False
    plt.rcParams['axes.spines.right'] = False
    plt.rcParams['axes.spines.left'] = True
    plt.rcParams['axes.spines.bottom'] = True
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.titleweight'] = 'bold'
    plt.rcParams['legend.frameon'] = False

    # A) Get open positions from DB
    positions_df = get_open_positions()
    if positions_df.empty:
        logging.error("No open positions found in fifo_equity_lots.")
        return

    # B) Define date range for returns (2-year window)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365*2) # changing from 365 x 2 for 2-year to 1 year for Sharpe.. 

    # C) Fetch SPY returns for reference (market)
    spy_returns = fetch_daily_returns_db("SPY", start_date, end_date)
    if spy_returns.empty:
        logging.error("No market data (SPY) retrieved. Cannot proceed.")
        return

    market_vol = spy_returns.std()

    # D) For each instrument: compute Beta & daily idiosyncratic vol, get net market value, etc.
    results = []
    for idx, row in positions_df.iterrows():
        instrument = row['instrument']
        total_shares = float(row['total_open_quantity'])

        inst_returns = fetch_daily_returns_db(instrument, start_date, end_date)
        if inst_returns.empty:
            logging.warning("Skipping %s due to no return data.", instrument)
            continue

        beta, daily_idio_vol = compute_instrument_beta_idio_vol(inst_returns, spy_returns)

        curr_price = fetch_current_price_db(instrument)
        if np.isnan(curr_price):
            logging.warning("Skipping %s due to no current price.", instrument)
            continue

        net_market_value = total_shares * curr_price
        dollar_beta = beta * net_market_value
        dollar_idio_vol = daily_idio_vol * net_market_value

        results.append({
            'instrument': instrument,
            'shares': total_shares,
            'beta': beta,
            'daily_idio_vol': daily_idio_vol,
            'current_price': curr_price,
            'net_market_value': net_market_value,
            'dollar_beta': dollar_beta,
            'dollar_idio_vol': dollar_idio_vol
        })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        logging.error("No valid instrument data to compute portfolio metrics.")
        return

    portfolio_net_value = results_df['net_market_value'].sum()
    portfolio_dollar_beta = results_df['dollar_beta'].sum()
    portfolio_beta_decimal = portfolio_dollar_beta / portfolio_net_value if portfolio_net_value > 0 else np.nan
    portfolio_market_component_vol = portfolio_dollar_beta * market_vol
    portfolio_idio_var = (results_df['dollar_idio_vol'] ** 2).sum()
    portfolio_idio_vol = np.sqrt(portfolio_idio_var)
    portfolio_total_vol = np.sqrt(portfolio_market_component_vol**2 + portfolio_idio_vol**2)
    annual_tracking_vol_usd = portfolio_idio_vol * np.sqrt(252)
    tracking_error_pct = (annual_tracking_vol_usd / portfolio_net_value) * 100.0 if portfolio_net_value > 0 else np.nan

    logging.info("=== Instrument-Level Results ===")
    logging.info("\n%s", results_df.to_string(index=False))

    logging.info("\n=== Portfolio-Level Summary ===")
    logging.info("Total Net Market Value (USD):        %.2f", portfolio_net_value)
    logging.info("Portfolio Dollar Beta:              %.2f", portfolio_dollar_beta)
    logging.info("Portfolio Beta (decimal):           %.3f", portfolio_beta_decimal)
    logging.info("Daily Market Vol (decimal):         %.3f%%", (market_vol*100))
    logging.info("Market Component of Vol (USD):      %.2f", portfolio_market_component_vol)
    logging.info("Portfolio Idio Variance (USD^2):    %.2f", portfolio_idio_var)
    logging.info("Idiosyncratic Vol (USD) [Daily]:    %.2f", portfolio_idio_vol)
    logging.info("Total Portfolio Vol (USD) [Daily]:  %.2f", portfolio_total_vol)
    logging.info("---")
    logging.info("Daily Tracking Vol (USD):           %.2f", portfolio_idio_vol)
    logging.info("Annual Tracking Vol (USD):          %.2f", annual_tracking_vol_usd)
    logging.info("Tracking Error (%% of NMV):          %.2f%%", tracking_error_pct)

    # === SHARPE RATIO FROM MONTHLY PORTFOLIO RETURNS ===
    portfolio_perf = get_portfolio_performance_data()
    rf_df = fetch_risk_free_rate()

    if portfolio_perf.empty or rf_df.empty:
        logging.warning("Missing portfolio or treasury yield data for Sharpe ratio calc.")
    else:
        # Normalize treasury yield_date to end of month
        rf_df["period_end_date"] = rf_df["yield_date"] + pd.offsets.MonthEnd(0)

        df = pd.merge(
            portfolio_perf,
            rf_df,
            on="period_end_date",
            how="inner"
        )
        df["excess_return"] = df["md_return_net"] - df["monthly_rf"]

        # Full History Sharpe
        ann_return_full = df["excess_return"].mean() * 12
        ann_vol_full    = df["excess_return"].std() * np.sqrt(12)
        sharpe_full     = ann_return_full / ann_vol_full if ann_vol_full > 0 else np.nan

        # Trailing 12-Month Sharpe
        trailing_df = df.sort_values("period_end_date").tail(12)
        ann_return_trailing = trailing_df["excess_return"].mean() * 12
        ann_vol_trailing    = trailing_df["excess_return"].std() * np.sqrt(12)
        sharpe_trailing     = ann_return_trailing / ann_vol_trailing if ann_vol_trailing > 0 else np.nan

        logging.info("\n=== Sharpe Ratio Summary ===")
        logging.info(">> Full History")
        logging.info("Annualized Excess Return:          %.2f%%", ann_return_full * 100)
        logging.info("Annualized Volatility:            %.2f%%", ann_vol_full * 100)
        logging.info("Sharpe Ratio:                      %.2f", sharpe_full)

        logging.info(">> Trailing 12-Months")
        logging.info("Annualized Excess Return:          %.2f%%", ann_return_trailing * 100)
        logging.info("Annualized Volatility:            %.2f%%", ann_vol_trailing * 100)
        logging.info("Sharpe Ratio:                      %.2f", sharpe_trailing)

    

    # Visualization
    results_df.sort_values('net_market_value', ascending=False, inplace=True)
    instruments = results_df['instrument'].tolist()
    x_pos = np.arange(len(instruments))

    fig, ax = plt.subplots()
    ax.bar(x_pos, results_df['net_market_value'], color='steelblue', alpha=0.7)
    ax.set_title('Net Market Value by Instrument')
    ax.set_xlabel('Instrument')
    ax.set_ylabel('Net Mkt Value (USD)')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(instruments, rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    fig2, ax2 = plt.subplots()
    ax2.bar(x_pos, results_df['dollar_beta'], color='darkorange', alpha=0.7)
    ax2.set_title('Dollar Beta by Instrument')
    ax2.set_xlabel('Instrument')
    ax2.set_ylabel('Dollar Beta (USD Exposure)')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(instruments, rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# -----------------------------------------------------------------------
# 5. Script Entry Point
# -----------------------------------------------------------------------
if __name__ == "__main__":
    main()