
"""Single-factor security risk analysis using daily adjusted market returns."""

import argparse
import os
from datetime import date, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg
import statsmodels.api as sm
from dotenv import load_dotenv
from matplotlib.ticker import PercentFormatter


load_dotenv()

TRADING_DAYS_PER_YEAR = 252
DEFAULT_BENCHMARK = "SPY"


def get_connection():
    """Connect to the Investment Analytics PostgreSQL database."""
    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def get_open_positions() -> list[str]:
    """Return securities with positive latest end-of-day positions."""
    query = """
        WITH latest_positions AS (
            SELECT DISTINCT ON (instrument)
                instrument,
                eod_qty
            FROM core.position_qty_eod_v
            ORDER BY instrument, date DESC
        )
        SELECT instrument
        FROM latest_positions
        WHERE eod_qty > 0
        ORDER BY instrument;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]


def fetch_daily_returns(
    ticker: str,
    benchmark: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch and align daily adjusted returns on the benchmark calendar."""

    if ticker == benchmark:
        raise ValueError("Ticker and benchmark must be different.")

    query = """
        SELECT instrument, price_date, adjusted_close
        FROM source.market_data_daily_adjusted
        WHERE instrument IN (%s, %s)
          AND price_date BETWEEN %s AND %s
        ORDER BY price_date;
    """

    # Include prices before the analysis window for the first return.
    query_start = start_date - timedelta(days=15)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (ticker, benchmark, query_start, end_date),
            )
            rows = cur.fetchall()

    if not rows:
        raise ValueError("No market data available for these symbols.")

    prices = pd.DataFrame(
        rows,
        columns=["instrument", "price_date", "adjusted_close"],
    )

    prices["adjusted_close"] = pd.to_numeric(
        prices["adjusted_close"], errors="coerce"
    )

    panel = (
        prices.pivot(
            index="price_date",
            columns="instrument",
            values="adjusted_close",
        )
        .sort_index()
    )

    for symbol in (ticker, benchmark):
        if symbol not in panel.columns:
            raise ValueError(f"Missing market data for {symbol}.")

    # Benchmark trading dates serve as the daily calendar.
    calendar = panel[benchmark].dropna().index
    aligned_prices = panel.reindex(calendar)

    # Missing prices remain missing, avoiding forward-filled returns.
    daily_returns = aligned_prices[
        [ticker, benchmark]
    ].pct_change(fill_method=None)

    daily_returns = daily_returns.loc[
        (daily_returns.index >= start_date)
        & (daily_returns.index <= end_date)
    ].dropna()

    if len(daily_returns) < 60:
        raise ValueError(
            f"Insufficient overlapping observations: {len(daily_returns)}."
        )

    return daily_returns


def analyze_single_factor(
    returns: pd.DataFrame,
    ticker: str,
    benchmark: str,
    inference: str = "classic",
    hac_lags: int = 5,
):
    """Estimate market beta, intercept, and historical risk metrics."""

    y = returns[ticker]
    X = sm.add_constant(returns[benchmark])


    regression = sm.OLS(y, X)

    if inference == "classic":
        model = regression.fit()

    elif inference == "hac":
        if hac_lags < 0:
            raise ValueError("hac_lags must be nonnegative.")

        model = regression.fit(
            cov_type="HAC",
            cov_kwds={"maxlags": hac_lags},
        )

    else:
        raise ValueError("inference must be 'classic' or 'hac'")


    annualization = np.sqrt(TRADING_DAYS_PER_YEAR)

    metrics = {
        "observations": len(returns),
        "start_date": returns.index.min(),
        "end_date": returns.index.max(),
        "alpha_daily": float(model.params["const"]),
        "beta": float(model.params[benchmark]),
        "r_squared": float(model.rsquared),
        "correlation": float(y.corr(returns[benchmark])),
        "volatility": float(y.std(ddof=1) * annualization),
        "idiosyncratic_volatility": float(
            np.sqrt(model.mse_resid) * annualization
        ),
        "tracking_error": float(
            (y - returns[benchmark]).std(ddof=1) * annualization
        ),
    }

    return metrics, model


def plot_regression(
    returns: pd.DataFrame,
    model,
    ticker: str,
    benchmark: str,
):
    """Plot observed daily returns and the fitted regression line."""

    x = returns[benchmark]
    y = returns[ticker]

    x_line = np.linspace(x.min(), x.max(), 100)
    y_line = (
        model.params["const"]
        + model.params[benchmark] * x_line
    )

    fig, ax = plt.subplots(figsize=(9, 6))

    ax.scatter(x, y, alpha=0.4, label="Daily observations")
    ax.plot(x_line, y_line, linewidth=2, label="OLS regression")

    ax.set_title(f"{ticker} vs. {benchmark} — Daily Returns")
    ax.set_xlabel(f"{benchmark} Daily Return")
    ax.set_ylabel(f"{ticker} Daily Return")
    ax.xaxis.set_major_formatter(PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))

    ax.grid(alpha=0.2)
    ax.legend()

    fig.tight_layout()
    plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Single-factor security risk analysis."
    )

    parser.add_argument("--ticker", type=str)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--plot", action="store_true")

    args = parser.parse_args()

    if args.list:
        print("Open positions:", ", ".join(get_open_positions()))
        return

    if not args.ticker:
        parser.error("Provide --ticker or use --list.")

    if args.years < 1:
        parser.error("--years must be positive.")

    ticker = args.ticker.upper()
    benchmark = args.benchmark.upper()

    end_date = args.as_of
    start_date = (
        pd.Timestamp(end_date) - pd.DateOffset(years=args.years)
    ).date()

    returns = fetch_daily_returns(
        ticker, benchmark, start_date, end_date
    )

    metrics, model = analyze_single_factor(
        returns, ticker, benchmark
    )

    print(f"\n{ticker} vs. {benchmark} — Single-Factor Risk")
    print(f"Period: {metrics['start_date']} to {metrics['end_date']}")
    print(f"Observations: {metrics['observations']}")
    print(f"Daily Alpha: {metrics['alpha_daily']:+.5%}")
    print(f"Beta: {metrics['beta']:.4f}")
    print(f"R-squared: {metrics['r_squared']:.4f}")
    print(f"Correlation: {metrics['correlation']:.4f}")
    print(f"Annualized Volatility: {metrics['volatility']:.2%}")
    print(
        "Annualized Idiosyncratic Volatility: "
        f"{metrics['idiosyncratic_volatility']:.2%}"
    )
    print(f"Annualized Tracking Error: {metrics['tracking_error']:.2%}")

    if args.plot:
        plot_regression(returns, model, ticker, benchmark)


if __name__ == "__main__":
    main()


# From your repository root, run:
# python -m investment_analytics.risk.single_factor --ticker TOST --plot
# This runs the TOST vs. SPY regression over the default two-year period and displays the scatterplot.

# To see your available holdings:
# python -m investment_analytics.risk.single_factor --list