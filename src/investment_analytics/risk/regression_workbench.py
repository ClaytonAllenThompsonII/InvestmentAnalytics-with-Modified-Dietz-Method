
"""Interactive regression workbench for investment risk research."""

import argparse
from datetime import date

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from matplotlib.ticker import PercentFormatter

from investment_analytics.risk.single_factor import (
    get_open_positions,
    fetch_daily_returns,
    analyze_single_factor,
)


def select_ticker() -> str | None:
    """Select an active holding or enter a ticker manually."""

    positions = get_open_positions()

    print("\n" + "=" * 60)
    print("INVESTMENT ANALYTICS — REGRESSION WORKBENCH")
    print("=" * 60)

    if positions:
        print("\nAvailable Open Positions:\n")

        for idx, ticker in enumerate(positions, start=1):
            print(f"  {idx:2d}. {ticker}")

    print("\nSelect a number or enter any ticker symbol.")
    print("Enter Q to quit.\n")

    while True:
        choice = input("Ticker selection: ").strip().upper()

        if choice == "Q":
            return None

        if choice.isdigit():
            index = int(choice) - 1

            if 0 <= index < len(positions):
                return positions[index]

            print("Invalid selection. Try again.")
            continue

        if choice:
            return choice

        print("Please enter a ticker or selection number.")


def display_regression_results(
    ticker: str,
    benchmark: str,
    metrics: dict,
    model,
) -> None:
    """Print the complete OLS statistical summary and risk metrics."""

    print("\n" + "=" * 72)
    print(f"REGRESSION ANALYSIS: {ticker} vs. {benchmark}")
    print("=" * 72)

    print(
        f"\nSample: {metrics['start_date']} "
        f"to {metrics['end_date']}"
    )
    print(f"Observations: {metrics['observations']}")
    print("Frequency: Daily")
    print("Return Series: Adjusted Close")
    print("Estimator: Ordinary Least Squares (OLS)")

    print("\n" + "-" * 72)
    print("STATISTICAL REGRESSION OUTPUT")
    print("-" * 72)

    print(f"Covariance Estimator: {model.cov_type}")

    if model.cov_type == "HAC":
        print(f"Newey-West Max Lags: {model.cov_kwds['maxlags']}")

    print(model.summary())

    confidence_interval = model.conf_int().loc[benchmark]

    print("\n" + "-" * 72)
    print("ADDITIONAL RISK METRICS")
    print("-" * 72)

    print(f"Market Beta:                 {metrics['beta']:>12.4f}")
    print(
        f"Beta 95% Confidence Interval: "
        f"[{confidence_interval.iloc[0]:.4f}, "
        f"{confidence_interval.iloc[1]:.4f}]"
    )
    print(f"Daily Alpha (Intercept):     {metrics['alpha_daily']:>12.4%}")
    print(f"Correlation:                 {metrics['correlation']:>12.4f}")
    print(f"R-Squared:                   {metrics['r_squared']:>12.4f}")
    print(f"Annualized Volatility:       {metrics['volatility']:>12.2%}")
    print(
        f"Idiosyncratic Volatility:    "
        f"{metrics['idiosyncratic_volatility']:>12.2%}"
    )
    print(
        f"Annualized Tracking Error:   "
        f"{metrics['tracking_error']:>12.2%}"
    )

    print("\n")


def plot_diagnostics(
    returns: pd.DataFrame,
    model,
    ticker: str,
    benchmark: str,
) -> None:
    """Display regression fit and residual diagnostic charts."""

    x = returns[benchmark]
    y = returns[ticker]

    x_line = np.linspace(x.min(), x.max(), 100)
    y_line = (
        model.params["const"]
        + model.params[benchmark] * x_line
    )

    fig, axes = plt.subplots(1, 3, figsize=(17, 5))

    # 1. Observed returns and fitted regression
    ax = axes[0]

    ax.scatter(
        x, y,
        alpha=0.4,
        s=20,
        label="Daily observations",
    )
    ax.plot(
        x_line, y_line,
        linewidth=2,
        label="OLS fitted line",
    )

    ax.set_title("Market Model Regression")
    ax.set_xlabel(f"{benchmark} Daily Return")
    ax.set_ylabel(f"{ticker} Daily Return")
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.legend()
    ax.grid(alpha=0.2)

    # 2. Residuals versus fitted values
    ax = axes[1]

    ax.scatter(
        model.fittedvalues,
        model.resid,
        alpha=0.4,
        s=20,
    )
    ax.axhline(0, linestyle="--", linewidth=1)

    ax.set_title("Residuals vs. Fitted Values")
    ax.set_xlabel("Fitted Daily Return")
    ax.set_ylabel("Regression Residual")
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(alpha=0.2)

    # 3. Residual normality diagnostic
    sm.qqplot(
        model.resid,
        line="s",
        ax=axes[2],
    )

    axes[2].set_title("Residual Q-Q Plot")
    axes[2].grid(alpha=0.2)

    fig.suptitle(
        f"{ticker} vs. {benchmark} | "
        f"{returns.index.min()} to {returns.index.max()}",
        fontsize=14,
        fontweight="bold",
    )

    fig.tight_layout()
    plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive statistical regression workbench."
    )

    parser.add_argument("--ticker", type=str, default=None)
    parser.add_argument("--benchmark", type=str, default="SPY")
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.today(),
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Print statistical results without charts.",
    )

    parser.add_argument(
        "--inference",
        choices=["classic", "hac"],
        default="classic",
        help="Standard-error method: classical OLS or Newey-West HAC.",
    )

    parser.add_argument(
        "--hac-lags",
        type=int,
        default=5,
        help="Maximum autocorrelation lags for Newey-West HAC.",
    )

    args = parser.parse_args()

    if args.years < 1:
        parser.error("--years must be at least 1.")

    ticker = (
        args.ticker.upper()
        if args.ticker
        else select_ticker()
    )

    if ticker is None:
        print("Exiting regression workbench.")
        return

    benchmark = args.benchmark.upper()

    start_date = (
        pd.Timestamp(args.as_of)
        - pd.DateOffset(years=args.years)
    ).date()

    returns = fetch_daily_returns(
        ticker=ticker,
        benchmark=benchmark,
        start_date=start_date,
        end_date=args.as_of,
    )

    metrics, model = analyze_single_factor(
        returns=returns,
        ticker=ticker,
        benchmark=benchmark,
        inference=args.inference,
        hac_lags=args.hac_lags,
    )

    display_regression_results(
        ticker, benchmark, metrics, model
    )

    if not args.no_plot:
        plot_diagnostics(
            returns, model, ticker, benchmark
        )


if __name__ == "__main__":
    main()


## Classical OLS (default)
## python -m investment_analytics.risk.regression_workbench --ticker TOST

## Newey-West HAC with five lags
## python -m investment_analytics.risk.regression_workbench --ticker TOST --inference hac

## Newey-West HAC with ten lags
## python -m investment_analytics.risk.regression_workbench --ticker TOST --inference hac --hac-lags 10

## You can still omit --ticker and select your security interactively. The three diagnostic charts will continue working without modification.

