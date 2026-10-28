
"""Security-level and holdings-implied portfolio risk analytics."""

from datetime import date

import numpy as np
import pandas as pd
import statsmodels.api as sm

from investment_analytics.data_access.risk import (
    get_daily_return_panel,
    get_equity_exposures,
    get_latest_benchmark_date,
)
from investment_analytics.risk.single_factor import (
    analyze_single_factor,
    fetch_daily_returns,
)


TRADING_DAYS_PER_YEAR = 252
DEFAULT_BENCHMARK = "SPY"
MIN_OBSERVATIONS = 60


# ---------------------------------------------------------------------
# SECURITY-LEVEL RISK
# ---------------------------------------------------------------------

def build_security_risk_matrix(
    returns: pd.DataFrame,
    exposures: pd.DataFrame,
    benchmark: str = DEFAULT_BENCHMARK,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Estimate security-level market risk and current dollar exposures.

    Returns
    -------
    risk_matrix:
        One row per current equity position.

    residual_panel:
        Dated OLS residual returns, retained for portfolio covariance.
    """

    if benchmark not in returns.columns:
        raise ValueError(f"Missing benchmark returns: {benchmark}")

    total_nmv = float(exposures["market_value"].sum())

    if total_nmv <= 0:
        raise ValueError("Total equity NMV must be positive.")

    benchmark_obs = int(returns[benchmark].notna().sum())

    rows = []
    residuals = {}

    for holding in exposures.itertuples(index=False):

        ticker = holding.instrument
        nmv = float(holding.market_value)
        weight = nmv / total_nmv

        metrics = None

        if ticker in returns.columns and ticker != benchmark:

            pair = returns[[ticker, benchmark]].dropna()
            observations = len(pair)

            if observations >= MIN_OBSERVATIONS:

                metrics, model = analyze_single_factor(
                    returns=pair,
                    ticker=ticker,
                    benchmark=benchmark,
                )

                residuals[ticker] = pd.Series(
                    model.resid,
                    index=pair.index,
                    dtype=float,
                    name=ticker,
                )

        else:
            observations = 0

        coverage = (
            observations / benchmark_obs
            if benchmark_obs > 0 else 0.0
        )

        if metrics is None:
            beta = np.nan
            daily_idio_vol = np.nan
            annual_idio_vol = np.nan
            annual_total_vol = np.nan
            r_squared = np.nan
            status = "Insufficient history"

        else:
            beta = metrics["beta"]
            annual_idio_vol = metrics["idiosyncratic_volatility"]
            daily_idio_vol = (
                annual_idio_vol / np.sqrt(TRADING_DAYS_PER_YEAR)
            )
            annual_total_vol = metrics["volatility"]
            r_squared = metrics["r_squared"]

            status = (
                "Partial history"
                if coverage < 0.95 else "OK"
            )

        rows.append({
            "ticker": ticker,
            "shares": float(holding.shares),
            "nmv": nmv,
            "weight": weight,
            "beta": beta,
            "dollar_beta": nmv * beta,
            "idio_vol_daily": daily_idio_vol,
            "idio_vol_ann": annual_idio_vol,
            "dollar_idio_vol_daily": nmv * daily_idio_vol,
            "volatility_ann": annual_total_vol,
            "r_squared": r_squared,
            "observations": observations,
            "coverage": coverage,
            "status": status,
        })

    matrix = (
        pd.DataFrame(rows)
        .sort_values("nmv", ascending=False)
        .reset_index(drop=True)
    )

    residual_panel = pd.DataFrame(residuals).sort_index()

    return matrix, residual_panel


# ---------------------------------------------------------------------
# PORTFOLIO-LEVEL RISK
# ---------------------------------------------------------------------

def calculate_portfolio_risk(
    risk_matrix: pd.DataFrame,
    residual_panel: pd.DataFrame,
    returns: pd.DataFrame,
    benchmark: str = DEFAULT_BENCHMARK,
) -> dict:
    """
    Calculate holdings-implied portfolio risk.

    Compare independent-residual and full-covariance estimates
    using the same common sample of regression residuals.
    """

    if risk_matrix.empty:
        raise ValueError("Risk matrix is empty.")

    invalid = risk_matrix[
        risk_matrix["beta"].isna()
        | risk_matrix["idio_vol_daily"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "Cannot aggregate risk with unmodeled holdings: "
            f"{invalid['ticker'].tolist()}"
        )

    # Current long-equity exposures
    portfolio_nmv = float(risk_matrix["nmv"].sum())
    dollar_beta = float(risk_matrix["dollar_beta"].sum())
    portfolio_beta = dollar_beta / portfolio_nmv

    weights = (
        risk_matrix.set_index("ticker")["weight"]
        .astype(float)
    )

    np.testing.assert_allclose(
        weights.sum(), 1.0, atol=1e-10
    )

    # Benchmark volatility over the selected estimation window
    market_returns = returns[benchmark].dropna()

    if len(market_returns) < MIN_OBSERVATIONS:
        raise ValueError("Insufficient benchmark return history.")

    market_vol_daily = float(market_returns.std(ddof=1))
    market_vol_ann = (
        market_vol_daily * np.sqrt(TRADING_DAYS_PER_YEAR)
    )

    # Portfolio systematic exposure
    portfolio_market_vol_daily = (
        abs(portfolio_beta) * market_vol_daily
    )

    portfolio_market_vol_ann = (
        portfolio_market_vol_daily
        * np.sqrt(TRADING_DAYS_PER_YEAR)
    )

    # Independent-residual risk using each security's own
    # available-history regression (up to the selected lookback).
    #
    # This is the original portfolio risk methodology.
    # It does not require a common historical return window.

    security_idio_daily = (
        risk_matrix.set_index("ticker")
        .reindex(weights.index)["idio_vol_daily"]
        .astype(float)
    )

    portfolio_idio_daily_available_history = float(
        np.sqrt(
            np.sum(
                (weights * security_idio_daily) ** 2
            )
        )
    )

    portfolio_idio_ann_available_history = (
        portfolio_idio_daily_available_history
        * np.sqrt(TRADING_DAYS_PER_YEAR)
    )

    portfolio_total_vol_ann_available_history = float(
        np.hypot(
            portfolio_market_vol_ann,
            portfolio_idio_ann_available_history,
        )
    )


    # Align all residuals on a single common observation sample
    common_residuals = (
        residual_panel
        .reindex(columns=weights.index)
        .dropna(how="any")
    )

    common_obs = len(common_residuals)

    if common_obs < MIN_OBSERVATIONS:
        raise ValueError(
            "Insufficient common residual observations "
            f"for covariance estimation: {common_obs}. "
            "Review securities with limited price history."
        )

    # Sample residual covariance matrix (daily return units)
    residual_cov = common_residuals.cov()

    w = weights.to_numpy(dtype=float)
    sigma = residual_cov.to_numpy(dtype=float)

    # Original simplifying assumption:
    # Residuals are mutually uncorrelated.
    independent_idio_variance = float(
        np.sum((w ** 2) * np.diag(sigma))
    )

    # Full covariance estimate:
    # Includes cross-security residual covariance.
    covariance_idio_variance = float(
        w @ sigma @ w
    )

    if min(independent_idio_variance, covariance_idio_variance) < -1e-12:
        raise ValueError("Invalid negative residual variance.")

    portfolio_idio_daily_independent = np.sqrt(
        max(0.0, independent_idio_variance)
    )

    portfolio_idio_daily_covariance = np.sqrt(
        max(0.0, covariance_idio_variance)
    )

    annualization = np.sqrt(TRADING_DAYS_PER_YEAR)

    portfolio_idio_ann_independent = (
        portfolio_idio_daily_independent * annualization
    )

    portfolio_idio_ann_covariance = (
        portfolio_idio_daily_covariance * annualization
    )

    # One-factor total-volatility model
    portfolio_total_vol_daily = np.hypot(
        portfolio_market_vol_daily,
        portfolio_idio_daily_covariance,
    )

    portfolio_total_vol_ann = (
        portfolio_total_vol_daily * annualization
    )

    # Difference caused by residual cross-covariance
    residual_cross_covariance = (
        covariance_idio_variance
        - independent_idio_variance
    )

    return {
        "portfolio_nmv": portfolio_nmv,
        "portfolio_beta": portfolio_beta,
        "dollar_beta": dollar_beta,

        "market_vol_daily": market_vol_daily,
        "market_vol_ann": market_vol_ann,

        "portfolio_market_vol_daily": portfolio_market_vol_daily,
        "portfolio_market_vol_ann": portfolio_market_vol_ann,

        # Common-date residual risk using available-history regressions
        "portfolio_idio_vol_daily_independent":
            portfolio_idio_daily_independent,
        "portfolio_idio_vol_daily_covariance":
            portfolio_idio_daily_covariance,

        "portfolio_idio_vol_ann_independent":
            portfolio_idio_ann_independent,
        "portfolio_idio_vol_ann_covariance":
            portfolio_idio_ann_covariance,
        
        # Available-history independent-residual model
        "portfolio_idio_vol_daily_available_history":
            portfolio_idio_daily_available_history,
        "portfolio_idio_vol_ann_available_history":
            portfolio_idio_ann_available_history,
        "portfolio_total_vol_ann_available_history":
            portfolio_total_vol_ann_available_history,

        # Mixed-window covariance-adjusted total risk    
        "portfolio_total_vol_daily": portfolio_total_vol_daily,
        "portfolio_total_vol_ann": portfolio_total_vol_ann,

        "residual_cross_covariance_daily":
            residual_cross_covariance,

        # Estimation sample metadata
        "common_observations": common_obs,
        "common_start_date": common_residuals.index.min().date(),
        "common_end_date": common_residuals.index.max().date(),
        "benchmark_observations": len(market_returns),
    }


# ---------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------

def validate_against_single_factor(
    ticker: str,
    benchmark: str,
    start_date: date,
    end_date: date,
    returns: pd.DataFrame,
    risk_matrix: pd.DataFrame,
) -> None:
    """Reconcile batch inputs and estimates with single_factor.py."""

    reference_returns = fetch_daily_returns(
        ticker=ticker,
        benchmark=benchmark,
        start_date=start_date,
        end_date=end_date,
    )

    reference_returns.index = pd.to_datetime(
        reference_returns.index
    )

    batch_returns = returns[[ticker, benchmark]].dropna()

    pd.testing.assert_frame_equal(
        batch_returns,
        reference_returns,
        check_freq=False,
        rtol=1e-12,
        atol=1e-12,
    )

    reference_metrics, _ = analyze_single_factor(
        returns=reference_returns,
        ticker=ticker,
        benchmark=benchmark,
    )

    actual = risk_matrix.loc[
        risk_matrix["ticker"].eq(ticker)
    ].iloc[0]

    np.testing.assert_allclose(
        actual["beta"],
        reference_metrics["beta"],
        rtol=1e-10,
    )

    np.testing.assert_allclose(
        actual["idio_vol_ann"],
        reference_metrics["idiosyncratic_volatility"],
        rtol=1e-10,
    )

    print(
        f"PASS — {ticker}/{benchmark}: "
        "returns, beta, and residual volatility reconcile."
    )


def validate_portfolio_risk(summary: dict) -> None:
    """Verify one-factor aggregation identities."""

    np.testing.assert_allclose(
        summary["portfolio_total_vol_ann"] ** 2,
        summary["portfolio_market_vol_ann"] ** 2
        + summary["portfolio_idio_vol_ann_covariance"] ** 2,
        rtol=1e-10,
        atol=1e-12,
    )

    print("PASS — Portfolio total-volatility decomposition.")



# ---------------------------------------------------------------------
# COMMON-SAMPLE PORTFOLIO VALIDATION
# ---------------------------------------------------------------------

def validate_common_sample_portfolio(
    returns: pd.DataFrame,
    risk_matrix: pd.DataFrame,
    residual_panel: pd.DataFrame,
    summary: dict,
    benchmark: str = DEFAULT_BENCHMARK,
) -> dict:
    """
    Independently validate holdings-implied portfolio volatility.

    Uses a common historical sample and fixed current equity weights.

    Compares:
      1. Direct portfolio return volatility
      2. Security-return covariance volatility
      3. Same-window one-factor volatility decomposition

    The synthetic portfolio is rebalanced daily to fixed weights.
    """

    tickers = risk_matrix["ticker"].tolist()

    weights = (
        risk_matrix.set_index("ticker")
        .loc[tickers, "weight"]
        .to_numpy(dtype=float)
    )

    # Require observations available for every security and SPY.
    common = (
        returns[[benchmark, *tickers]]
        .dropna(how="any")
        .sort_index()
    )

    # Confirm this is the same date sample used by our
    # existing residual covariance calculation.
    original_residuals = (
        residual_panel
        .reindex(columns=tickers)
        .dropna(how="any")
        .sort_index()
    )

    if not common.index.equals(original_residuals.index):
        raise AssertionError(
            "Common return and residual dates do not match."
        )

    if len(common) < MIN_OBSERVATIONS:
        raise ValueError(
            "Insufficient common observations for validation."
        )

    annualization = np.sqrt(TRADING_DAYS_PER_YEAR)

    # --------------------------------------------------------------
    # METHOD 1: Direct synthetic portfolio returns
    # --------------------------------------------------------------

    portfolio_returns = (
        common[tickers] @ weights
    )

    direct_vol_ann = float(
        portfolio_returns.std(ddof=1) * annualization
    )

    # --------------------------------------------------------------
    # METHOD 2: Direct security-return covariance
    # --------------------------------------------------------------

    security_cov = common[tickers].cov().to_numpy()

    covariance_vol_ann = float(
        np.sqrt(weights @ security_cov @ weights)
        * annualization
    )

    np.testing.assert_allclose(
        direct_vol_ann,
        covariance_vol_ann,
        rtol=1e-10,
        atol=1e-12,
    )

    # --------------------------------------------------------------
    # METHOD 3: Re-estimate all regressions on the common sample
    # --------------------------------------------------------------

    market_returns = common[benchmark]

    portfolio_model = sm.OLS(
        portfolio_returns,
        sm.add_constant(market_returns),
    ).fit()

    portfolio_beta_common = float(
        portfolio_model.params[benchmark]
    )

    portfolio_alpha_common = float(
        portfolio_model.params["const"]
    )

    # Re-estimate each security using these exact same dates.
    security_betas = []
    security_alphas = []
    common_residuals = pd.DataFrame(index=common.index)

    for ticker in tickers:

        model = sm.OLS(
            common[ticker],
            sm.add_constant(market_returns),
        ).fit()

        security_betas.append(
            float(model.params[benchmark])
        )

        security_alphas.append(
            float(model.params["const"])
        )

        common_residuals[ticker] = model.resid

    # The portfolio's OLS coefficients must equal the
    # weighted sum of security OLS coefficients.
    weighted_beta = float(
        weights @ np.asarray(security_betas)
    )

    weighted_alpha = float(
        weights @ np.asarray(security_alphas)
    )

    np.testing.assert_allclose(
        portfolio_beta_common,
        weighted_beta,
        rtol=1e-10,
        atol=1e-12,
    )

    np.testing.assert_allclose(
        portfolio_alpha_common,
        weighted_alpha,
        rtol=1e-10,
        atol=1e-12,
    )

    # Common-window market component
    market_vol_ann = float(
        market_returns.std(ddof=1) * annualization
    )

    systematic_vol_ann = (
        abs(portfolio_beta_common) * market_vol_ann
    )

    # Common-window residual covariance
    residual_cov = common_residuals.cov().to_numpy()

    idio_cov_ann = float(
        np.sqrt(max(0.0, weights @ residual_cov @ weights))
        * annualization
    )

    # Original independent-residual assumption,
    # recalculated on the identical common sample.
    idio_independent_ann = float(
        np.sqrt(
            np.sum(
                weights ** 2 * np.diag(residual_cov)
            )
        ) * annualization
    )

    factor_total_vol_ann = float(
        np.hypot(systematic_vol_ann, idio_cov_ann)
    )

    # Same-window OLS residuals should be orthogonal to
    # market returns. Therefore, total volatility reconciles.
    np.testing.assert_allclose(
        direct_vol_ann,
        factor_total_vol_ann,
        rtol=1e-10,
        atol=1e-12,
    )

    # Additional check: independently fitted portfolio residual
    # volatility equals covariance-aggregated security residuals.
    portfolio_residual_vol_ann = float(
        portfolio_model.resid.std(ddof=1)
        * annualization
    )

    np.testing.assert_allclose(
        portfolio_residual_vol_ann,
        idio_cov_ann,
        rtol=1e-10,
        atol=1e-12,
    )

    # --------------------------------------------------------------
    # REPORT
    # --------------------------------------------------------------

    print("\nCOMMON-SAMPLE PORTFOLIO VALIDATION")
    print("-" * 58)

    print(f"Observations:                  {len(common)}")
    print(
        f"Sample:                        "
        f"{common.index.min().date()} to "
        f"{common.index.max().date()}"
    )

    print("\nVOLATILITY RECONCILIATION")

    print(
        f"Direct Portfolio Vol (Ann.):   "
        f"{direct_vol_ann:.2%}"
    )
    print(
        f"Security Covariance (Ann.):    "
        f"{covariance_vol_ann:.2%}"
    )
    print(
        f"One-Factor Model (Ann.):       "
        f"{factor_total_vol_ann:.2%}"
    )

    print("\nCOMMON-WINDOW FACTOR COMPONENTS")

    print(
        f"Portfolio Beta:                "
        f"{portfolio_beta_common:.4f}"
    )
    print(
        f"Market Vol (Ann.):             "
        f"{systematic_vol_ann:.2%}"
    )
    print(
        f"Idio Vol - Independent:        "
        f"{idio_independent_ann:.2%}"
    )
    print(
        f"Idio Vol - Covariance:         "
        f"{idio_cov_ann:.2%}"
    )

    print("\nESTIMATION WINDOW COMPARISON")

    print(
        f"Original Portfolio Beta:       "
        f"{summary['portfolio_beta']:.4f}"
    )
    print(
        f"Common-Window Beta:            "
        f"{portfolio_beta_common:.4f}"
    )

    print(
        f"Original Idio Vol (Ann.):      "
        f"{summary['portfolio_idio_vol_ann_covariance']:.2%}"
    )
    print(
        f"Common-Window Idio (Ann.):     "
        f"{idio_cov_ann:.2%}"
    )

    print(
        f"Original Total Vol (Ann.):     "
        f"{summary['portfolio_total_vol_ann']:.2%}"
    )
    print(
        f"Common-Window Total (Ann.):    "
        f"{direct_vol_ann:.2%}"
    )

    print("\nPASS — Direct and covariance volatility reconcile.")
    print("PASS — Common-window factor decomposition reconciles.")
    print("PASS — Weighted security and portfolio OLS agree.")

    return {
        "observations": len(common),
        "direct_vol_ann": direct_vol_ann,
        "covariance_vol_ann": covariance_vol_ann,
        "factor_total_vol_ann": factor_total_vol_ann,
        "portfolio_beta_common": portfolio_beta_common,
        "systematic_vol_ann": systematic_vol_ann,
        "idio_independent_ann": idio_independent_ann,
        "idio_cov_ann": idio_cov_ann,
    }

# ---------------------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------------------

def main() -> None:

    benchmark = DEFAULT_BENCHMARK

    end_date = get_latest_benchmark_date(benchmark)

    start_date = (
        pd.Timestamp(end_date)
        - pd.DateOffset(years=2)
    ).date()

    exposures = get_equity_exposures(end_date)

    returns = get_daily_return_panel(
        symbols=exposures["instrument"].tolist(),
        start_date=start_date,
        end_date=end_date,
        benchmark=benchmark,
    )

    matrix, residual_panel = build_security_risk_matrix(
        returns=returns,
        exposures=exposures,
        benchmark=benchmark,
    )

    print("\nSECURITY RISK MATRIX")
    print(f"As of: {end_date}")
    print(f"Lookback: {start_date} to {end_date}")
    print(f"Benchmark: {benchmark}\n")

    columns = [
        "ticker",
        "nmv",
        "weight",
        "beta",
        "idio_vol_daily",
        "dollar_beta",
        "observations",
        "status",
    ]

    formatters = {
        "nmv": lambda x: f"${x:,.0f}",
        "weight": lambda x: f"{x:.2%}",
        "beta": lambda x: f"{x:.3f}",
        "idio_vol_daily": lambda x: f"{x:.2%}",
        "dollar_beta": lambda x: f"${x:,.0f}",
    }

    print(
        matrix[columns].to_string(
            index=False,
            formatters=formatters,
        )
    )

    summary = calculate_portfolio_risk(
        risk_matrix=matrix,
        residual_panel=residual_panel,
        returns=returns,
        benchmark=benchmark,
    )

    print("\nPORTFOLIO RISK OVERVIEW")
    print(f"Portfolio NMV:                  ${summary['portfolio_nmv']:,.2f}")
    print(f"Portfolio Beta:                 {summary['portfolio_beta']:.4f}")
    print(f"Dollar Beta:                    ${summary['dollar_beta']:,.2f}")
    print(f"Daily Market Vol ({benchmark}):        {summary['market_vol_daily']:.2%}")
    print(f"Portfolio Market Vol (Ann.):     {summary['portfolio_market_vol_ann']:.2%}")

    print("\nIDIOSYNCRATIC RISK COMPARISON")
    print(
        f"Independent Residuals (Ann.):   "
        f"{summary['portfolio_idio_vol_ann_independent']:.2%}"
    )
    print(
        f"Residual Covariance (Ann.):     "
        f"{summary['portfolio_idio_vol_ann_covariance']:.2%}"
    )

    print(
        f"\nPortfolio Total Vol (Ann.):     "
        f"{summary['portfolio_total_vol_ann']:.2%}"
    )

    print(
        f"\nCommon Residual Observations:   "
        f"{summary['common_observations']}"
    )
    print(
        f"Common Sample:                  "
        f"{summary['common_start_date']} to "
        f"{summary['common_end_date']}"
    )

    print("\nAVAILABLE-HISTORY INDEPENDENT MODEL")
    print(
        f"Portfolio Idio Vol (Ann.):      "
        f"{summary['portfolio_idio_vol_ann_available_history']:.2%}"
    )
    print(
        f"Portfolio Total Vol (Ann.):     "
        f"{summary['portfolio_total_vol_ann_available_history']:.2%}"
    )


    print("\nVALIDATION")

    if "TOST" in matrix["ticker"].values:
        validate_against_single_factor(
            ticker="TOST",
            benchmark=benchmark,
            start_date=start_date,
            end_date=end_date,
            returns=returns,
            risk_matrix=matrix,
        )

    validate_portfolio_risk(summary)

    validate_common_sample_portfolio(
        returns=returns,
        risk_matrix=matrix,
        residual_panel=residual_panel,
        summary=summary,
        benchmark=benchmark,
    )


if __name__ == "__main__":
    main()
