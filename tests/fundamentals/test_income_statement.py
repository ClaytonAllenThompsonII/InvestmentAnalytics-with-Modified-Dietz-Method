import pandas as pd
import pytest
from datetime import date

from investment_analytics.fundamentals.income_statement import (
    calculate_income_statement_metrics,
    prepare_income_statement,
    calculate_ttm_metrics,
    calculate_cagr,
    build_income_statement_summary,
)


def test_prepare_income_statement_converts_and_sorts():
    df = pd.DataFrame(
        {
            "instrument": ["TEST", "TEST"],
            "fiscal_date": ["2025-12-31", "2024-12-31"],
            "frequency": ["annual", "annual"],
            "reported_currency": ["USD", "USD"],
            "total_revenue": ["120", "100"],
        }
    )

    result = prepare_income_statement(df)

    assert result["fiscal_date"].is_monotonic_increasing
    assert pd.api.types.is_datetime64_any_dtype(result["fiscal_date"])
    assert pd.api.types.is_numeric_dtype(result["total_revenue"])


def test_annual_revenue_growth_uses_prior_year():
    df = pd.DataFrame(
        {
            "instrument": ["TEST", "TEST"],
            "fiscal_date": ["2024-12-31", "2025-12-31"],
            "frequency": ["annual", "annual"],
            "reported_currency": ["USD", "USD"],
            "total_revenue": [100.0, 125.0],
            "net_income": [10.0, 15.0],
            "gross_profit": [50.0, 65.0],
            "operating_income": [20.0, 30.0],
            "ebitda": [25.0, 35.0],
            "research_and_development": [5.0, 6.0],
            "selling_general_and_administrative": [10.0, 11.0],
        }
    )

    result = calculate_income_statement_metrics(df)

    assert result.iloc[-1]["revenue_growth"] == pytest.approx(0.25)

    assert result.iloc[-1]["operating_margin"] == pytest.approx(0.24)


def test_quarterly_revenue_growth_uses_same_quarter_prior_year():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 5,
            "fiscal_date": [
                "2024-06-30",
                "2024-09-30",
                "2024-12-31",
                "2025-03-31",
                "2025-06-30",
            ],
            "frequency": ["quarterly"] * 5,
            "reported_currency": ["USD"] * 5,
            "total_revenue": [100.0, 105.0, 110.0, 115.0, 120.0],
            "net_income": [10.0] * 5,
            "gross_profit": [50.0] * 5,
            "operating_income": [20.0] * 5,
            "ebitda": [25.0] * 5,
            "research_and_development": [5.0] * 5,
            "selling_general_and_administrative": [10.0] * 5,
        }
    )

    result = calculate_income_statement_metrics(df)

    assert result.iloc[-1]["revenue_growth"] == pytest.approx(0.20)


def test_mixed_frequency_raises_error():
    df = pd.DataFrame(
        {
            "instrument": ["TEST", "TEST"],
            "fiscal_date": ["2025-06-30", "2025-12-31"],
            "frequency": ["quarterly", "annual"],
        }
    )

    with pytest.raises(
        ValueError,
        match="exactly one reporting frequency",
    ):
        calculate_income_statement_metrics(df)


def test_quarterly_yoy_growth_handles_missing_quarter():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 5,
            "fiscal_date": [
                "2024-06-30",
                "2024-09-30",
                "2024-12-31",
                "2025-03-31",
                "2025-09-30",
            ],
            "frequency": ["quarterly"] * 5,
            "reported_currency": ["USD"] * 5,
            "total_revenue": [
                100.0,
                200.0,
                300.0,
                400.0,
                240.0,
            ],
            "net_income": [10.0] * 5,
            "gross_profit": [50.0] * 5,
            "operating_income": [20.0] * 5,
            "ebitda": [25.0] * 5,
            "research_and_development": [5.0] * 5,
            "selling_general_and_administrative": [10.0] * 5,
        }
    )

    result = calculate_income_statement_metrics(df)

    assert result.iloc[-1]["revenue_growth"] == pytest.approx(0.20)



def test_ttm_revenue_sums_four_consecutive_quarters():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 4,
            "fiscal_date": [
                "2024-09-30",
                "2024-12-31",
                "2025-03-31",
                "2025-06-30",
            ],
            "frequency": ["quarterly"] * 4,
            "reported_currency": ["USD"] * 4,
            "total_revenue": [100.0, 110.0, 120.0, 130.0],
            "gross_profit": [50.0] * 4,
            "operating_income": [20.0] * 4,
            "ebitda": [25.0] * 4,
            "net_income": [10.0] * 4,
            "research_and_development": [5.0] * 4,
            "selling_general_and_administrative": [10.0] * 4,
        }
    )

    result = calculate_ttm_metrics(df)

    assert result.iloc[-1]["total_revenue_ttm"] == pytest.approx(460.0)


def test_ttm_revenue_is_missing_when_quarter_is_missing():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 4,
            "fiscal_date": [
                "2024-09-30",
                "2024-12-31",
                "2025-03-31",
                "2025-09-30",
            ],
            "frequency": ["quarterly"] * 4,
            "reported_currency": ["USD"] * 4,
            "total_revenue": [100.0, 110.0, 120.0, 140.0],
            "gross_profit": [50.0] * 4,
            "operating_income": [20.0] * 4,
            "ebitda": [25.0] * 4,
            "net_income": [10.0] * 4,
            "research_and_development": [5.0] * 4,
            "selling_general_and_administrative": [10.0] * 4,
        }
    )

    result = calculate_ttm_metrics(df)

    assert pd.isna(result.iloc[-1]["total_revenue_ttm"])

def test_ttm_metrics_reject_annual_data():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 4,
            "fiscal_date": [
                "2022-12-31",
                "2023-12-31",
                "2024-12-31",
                "2025-12-31",
            ],
            "frequency": ["annual"] * 4,
            "reported_currency": ["USD"] * 4,
            "total_revenue": [100.0, 110.0, 120.0, 130.0],
        }
    )

    with pytest.raises(
        ValueError,
        match="TTM metrics require quarterly",
    ):
        calculate_ttm_metrics(df)


def test_three_year_cagr():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 4,
            "fiscal_date": [
                "2022-12-31",
                "2023-12-31",
                "2024-12-31",
                "2025-12-31",
            ],
            "frequency": ["annual"] * 4,
            "reported_currency": ["USD"] * 4,
            "total_revenue": [
                100.0,
                120.0,
                144.0,
                172.8,
            ],
        }
    )

    result = calculate_cagr(
        df,
        "total_revenue",
        years=3,
    )

    assert result == pytest.approx(0.20)


def test_cagr_is_missing_when_start_period_is_missing():
    df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 3,
            "fiscal_date": [
                "2023-12-31",
                "2024-12-31",
                "2025-12-31",
            ],
            "frequency": ["annual"] * 3,
            "reported_currency": ["USD"] * 3,
            "total_revenue": [
                120.0,
                144.0,
                172.8,
            ],
        }
    )

    result = calculate_cagr(
        df,
        "total_revenue",
        years=3,
    )

    assert pd.isna(result)



def test_build_income_statement_summary():
    annual_df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 6,
            "fiscal_date": [
                "2020-12-31",
                "2021-12-31",
                "2022-12-31",
                "2023-12-31",
                "2024-12-31",
                "2025-12-31",
            ],
            "frequency": ["annual"] * 6,
            "reported_currency": ["USD"] * 6,
            "total_revenue": [
                100.0,
                120.0,
                144.0,
                172.8,
                207.36,
                248.832,
            ],
            "operating_income": [
                20.0,
                24.0,
                28.8,
                34.56,
                41.472,
                49.7664,
            ],
        }
    )

    quarterly_df = pd.DataFrame(
        {
            "instrument": ["TEST"] * 8,
            "fiscal_date": [
                "2024-09-30",
                "2024-12-31",
                "2025-03-31",
                "2025-06-30",
                "2025-09-30",
                "2025-12-31",
                "2026-03-31",
                "2026-06-30",
            ],
            "frequency": ["quarterly"] * 8,
            "reported_currency": ["USD"] * 8,
            "total_revenue": [
                100.0,
                110.0,
                120.0,
                130.0,
                120.0,
                132.0,
                144.0,
                156.0,
            ],
            "gross_profit": [
                50.0,
                55.0,
                60.0,
                65.0,
                60.0,
                66.0,
                72.0,
                78.0,
            ],
            "operating_income": [
                20.0,
                22.0,
                24.0,
                26.0,
                24.0,
                26.4,
                28.8,
                31.2,
            ],
            "ebitda": [
                25.0,
                27.5,
                30.0,
                32.5,
                30.0,
                33.0,
                36.0,
                39.0,
            ],
            "net_income": [
                10.0,
                11.0,
                12.0,
                13.0,
                12.0,
                13.2,
                14.4,
                15.6,
            ],
            "research_and_development": [5.0] * 8,
            "selling_general_and_administrative": [10.0] * 8,
        }
    )

    summary = build_income_statement_summary(
        annual_df,
        quarterly_df,
    )

    assert summary["instrument"] == "TEST"
    assert summary["reported_currency"] == "USD"

    assert summary["latest_quarter"]["fiscal_date"] == date(
        2026,
        6,
        30,
    )

    assert summary["latest_quarter"]["revenue"] == pytest.approx(156.0)
    assert summary["latest_quarter"]["revenue_growth"] == pytest.approx(0.20)

    assert summary["ttm"]["revenue"] == pytest.approx(552.0)
    assert summary["ttm"]["revenue_growth"] == pytest.approx(0.20)

    assert summary["long_term"]["revenue_cagr_3y"] == pytest.approx(0.20)
    assert summary["long_term"]["revenue_cagr_5y"] == pytest.approx(0.20)

    assert isinstance(
        summary["latest_quarter"]["revenue"],
        float,
    )
    assert isinstance(
        summary["latest_quarter"]["fiscal_date"],
        date,
    )    