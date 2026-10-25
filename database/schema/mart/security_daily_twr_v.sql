
-- View: mart.security_daily_twr_v
--
-- Purpose:
-- Calculate daily-valued security-level TWR using
-- economic security values and classified cash flows.
--
-- Valuation:
--   Actual share market value + dividend receivables
--
-- Cash-flow convention:
--   Purchases: beginning of day
--   Sales and distributions: end of day
--
-- Reporting:
--   Gross: before ADR fees and withholding
--   Net fee: after ADR fees, before withholding
--   After withholding: after fees and foreign withholding

CREATE OR REPLACE VIEW mart.security_daily_twr_v AS

WITH valuations AS (
    SELECT
        instrument,
        date AS valuation_date,
        security_value AS ending_value,

        LAG(date) OVER (
            PARTITION BY instrument
            ORDER BY date
        ) AS previous_valuation_date,

        LAG(security_value) OVER (
            PARTITION BY instrument
            ORDER BY date
        ) AS beginning_value

    FROM core.security_value_daily_v
),

daily_flows AS (
    SELECT
        v.instrument,
        v.valuation_date,

        COALESCE(SUM(f.purchases), 0)::numeric
            AS purchases,

        COALESCE(SUM(f.sales), 0)::numeric
            AS sales,

        COALESCE(SUM(f.gross_dividend_paid), 0)::numeric
            AS gross_dividend_paid,

        COALESCE(SUM(f.adr_fee_cash), 0)::numeric
            AS adr_fee_cash,

        COALESCE(SUM(f.withholding_tax_cash), 0)::numeric
            AS withholding_tax_cash

    FROM valuations v

    LEFT JOIN core.security_cashflows_daily_v f
        ON f.instrument = v.instrument
       AND f.date > COALESCE(
           v.previous_valuation_date,
           v.valuation_date - 1
       )
       AND f.date <= v.valuation_date

    GROUP BY v.instrument, v.valuation_date
),

calculations AS (
    SELECT
        v.instrument,
        v.valuation_date AS date,
        v.previous_valuation_date,

        v.beginning_value,
        v.ending_value,

        f.purchases,
        f.sales,
        f.gross_dividend_paid,
        f.adr_fee_cash,
        f.withholding_tax_cash,

        -- Capital available during the trading day
        (
            v.beginning_value + f.purchases
        )::numeric AS capital_base,

        -- Gross investment profit/loss
        (
            v.ending_value
            - v.beginning_value
            - f.purchases
            + f.sales
            + f.gross_dividend_paid
        )::numeric AS gross_period_pnl,

        -- After attributable ADR fees
        (
            v.ending_value
            - v.beginning_value
            - f.purchases
            + f.sales
            + f.gross_dividend_paid
            + f.adr_fee_cash
        )::numeric AS net_fee_period_pnl,

        -- After fees and foreign withholding
        (
            v.ending_value
            - v.beginning_value
            - f.purchases
            + f.sales
            + f.gross_dividend_paid
            + f.adr_fee_cash
            + f.withholding_tax_cash
        )::numeric AS after_withholding_period_pnl

    FROM valuations v

    JOIN daily_flows f
        ON f.instrument = v.instrument
       AND f.valuation_date = v.valuation_date
)

SELECT
    *,

    CASE
        WHEN beginning_value IS NULL
          OR ending_value IS NULL
          OR capital_base <= 0
            THEN NULL
        ELSE gross_period_pnl / capital_base
    END AS gross_daily_return,

    CASE
        WHEN beginning_value IS NULL
          OR ending_value IS NULL
          OR capital_base <= 0
            THEN NULL
        ELSE net_fee_period_pnl / capital_base
    END AS net_fee_daily_return,

    CASE
        WHEN beginning_value IS NULL
          OR ending_value IS NULL
          OR capital_base <= 0
            THEN NULL
        ELSE after_withholding_period_pnl / capital_base
    END AS after_withholding_daily_return,

    CASE
        WHEN beginning_value IS NULL
          OR ending_value IS NULL
            THEN 'unpriced'
        WHEN capital_base <= 0
            THEN 'no_capital'
        ELSE 'complete'
    END AS valuation_status

FROM calculations;
