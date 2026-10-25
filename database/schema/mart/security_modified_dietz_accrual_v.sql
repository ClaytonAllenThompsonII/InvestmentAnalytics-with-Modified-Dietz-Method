
-- View: mart.security_modified_dietz_accrual_v
--
-- Purpose:
-- Calculate security-level Modified Dietz returns using
-- actual equity values and accrued dividend receivables.
--
-- Reporting conventions:
-- Gross: before ADR fees and foreign withholding
-- Net: after ADR fees, before withholding
-- After withholding: after ADR fees and foreign taxes
--
-- Positive trade_cash = capital invested in security.
-- Positive dividend_cash = distribution out of security.
-- ADR fees and withholding taxes are signed negative.

CREATE OR REPLACE VIEW mart.security_modified_dietz_accrual_v AS

WITH inputs AS (
    SELECT
        n.instrument,
        n.period_start,
        n.period_end,
        n.nav_date,
        n.valuation_status,

        n.value_bom,
        n.value_eom,

        COALESCE(c.trade_cash, 0) AS trade_cash,
        COALESCE(c.weighted_trade_cash, 0)
            AS weighted_trade_cash,

        COALESCE(c.dividend_cash, 0) AS dividend_cash,
        COALESCE(c.weighted_dividend_cash, 0)
            AS weighted_dividend_cash,

        COALESCE(c.adr_fee_cash, 0) AS adr_fee_cash,
        COALESCE(c.weighted_adr_fee_cash, 0)
            AS weighted_adr_fee_cash,

        COALESCE(c.withholding_tax_cash, 0)
            AS withholding_tax_cash,
        COALESCE(c.weighted_withholding_tax_cash, 0)
            AS weighted_withholding_tax_cash

    FROM core.security_value_bom_eom_v n

    LEFT JOIN core.security_period_cashflows_v c
        ON c.instrument = n.instrument
       AND c.period_start = n.period_start
),

flows AS (
    SELECT
        *,

        -- Gross: trades less gross distributions
        trade_cash - dividend_cash
            AS gross_flow,

        weighted_trade_cash - weighted_dividend_cash
            AS weighted_gross_flow,

        -- Net of ADR fees
        trade_cash - dividend_cash - adr_fee_cash
            AS net_fee_flow,

        weighted_trade_cash
            - weighted_dividend_cash
            - weighted_adr_fee_cash
            AS weighted_net_fee_flow,

        -- Net of ADR fees and withholding
        trade_cash - dividend_cash
            - adr_fee_cash - withholding_tax_cash
            AS after_withholding_flow,

        weighted_trade_cash
            - weighted_dividend_cash
            - weighted_adr_fee_cash
            - weighted_withholding_tax_cash
            AS weighted_after_withholding_flow

    FROM inputs
),

calculations AS (
    SELECT
        *,

        value_eom - value_bom - gross_flow
            AS gross_period_pnl,

        value_bom + weighted_gross_flow
            AS gross_avg_capital,

        value_eom - value_bom - net_fee_flow
            AS net_fee_period_pnl,

        value_bom + weighted_net_fee_flow
            AS net_fee_avg_capital,

        value_eom - value_bom - after_withholding_flow
            AS after_withholding_period_pnl,

        value_bom + weighted_after_withholding_flow
            AS after_withholding_avg_capital

    FROM flows
)

SELECT
    instrument,
    period_start,
    period_end,
    nav_date,
    valuation_status,

    value_bom,
    value_eom,

    trade_cash,
    dividend_cash,
    adr_fee_cash,
    withholding_tax_cash,

    gross_period_pnl,
    gross_avg_capital,

    CASE
        WHEN gross_avg_capital <= 0 THEN NULL
        ELSE gross_period_pnl / gross_avg_capital
    END AS gross_md_return,

    net_fee_period_pnl,
    net_fee_avg_capital,

    CASE
        WHEN net_fee_avg_capital <= 0 THEN NULL
        ELSE net_fee_period_pnl / net_fee_avg_capital
    END AS net_fee_md_return,

    after_withholding_period_pnl,
    after_withholding_avg_capital,

    CASE
        WHEN after_withholding_avg_capital <= 0 THEN NULL
        ELSE
            after_withholding_period_pnl
            / after_withholding_avg_capital
    END AS after_withholding_md_return

FROM calculations;
