-- View: core.security_period_cashflows_v
--
-- Purpose:
-- Aggregate security-level transaction cash flows
-- for performance measurement.
--
-- Trade cash:
--   Positive = capital invested in security
--   Negative = capital withdrawn from security
--
-- Dividends:
--   Positive = gross dividend received
--
-- ADR fees and withholding taxes:
--   Negative = expense or withholding
--
-- Legacy tax_fee_cash columns are retained temporarily
-- for compatibility with existing dependent views.

CREATE OR REPLACE VIEW core.security_period_cashflows_v AS

SELECT
    t.normalized_instrument AS instrument,
    t.period_start_date::date AS period_start,
    t.period_end_date::date AS period_end,

    -- Equity purchases and sales
    SUM(CASE
        WHEN t.raw_trans_code IN ('Buy', 'Sell')
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS trade_cash,

    SUM(CASE
        WHEN t.raw_trans_code IN ('Buy', 'Sell')
            THEN t.cash_flow * t.weight
        ELSE 0
    END)::numeric AS weighted_trade_cash,

    -- Gross cash dividends
    SUM(CASE
        WHEN t.raw_trans_code = 'CDIV'
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS dividend_cash,

    SUM(CASE
        WHEN t.raw_trans_code = 'CDIV'
            THEN t.cash_flow * t.weight
        ELSE 0
    END)::numeric AS weighted_dividend_cash,

    -- Legacy combined tax/fee fields
    SUM(CASE
        WHEN t.raw_trans_code IN ('DTAX', 'DFEE', 'GOLD')
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS tax_fee_cash,

    SUM(CASE
        WHEN t.raw_trans_code IN ('DTAX', 'DFEE', 'GOLD')
            THEN t.cash_flow * t.weight
        ELSE 0
    END)::numeric AS weighted_tax_fee_cash,

    -- NEW: Security-attributable ADR fees
    SUM(CASE
        WHEN t.raw_trans_code IN ('AFEE', 'DFEE')
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS adr_fee_cash,

    SUM(CASE
        WHEN t.raw_trans_code IN ('AFEE', 'DFEE')
            THEN t.cash_flow * t.weight
        ELSE 0
    END)::numeric AS weighted_adr_fee_cash,

    -- NEW: Foreign dividend withholding taxes
    SUM(CASE
        WHEN t.raw_trans_code = 'DTAX'
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS withholding_tax_cash,

    SUM(CASE
        WHEN t.raw_trans_code = 'DTAX'
            THEN t.cash_flow * t.weight
        ELSE 0
    END)::numeric AS weighted_withholding_tax_cash

FROM stage.txn_classified_v t

WHERE t.normalized_instrument IS NOT NULL
  AND t.normalized_instrument <> 'CASH'
  AND t.raw_trans_code IN (
      'Buy', 'Sell', 'CDIV',
      'DTAX', 'DFEE', 'AFEE', 'GOLD'
  )

GROUP BY 1, 2, 3;