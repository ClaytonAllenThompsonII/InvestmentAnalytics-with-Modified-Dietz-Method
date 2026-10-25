
-- View: core.security_cashflows_daily_v
--
-- Purpose:
-- Aggregate daily security-level cash-flow components.
--
-- Purchases and sales are positive magnitudes.
-- Gross dividends are positive distributions.
-- ADR fees and withholding taxes retain negative signs.

CREATE OR REPLACE VIEW core.security_cashflows_daily_v AS

SELECT
    t.econ_date::date AS date,
    t.normalized_instrument AS instrument,

    SUM(CASE
        WHEN t.raw_trans_code = 'Buy'
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS purchases,

    SUM(CASE
        WHEN t.raw_trans_code = 'Sell'
            THEN -t.cash_flow
        ELSE 0
    END)::numeric AS sales,

    SUM(CASE
        WHEN t.raw_trans_code = 'CDIV'
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS gross_dividend_paid,

    SUM(CASE
        WHEN t.raw_trans_code IN ('AFEE', 'DFEE')
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS adr_fee_cash,

    SUM(CASE
        WHEN t.raw_trans_code = 'DTAX'
            THEN t.cash_flow
        ELSE 0
    END)::numeric AS withholding_tax_cash

FROM stage.txn_classified_v t

WHERE t.normalized_instrument IS NOT NULL
  AND t.normalized_instrument <> 'CASH'
  AND t.raw_trans_code IN (
      'Buy', 'Sell', 'CDIV',
      'AFEE', 'DFEE', 'DTAX'
  )

GROUP BY 1, 2;
