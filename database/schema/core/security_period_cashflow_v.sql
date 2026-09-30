CREATE OR REPLACE VIEW core.security_period_cashflows_v AS
SELECT
  t.normalized_instrument AS instrument,
  t.period_start_date::date AS period_start,
  t.period_end_date::date   AS period_end,

  /* ----------------------------
     Trade cash (equity buy/sell)
     ---------------------------- */
  SUM(CASE
        WHEN t.raw_trans_code IN ('Buy','Sell') THEN t.cash_flow
        ELSE 0
      END)::numeric AS trade_cash,

  SUM(CASE
        WHEN t.raw_trans_code IN ('Buy','Sell') THEN t.cash_flow * t.weight
        ELSE 0
      END)::numeric AS weighted_trade_cash,

  /* ----------------------------
     Dividends
     ---------------------------- */
  SUM(CASE
        WHEN t.raw_trans_code = 'CDIV' THEN t.cash_flow
        ELSE 0
      END)::numeric AS dividend_cash,

  SUM(CASE
        WHEN t.raw_trans_code = 'CDIV' THEN t.cash_flow * t.weight
        ELSE 0
      END)::numeric AS weighted_dividend_cash,

  /* ----------------------------
     Taxes & fees
     ---------------------------- */
  SUM(CASE
        WHEN t.raw_trans_code IN ('DTAX','DFEE','GOLD') THEN t.cash_flow
        ELSE 0
      END)::numeric AS tax_fee_cash,

  SUM(CASE
        WHEN t.raw_trans_code IN ('DTAX','DFEE','GOLD') THEN t.cash_flow * t.weight
        ELSE 0
      END)::numeric AS weighted_tax_fee_cash

FROM stage.txn_classified_v t
WHERE t.normalized_instrument IS NOT NULL
  AND t.normalized_instrument <> 'CASH'
  AND t.raw_trans_code IN ('Buy','Sell','CDIV','DTAX','DFEE','GOLD')
GROUP BY 1,2,3;