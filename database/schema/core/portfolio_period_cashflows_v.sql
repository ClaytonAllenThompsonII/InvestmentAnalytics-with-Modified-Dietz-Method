CREATE OR REPLACE VIEW core.portfolio_period_cashflows_v AS
SELECT
  period_start_date::date AS period_start,
  period_end_date::date   AS period_end,

  SUM(amount)::numeric AS external_flow,
  SUM((amount * weight))::numeric AS weighted_external_flow

FROM stage.txn_classified_v
WHERE raw_trans_code = 'ACH'
GROUP BY 1,2
ORDER BY period_start;