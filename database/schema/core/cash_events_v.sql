CREATE OR REPLACE VIEW core.cash_events_v AS
SELECT
  econ_date::date AS date,
  'CASH'::text AS instrument,
  raw_trans_code,
  trans_code,
  txn_bucket,

  CASE
    WHEN raw_trans_code IN ('Buy','Sell') THEN (-cash_flow)::numeric
    ELSE cash_flow::numeric
  END AS cash_amount,

  CASE
    WHEN txn_bucket = 'external_flow' THEN 'external'
    WHEN txn_bucket = 'income' THEN 'income'
    WHEN txn_bucket IN ('fee','tax') THEN 'expense'
    WHEN txn_bucket IN ('option_trade','option_event') THEN 'option_cash'
    WHEN txn_bucket = 'equity_trade' THEN 'trade_cash'
    ELSE 'other_cash'
  END AS cash_event_type
FROM stage.txn_classified_v
WHERE affects_cash_balance = TRUE
  AND cash_flow IS NOT NULL;




  -- Note) Move the view object into core
ALTER VIEW public.cash_events_v SET SCHEMA core;