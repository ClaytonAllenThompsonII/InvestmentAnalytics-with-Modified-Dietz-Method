CREATE OR REPLACE VIEW stage.txn_classified_v AS
SELECT
  e.*,
  e.corrected_activity_date::date AS econ_date,

  /* --- primary classification --- */
  CASE
    WHEN e.raw_trans_code = 'ACH' THEN 'external_flow'

    WHEN e.raw_trans_code IN ('Buy', 'Sell') THEN 'equity_trade'

    WHEN e.raw_trans_code IN ('BTO','STC','STO','BTC') THEN 'option_trade'
    WHEN e.raw_trans_code = 'OEXP' THEN 'option_event'

    WHEN e.raw_trans_code = 'CDIV' THEN 'income'

    WHEN e.raw_trans_code IN ('DFEE','GOLD') THEN 'fee'
    WHEN e.raw_trans_code = 'DTAX' THEN 'tax'

    WHEN e.raw_trans_code = 'SPL' THEN 'corp_action'

    WHEN e.raw_trans_code = 'OCA' THEN 'admin'
    WHEN e.raw_trans_code = 'REC' THEN 'other'

    ELSE 'other'
  END AS txn_bucket,

  /* --- helpful booleans --- */
  (e.raw_trans_code = 'ACH') AS is_external_flow,
  (e.raw_trans_code = 'CDIV') AS is_income,
  (e.raw_trans_code IN ('DFEE','GOLD')) AS is_fee,
  (e.raw_trans_code = 'DTAX') AS is_tax,
  (e.raw_trans_code = 'SPL') AS is_corp_action,

  (e.raw_trans_code IN ('Buy','Sell')) AS is_equity_trade,
  (e.raw_trans_code IN ('BTO','STC','STO','BTC')) AS is_option_trade,
  (e.raw_trans_code = 'OEXP') AS is_option_event,

  /* --- trade direction for delta logic --- */
  CASE
    WHEN e.raw_trans_code IN ('Buy','BTO') THEN 'buy'
    WHEN e.raw_trans_code IN ('Sell','STC') THEN 'sell'
    WHEN e.raw_trans_code = 'STO' THEN 'sell_open'
    WHEN e.raw_trans_code = 'BTC' THEN 'buy_close'
    ELSE NULL
  END AS trade_side,

  /* --- external flow direction (nice for reporting) --- */
  CASE
    WHEN e.raw_trans_code = 'ACH' AND e.amount > 0 THEN 'contribution'
    WHEN e.raw_trans_code = 'ACH' AND e.amount < 0 THEN 'withdrawal'
    ELSE NULL
  END AS flow_side,

  /* --- NEW: cash / qty effect flags (key for Portfolio NAV + NMV separation) --- */
  CASE
    WHEN e.raw_trans_code IN ('ACH', 'Buy','Sell','CDIV','DFEE','GOLD','DTAX','BTO','STC','STO','BTC','OEXP') THEN TRUE
    ELSE FALSE
  END AS affects_cash_balance,

  CASE
    WHEN e.raw_trans_code IN ('Buy','Sell') THEN TRUE          -- equity qty changes
    WHEN e.raw_trans_code = 'SPL' THEN TRUE                    -- split changes qty
    ELSE FALSE
  END AS affects_position_qty,

  /* --- NEW: cashflow semantics (external vs internal) --- */
  (e.raw_trans_code = 'ACH') AS is_external_cashflow,

  (e.raw_trans_code IN ('Buy','Sell','CDIV','DFEE','GOLD','DTAX','BTO','STC','STO','BTC','OEXP')) AS is_internal_cashflow

FROM stage.enriched_transactions_view e;