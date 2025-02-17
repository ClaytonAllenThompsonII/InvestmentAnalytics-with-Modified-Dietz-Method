CREATE OR REPLACE VIEW other_cashflow_transactions AS
SELECT *
FROM enriched_transactions_view
WHERE raw_trans_code NOT IN (
    'Buy','Sell','SPL',
    'BTO','STC','STO','BTC','OEXP', 'OCA'
)
  -- i.e., 'CDIV','DFEE','GOLD','DTAX','ACH','REC', etc.
;