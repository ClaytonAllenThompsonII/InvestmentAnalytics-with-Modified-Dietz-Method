CREATE OR REPLACE VIEW option_transactions AS
SELECT *
FROM enriched_transactions_view
WHERE 
    raw_trans_code IN ('BTO','STO','STC','BTC','OEXP')
    OR option_type IS NOT NULL
    OR strike_price IS NOT NULL;