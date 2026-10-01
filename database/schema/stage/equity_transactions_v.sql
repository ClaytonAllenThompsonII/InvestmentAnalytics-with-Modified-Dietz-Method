CREATE OR REPLACE VIEW stage.equity_transactions_v AS
SELECT *
FROM stage.enriched_transactions_view
WHERE 
    -- We only want raw_trans_codes relevant to equities
    raw_trans_code IN ('Buy','Sell','SPL', 'REC')
    -- AND optionally filter out any rows that actually have option details
    AND option_type IS NULL
    AND strike_price IS NULL;
