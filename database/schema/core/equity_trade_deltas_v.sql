-- View: core.equity_trade_deltas_v

-- DROP VIEW core.equity_trade_deltas_v;

CREATE OR REPLACE VIEW core.equity_trade_deltas_v
 AS
 SELECT econ_date AS date,
    normalized_instrument AS instrument,
    sum(
        CASE
            WHEN raw_trans_code::text = 'Buy'::text THEN COALESCE(quantity, 0::numeric)
            WHEN raw_trans_code::text = 'Sell'::text THEN - COALESCE(quantity, 0::numeric)
            ELSE 0::numeric
        END) AS qty_delta
   FROM stage.txn_classified_v
  WHERE txn_bucket = 'equity_trade'::text AND normalized_instrument IS NOT NULL AND normalized_instrument::text <> 'CASH'::text
  GROUP BY econ_date, normalized_instrument
 HAVING sum(
        CASE
            WHEN raw_trans_code::text = 'Buy'::text THEN COALESCE(quantity, 0::numeric)
            WHEN raw_trans_code::text = 'Sell'::text THEN - COALESCE(quantity, 0::numeric)
            ELSE 0::numeric
        END) <> 0::numeric
  ORDER BY econ_date, normalized_instrument;

