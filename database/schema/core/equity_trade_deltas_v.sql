-- Reconstruct daily equity share-count changes from Buy, Sell, and REC events.
-- REC is included because Robinhood used it for a free share received into the account.

-- View: core.equity_trade_deltas_v

-- DROP VIEW core.equity_trade_deltas_v;

CREATE OR REPLACE VIEW core.equity_trade_deltas_v AS

SELECT
    econ_date AS date,
    normalized_instrument AS instrument,

    SUM(
        CASE
            WHEN raw_trans_code IN ('Buy', 'REC')
                THEN COALESCE(quantity, 0::numeric)

            WHEN raw_trans_code = 'Sell'
                THEN -COALESCE(quantity, 0::numeric)

            ELSE 0::numeric
        END
    ) AS qty_delta

FROM stage.txn_classified_v

WHERE raw_trans_code IN ('Buy', 'Sell', 'REC')
  AND normalized_instrument IS NOT NULL
  AND normalized_instrument <> 'CASH'

GROUP BY
    econ_date,
    normalized_instrument

HAVING
    SUM(
        CASE
            WHEN raw_trans_code IN ('Buy', 'REC')
                THEN COALESCE(quantity, 0::numeric)

            WHEN raw_trans_code = 'Sell'
                THEN -COALESCE(quantity, 0::numeric)

            ELSE 0::numeric
        END
    ) <> 0::numeric

ORDER BY
    econ_date,
    normalized_instrument;

