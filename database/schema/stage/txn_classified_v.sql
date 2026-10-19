-- View: stage.txn_classified_v
--
-- Purpose:
-- Classify Robinhood transactions by economic activity,
-- cash-flow treatment, and position effects.
--
-- Notes:
-- OEXP/OCA are noncash option events.
-- REC affects equity quantity but is not an equity purchase.
-- Option quantities are reconstructed separately in core.
-- Transaction classification does not determine realized P&L.

CREATE OR REPLACE VIEW stage.txn_classified_v AS

SELECT
    e.*,
    e.corrected_activity_date::date AS econ_date,

    -- Primary transaction classification
    CASE
        WHEN e.raw_trans_code = 'ACH'
            THEN 'external_flow'

        WHEN e.raw_trans_code IN ('Buy','Sell')
            THEN 'equity_trade'

        WHEN e.raw_trans_code IN ('BTO','STC','STO','BTC')
            THEN 'option_trade'

        WHEN e.raw_trans_code IN ('OEXP','OCA')
            THEN 'option_event'

        WHEN e.raw_trans_code = 'CDIV'
            THEN 'income'

        WHEN e.raw_trans_code IN ('AFEE','DFEE','GOLD')
            THEN 'fee'

        WHEN e.raw_trans_code = 'DTAX'
            THEN 'tax'

        WHEN e.raw_trans_code = 'SPL'
            THEN 'corp_action'

        WHEN e.raw_trans_code = 'REC'
            THEN 'other'

        ELSE 'other'
    END AS txn_bucket,

    -- Classification flags
    (e.raw_trans_code = 'ACH') AS is_external_flow,
    (e.raw_trans_code = 'CDIV') AS is_income,
    (e.raw_trans_code IN ('AFEE','DFEE','GOLD')) AS is_fee,
    (e.raw_trans_code = 'DTAX') AS is_tax,
    (e.raw_trans_code = 'SPL') AS is_corp_action,

    (e.raw_trans_code IN ('Buy','Sell')) AS is_equity_trade,

    (e.raw_trans_code IN ('BTO','STC','STO','BTC'))
        AS is_option_trade,

    (e.raw_trans_code IN ('OEXP','OCA'))
        AS is_option_event,

    -- Trade direction
    CASE
        WHEN e.raw_trans_code IN ('Buy','BTO')
            THEN 'buy'

        WHEN e.raw_trans_code IN ('Sell','STC')
            THEN 'sell'

        WHEN e.raw_trans_code = 'STO'
            THEN 'sell_open'

        WHEN e.raw_trans_code = 'BTC'
            THEN 'buy_close'

        ELSE NULL
    END AS trade_side,

    -- External cash-flow direction
    CASE
        WHEN e.raw_trans_code = 'ACH' AND e.amount > 0
            THEN 'contribution'

        WHEN e.raw_trans_code = 'ACH' AND e.amount < 0
            THEN 'withdrawal'

        ELSE NULL
    END AS flow_side,

    -- Cash balance effects
    (
        e.raw_trans_code IN (
            'ACH','Buy','Sell','CDIV',
            'AFEE','DFEE','GOLD','DTAX',
            'BTO','STC','STO','BTC'
        )
    ) AS affects_cash_balance,

    -- Equity position effects (options reconstructed separately)
    (
        e.raw_trans_code IN ('Buy','Sell','REC','SPL')
    ) AS affects_position_qty,

    -- External vs. internal cash flows
    (e.raw_trans_code = 'ACH') AS is_external_cashflow,

    (
        e.raw_trans_code IN (
            'Buy','Sell','CDIV',
            'AFEE','DFEE','GOLD','DTAX',
            'BTO','STC','STO','BTC'
        )
    ) AS is_internal_cashflow

FROM stage.enriched_transactions_view e;