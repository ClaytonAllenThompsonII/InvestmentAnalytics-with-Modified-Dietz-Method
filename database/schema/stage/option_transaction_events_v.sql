-- View: stage.option_transaction_events_v
--
-- Purpose:
-- Normalize Robinhood option transactions into standardized
-- position and cash events.
--
-- Dependencies:
-- stage.enriched_transactions_view
--
-- Notes:
-- 1. Preserve historical underlying symbols (FB vs META).
-- 2. Opening/closing trades use transaction quantity and code.
-- 3. Expiration/adjustment events derive signed quantity
--    changes from the original raw_quantity field.
-- 4. Cash deltas use actual transaction amounts, including fees.
-- 5. Contract identity will be resolved in a downstream model.

CREATE OR REPLACE VIEW stage.option_transaction_events_v AS

WITH parsed_events AS (

    SELECT
        e.transaction_id,
        e.activity_date AS event_date,
        e.process_date,
        e.settle_date,

        -- Original transaction information
        e.raw_trans_code,
        e.instrument,
        e.description,

        -- Parsed contract attributes
        e.expiration_date,
        LOWER(e.option_type) AS option_type,
        e.strike_price,

        -- Original quantities and amounts
        e.quantity,
        e.raw_quantity,
        e.price,
        e.amount,

        -- Preserve signed quantity from Robinhood's raw field.
        -- Observed convention:
        -- '2S' = -2
        -- '2'  = +2

        CASE
            WHEN BTRIM(e.raw_quantity) ~ '^[0-9]+(\.[0-9]+)?S$'
                THEN -LEFT(
                    BTRIM(e.raw_quantity),
                    LENGTH(BTRIM(e.raw_quantity)) - 1
                )::numeric

            WHEN BTRIM(e.raw_quantity) ~ '^[0-9]+(\.[0-9]+)?$'
                THEN BTRIM(e.raw_quantity)::numeric

            ELSE NULL::numeric
        END AS signed_raw_quantity

    FROM stage.enriched_transactions_view e

    WHERE e.raw_trans_code IN (
        'BTO',
        'STO',
        'STC',
        'BTC',
        'OEXP',
        'OCA'
    )
)

SELECT
    transaction_id,
    event_date,
    process_date,
    settle_date,

    raw_trans_code,

    -- Standardized economic event
    CASE
        WHEN raw_trans_code = 'BTO' THEN 'buy_to_open'
        WHEN raw_trans_code = 'STO' THEN 'sell_to_open'
        WHEN raw_trans_code = 'STC' THEN 'sell_to_close'
        WHEN raw_trans_code = 'BTC' THEN 'buy_to_close'
        WHEN raw_trans_code = 'OEXP' THEN 'expiration'
        WHEN raw_trans_code = 'OCA' THEN 'contract_adjustment'
    END AS event_type,

    -- Historical symbol; intentionally not normalized
    instrument,

    description,
    expiration_date,
    option_type,
    strike_price,

    quantity,
    raw_quantity,
    price,
    amount,

    -- Signed change in option contract quantity
    CASE
        WHEN raw_trans_code IN ('BTO', 'BTC')
            THEN quantity

        WHEN raw_trans_code IN ('STO', 'STC')
            THEN -quantity

        WHEN raw_trans_code IN ('OEXP', 'OCA')
            THEN signed_raw_quantity

        ELSE NULL::numeric
    END AS position_quantity_delta,

    -- Actual cash impact of the event
    CASE
        WHEN raw_trans_code IN ('OEXP', 'OCA')
            THEN COALESCE(amount, 0)

        ELSE amount
    END AS cash_delta

FROM parsed_events;