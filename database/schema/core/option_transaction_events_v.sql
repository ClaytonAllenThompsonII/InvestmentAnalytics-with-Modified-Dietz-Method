-- View: core.option_transaction_events_v
--
-- Purpose:
-- Resolve standardized option transaction events to canonical
-- option contract identities.
--
-- Dependencies:
-- stage.option_transaction_events_v
-- core.option_contracts
--
-- Notes:
-- 1. Preserve historical underlying symbols (FB vs META).
-- 2. Resolve transactions using canonical contract attributes.
-- 3. Normalize FB to META for the verified historical transition.
-- 4. Preserve signed position and cash deltas from staging.
-- 5. Unmatched transactions remain visible for validation.
-- 6. Adjusted contracts require explicit handling before support.

CREATE OR REPLACE VIEW core.option_transaction_events_v AS

WITH normalized_events AS (

    SELECT
        e.transaction_id,
        e.event_date,
        e.process_date,
        e.settle_date,

        -- Original event information
        e.raw_trans_code,
        e.event_type,
        e.instrument AS historical_underlying_symbol,

        -- Canonical underlying
        CASE
            WHEN e.instrument = 'FB' THEN 'META'
            ELSE e.instrument
        END AS canonical_underlying_symbol,

        -- Contract attributes
        e.expiration_date,
        e.option_type,
        e.strike_price,

        -- Original transaction information
        e.description,
        e.quantity,
        e.raw_quantity,
        e.price,
        e.amount,

        -- Standardized accounting events
        e.position_quantity_delta,
        e.cash_delta

    FROM stage.option_transaction_events_v e

)

SELECT
    e.transaction_id,
    e.event_date,
    e.process_date,
    e.settle_date,

    e.raw_trans_code,
    e.event_type,

    -- Canonical contract identity
    c.option_contract_id,

    -- Preserve historical and canonical symbols
    e.historical_underlying_symbol,
    e.canonical_underlying_symbol,

    -- Contract attributes
    e.expiration_date,
    e.option_type,
    e.strike_price,

    c.premium_multiplier,

    -- Original transaction information
    e.description,
    e.quantity,
    e.raw_quantity,
    e.price,
    e.amount,

    -- Signed position and cash movements
    e.position_quantity_delta,
    e.cash_delta

FROM normalized_events e

LEFT JOIN core.option_contracts c
    ON c.underlying_symbol = e.canonical_underlying_symbol
   AND c.expiration_date = e.expiration_date
   AND c.option_type = e.option_type
   AND c.strike_price = e.strike_price
   AND c.is_adjusted = FALSE;