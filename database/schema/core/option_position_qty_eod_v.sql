-- View: core.option_position_qty_eod_v
--
-- Purpose:
-- Reconstruct historical option contract quantities
-- at the end of each transaction event date.
--
-- Dependencies:
-- core.option_transaction_events_v
--
-- Notes:
-- 1. Aggregate signed position changes by contract and date.
-- 2. Calculate cumulative contract quantities.
-- 3. Preserve negative quantities for short positions.
-- 4. Corporate action adjustments are already normalized.
-- 5. Calendar expansion occurs in a downstream view.
-- 6. Only resolved canonical contracts are included.

CREATE OR REPLACE VIEW core.option_position_qty_eod_v AS

WITH daily_deltas AS (

    SELECT
        option_contract_id,
        event_date AS date,

        SUM(position_quantity_delta) AS qty_delta,

        COUNT(*) AS event_count

    FROM core.option_transaction_events_v

    WHERE option_contract_id IS NOT NULL

    GROUP BY
        option_contract_id,
        event_date

)

SELECT
    date,
    option_contract_id,
    qty_delta,
    event_count,

    SUM(qty_delta) OVER (
        PARTITION BY option_contract_id
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS eod_qty

FROM daily_deltas;