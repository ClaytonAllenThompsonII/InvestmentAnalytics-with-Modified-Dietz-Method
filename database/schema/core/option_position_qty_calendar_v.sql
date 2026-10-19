-- View: core.option_position_qty_calendar_v
--
-- Purpose:
-- Expand historical option position quantities across
-- the available market calendar.
--
-- Dependencies:
-- core.option_position_qty_eod_v
-- core.option_contracts
-- core.market_calendar_v
--
-- Notes:
-- 1. Carry forward the latest EOD quantity between events.
-- 2. Preserve negative quantities for short positions.
-- 3. Apply events on non-trading dates to the next market date.
-- 4. Do not extend positions beyond contract expiration.
-- 5. Preserve zero quantities between closing and reopening.
-- 6. Stop expanding closed positions after their final event.
-- 7. Market calendar freshness must be validated separately.

CREATE OR REPLACE VIEW core.option_position_qty_calendar_v AS

WITH event_periods AS (

    SELECT
        p.option_contract_id,

        p.date AS event_date,

        p.eod_qty,

        c.expiration_date,

        -- Identify the next date when the position changes
        LEAD(p.date) OVER (
            PARTITION BY p.option_contract_id
            ORDER BY p.date
        ) AS next_event_date

    FROM core.option_position_qty_eod_v p

    JOIN core.option_contracts c
        ON p.option_contract_id = c.option_contract_id

)

SELECT
    m.market_date AS date,

    p.option_contract_id,

    p.eod_qty,

    p.event_date AS last_event_date

FROM event_periods p

JOIN core.market_calendar_v m

    -- Position becomes effective on its event date
    ON m.market_date >= p.event_date

    -- Carry forward until the next event
    AND (
        p.next_event_date IS NULL
        OR m.market_date < p.next_event_date
    )

    -- Never extend positions beyond expiration
    AND m.market_date <= p.expiration_date

WHERE

    -- Retain intermediate periods, including zero positions
    p.next_event_date IS NOT NULL

    -- Extend final periods only while positions remain open
    OR p.eod_qty <> 0

    -- Preserve the final closing date when it is a market date
    OR m.market_date = p.event_date;