-- View: core.cash_balance_daily_v
--
-- Purpose:
-- Calculate cumulative portfolio cash balances for each market date.
--
-- Dependencies:
-- core.market_calendar_v
-- core.daily_cash_change_v
--
-- Notes:
-- Includes cash activity occurring on weekends and market holidays.
-- Non-trading-day cash changes carry into the next market date.
-- Returns one cash balance per market-calendar date.
-- Assumes an opening cash balance of zero.

CREATE OR REPLACE VIEW core.cash_balance_daily_v AS

WITH dated_cash_flows AS (

    -- Include all market dates.
    SELECT
        market_date AS date,
        0::numeric AS net_cash_change,
        TRUE AS is_market_date
    FROM core.market_calendar_v

    UNION ALL

    -- Include all cash activity, even on non-market dates.
    SELECT
        date,
        net_cash_change,
        FALSE AS is_market_date
    FROM core.daily_cash_change_v

),

daily_changes AS (

    SELECT
        date,
        SUM(net_cash_change)::numeric AS net_cash_change,
        BOOL_OR(is_market_date) AS is_market_date

    FROM dated_cash_flows

    GROUP BY date

),

running_balances AS (

    SELECT
        date,
        is_market_date,

        SUM(net_cash_change) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::numeric AS cash_balance

    FROM daily_changes

)

SELECT
    date,
    cash_balance

FROM running_balances

WHERE is_market_date

ORDER BY date;