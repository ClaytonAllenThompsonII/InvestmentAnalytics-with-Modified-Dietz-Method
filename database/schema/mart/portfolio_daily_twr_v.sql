
-- View: mart.portfolio_daily_twr_v
--
-- Daily portfolio returns adjusted for external cash flows.
-- Assumes external flows occur at the beginning of the
-- applicable valuation day.
--
-- Non-trading-day flows are assigned to the next available
-- valuation date.
--
-- Daily returns are stored as decimals, not percentages.

CREATE OR REPLACE VIEW mart.portfolio_daily_twr_v AS

WITH daily_nav AS (
    SELECT
        date AS nav_date,

        -- Use authoritative portfolio NAV
        portfolio_nav AS ending_nav,

        valuation_status,

        LAG(date) OVER (
            ORDER BY date
        ) AS previous_nav_date,

        LAG(portfolio_nav) OVER (
            ORDER BY date
        ) AS beginning_nav,

        LAG(valuation_status) OVER (
            ORDER BY date
        ) AS beginning_valuation_status

    FROM core.portfolio_nav_daily_v
),

daily_flows AS (
    SELECT
        n.nav_date,

        COALESCE(
            SUM(t.amount), 0
        )::numeric AS external_flow

    FROM daily_nav n

    LEFT JOIN stage.txn_classified_v t
        ON t.raw_trans_code = 'ACH'
       AND t.econ_date > COALESCE(
           n.previous_nav_date,
           n.nav_date - 1
       )
       AND t.econ_date <= n.nav_date

    GROUP BY n.nav_date
)

SELECT
    n.nav_date,
    n.previous_nav_date,
    n.beginning_nav,
    n.ending_nav,
    f.external_flow,

    CASE
        -- No return before the first beginning valuation.
        WHEN n.beginning_nav IS NULL THEN NULL::numeric

        -- Prevent invalid or zero-capital denominators.
        WHEN n.beginning_nav + f.external_flow <= 0
            THEN NULL::numeric

        ELSE (
            n.ending_nav
            / (n.beginning_nav + f.external_flow)
            - 1
        )::numeric
    END AS daily_return,

    CASE
        WHEN n.valuation_status = 'complete'
         AND n.beginning_valuation_status = 'complete'
            THEN 'complete'
        ELSE 'partial'
    END AS valuation_status

FROM daily_nav n

JOIN daily_flows f
    ON f.nav_date = n.nav_date

ORDER BY n.nav_date;
