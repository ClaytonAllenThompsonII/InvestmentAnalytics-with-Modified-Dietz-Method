-- View: core.portfolio_nav_daily_v
--
-- Purpose:
-- Calculate daily portfolio NAV from equity holdings,
-- option holdings, cash balances, and accrued dividends.
--
-- NAV = Equity NMV + Option NMV + Cash + Dividend Receivables
--
-- Notes:
-- Partial equity valuations are flagged but retained.
-- Missing option valuations or cash balances produce NULL NAV.

CREATE OR REPLACE VIEW core.portfolio_nav_daily_v AS

WITH receivables AS (
    SELECT
        date,
        SUM(dividend_receivable)::numeric
            AS dividend_receivable
    FROM core.dividend_receivable_daily_v
    GROUP BY date
)

SELECT
    e.date,

    -- Total invested market value (equities + options)
    (
        COALESCE(e.portfolio_nmv, 0)
        + o.option_nmv
    )::numeric AS portfolio_nmv,

    c.cash_balance::numeric AS cash_balance,

    -- Total portfolio NAV, including accrued dividends
    (
        COALESCE(e.portfolio_nmv, 0)
        + o.option_nmv
        + c.cash_balance
        + COALESCE(d.dividend_receivable, 0)
    )::numeric AS portfolio_nav,

    -- Component detail
    e.portfolio_nmv::numeric AS equity_nmv,
    o.option_nmv::numeric AS option_nmv,

    -- Equity valuation diagnostics
    e.unpriced_positions,
    e.unpriced_instruments,

    CASE
        WHEN o.option_nmv IS NULL
          OR c.cash_balance IS NULL
            THEN 'unpriced'

        WHEN e.valuation_status = 'partial'
            THEN 'partial'

        ELSE 'complete'
    END AS valuation_status,

    -- New column must be appended last
    COALESCE(d.dividend_receivable, 0)::numeric
        AS dividend_receivable

FROM core.portfolio_nmv_daily_v e

LEFT JOIN core.portfolio_option_nmv_daily_v o
    ON o.date = e.date

LEFT JOIN core.cash_balance_daily_v c
    ON c.date = e.date

LEFT JOIN receivables d
    ON d.date = e.date;