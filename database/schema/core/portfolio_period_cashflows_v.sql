-- View: core.portfolio_period_cashflows_v
--
-- Purpose:
-- Calculate monthly external flows and Modified Dietz weights.
--
-- Notes:
-- First period starts at portfolio inception.
-- Completed periods use calendar month boundaries.
-- Current MTD uses latest NAV date.
-- Inclusive calendar-day weighting; avoids DST errors.

CREATE OR REPLACE VIEW core.portfolio_period_cashflows_v AS

WITH inception AS (
    SELECT MIN(date) AS inception_date
    FROM core.portfolio_nav_daily_v
),

periods AS (
    SELECT
        e.period_start,
        e.period_end,

        GREATEST(
            e.period_start,
            i.inception_date
        ) AS effective_start_date,

        CASE
            WHEN e.period_end >= CURRENT_DATE
                THEN e.nav_date
            ELSE e.period_end
        END AS effective_end_date

    FROM core.portfolio_nav_eom_v e
    CROSS JOIN inception i
)

SELECT
    p.period_start,
    p.period_end,

    SUM(t.amount)::numeric AS external_flow,

    SUM(
        t.amount::numeric
        *
        (
            (p.effective_end_date - t.econ_date::date + 1)::numeric
            /
            NULLIF(
                p.effective_end_date - p.effective_start_date + 1,
                0
            )
        )
    )::numeric AS weighted_external_flow

FROM periods p

JOIN stage.txn_classified_v t
    ON t.raw_trans_code = 'ACH'
   AND t.econ_date::date >= p.effective_start_date
   AND t.econ_date::date <= p.effective_end_date

GROUP BY
    p.period_start,
    p.period_end;