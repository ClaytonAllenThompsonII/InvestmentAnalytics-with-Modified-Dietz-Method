
-- View: core.security_value_bom_eom_v
--
-- Purpose:
-- Calculate beginning and ending monthly security values,
-- including accrued dividend receivables.
--
-- Security Value = Equity NMV + Dividend Receivable

CREATE OR REPLACE VIEW core.security_value_bom_eom_v AS

WITH instruments AS (
    SELECT DISTINCT instrument
    FROM core.security_value_daily_v
),

monthly_values AS (
    SELECT
        i.instrument,
        m.period_start,
        m.period_end,
        m.nav_date,

        -- No security position or receivable = zero value.
        -- Preserve NULL when an existing valuation is missing.
        CASE
            WHEN s.date IS NULL THEN 0::numeric
            ELSE s.security_value
        END AS value_eom,

        CASE
            WHEN s.date IS NULL THEN 'no_position'
            ELSE s.valuation_status
        END AS valuation_status

    FROM instruments i

    CROSS JOIN core.portfolio_nav_eom_v m

    LEFT JOIN core.security_value_daily_v s
        ON s.instrument = i.instrument
       AND s.date = m.nav_date
),

with_bom AS (
    SELECT
        *,
        LAG(value_eom) OVER (
            PARTITION BY instrument
            ORDER BY period_start
        ) AS previous_value,

        ROW_NUMBER() OVER (
            PARTITION BY instrument
            ORDER BY period_start
        ) AS month_number

    FROM monthly_values
)

SELECT
    instrument,
    period_start,
    period_end,

    CASE
        WHEN month_number = 1 THEN 0::numeric
        ELSE previous_value
    END AS value_bom,

    value_eom,

    nav_date,
    valuation_status

FROM with_bom;
