-- View: core.portfolio_option_nmv_daily_v
--
-- Purpose:
-- Aggregate signed option contract market values into
-- daily portfolio-level option NMV.
--
-- Dependencies:
-- core.market_calendar_v
-- core.option_nmv_daily_v
--
-- Valuation:
-- Portfolio Option NMV = SUM(contract-level NMV)
--
-- Notes:
-- Includes every market-calendar date.
-- Returns 0 when no option positions are held.
-- Returns NULL if any open contract cannot be valued.
-- Preserves negative values for short option liabilities.

CREATE OR REPLACE VIEW core.portfolio_option_nmv_daily_v AS

WITH daily_valuations AS (

    SELECT
        date,

        COUNT(*) AS open_option_contracts,

        COUNT(*) FILTER (
            WHERE valuation_status <> 'priced'
               OR nmv IS NULL
        ) AS unpriced_contracts,

        SUM(nmv) AS total_nmv

    FROM core.option_nmv_daily_v

    GROUP BY date

)

SELECT
    m.market_date AS date,

    COALESCE(
        v.open_option_contracts, 0
    ) AS open_option_contracts,

    COALESCE(
        v.unpriced_contracts, 0
    ) AS unpriced_contracts,

    CASE
        WHEN COALESCE(v.unpriced_contracts, 0) > 0
            THEN NULL

        ELSE COALESCE(v.total_nmv, 0)

    END AS option_nmv

FROM core.market_calendar_v m

LEFT JOIN daily_valuations v
    ON v.date = m.market_date;