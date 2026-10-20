-- View: core.portfolio_nmv_daily_v
--
-- Purpose:
-- Aggregate daily equity market values across the portfolio.
--
-- Notes:
-- Missing security prices are excluded from SUM(nmv).
-- Incomplete valuations are explicitly identified.
-- Known historical exceptions: VBIV, MNMD, CYBN.

CREATE OR REPLACE VIEW core.portfolio_nmv_daily_v AS

SELECT
    date,

    SUM(nmv)::numeric AS portfolio_nmv,

    COUNT(*) FILTER (
        WHERE qty_raw <> 0
          AND nmv IS NULL
    ) AS unpriced_positions,

    STRING_AGG(instrument, ', ' ORDER BY instrument) FILTER (
        WHERE qty_raw <> 0
          AND nmv IS NULL
    ) AS unpriced_instruments,

    CASE
        WHEN COUNT(*) FILTER (
            WHERE qty_raw <> 0
              AND nmv IS NULL
        ) = 0
            THEN 'complete'
        ELSE 'partial'
    END AS valuation_status

FROM core.nmv_daily_v

GROUP BY date;