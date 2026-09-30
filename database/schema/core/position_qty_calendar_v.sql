CREATE OR REPLACE VIEW core.position_qty_calendar_v AS
WITH instruments AS (
    SELECT DISTINCT instrument
    FROM core.position_qty_eod_v
),
grid AS (
    SELECT
        mc.market_date AS date,
        i.instrument
    FROM core.market_calendar_v mc
    CROSS JOIN instruments i
)
SELECT
    g.date,
    g.instrument,
    COALESCE(p.eod_qty, 0)::numeric AS qty
FROM grid g
LEFT JOIN LATERAL (
    SELECT e.eod_qty
    FROM core.position_qty_eod_v e
    WHERE e.instrument = g.instrument
      AND e.date <= g.date
    ORDER BY e.date DESC
    LIMIT 1
) p ON TRUE
ORDER BY g.instrument, g.date;