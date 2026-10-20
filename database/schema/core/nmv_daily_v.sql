CREATE OR REPLACE VIEW core.nmv_daily_v AS

SELECT
    q.date,
    q.instrument,

    -- Preserve existing adjusted fields
    q.qty_adj,
    p.adj_close,

    -- Historical equity market value
    CASE
        WHEN q.qty_raw = 0 THEN 0::numeric
        WHEN p.close_price IS NULL THEN NULL::numeric
        ELSE (q.qty_raw * p.close_price)::numeric
    END AS nmv,

    -- Additional valuation detail
    q.qty_raw,
    p.close_price,

    CASE
        WHEN q.qty_raw = 0 THEN 'no_position'
        WHEN p.close_price IS NULL THEN 'missing_price'
        ELSE 'priced'
    END AS valuation_status

FROM core.position_qty_calendar_adj_v q

LEFT JOIN core.daily_prices_v p
    ON p.date = q.date
   AND p.instrument = q.instrument;
