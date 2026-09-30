-- View: core.nmv_daily_v

-- DROP VIEW core.nmv_daily_v;

CREATE OR REPLACE VIEW core.nmv_daily_v AS
SELECT
    q.date,
    q.instrument,
    q.qty_adj,
    p.adj_close,
    (q.qty_adj * p.adj_close)::numeric AS nmv
FROM core.position_qty_calendar_adj_v q
JOIN core.daily_prices_v p
  ON p.date = q.date
 AND p.instrument = q.instrument;
