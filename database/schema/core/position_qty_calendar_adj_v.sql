CREATE OR REPLACE VIEW core.position_qty_calendar_adj_v AS
SELECT
  c.date,
  c.instrument,
  c.qty AS qty_raw,
  COALESCE(r.split_factor_forward, 1)::numeric AS split_factor_forward,
  (c.qty * COALESCE(r.split_factor_forward, 1))::numeric AS qty_adj
FROM core.position_qty_calendar_v c
LEFT JOIN core.split_factor_forward_ranges_v r
  ON r.instrument = c.instrument
 AND c.date BETWEEN r.start_date AND r.end_date;
