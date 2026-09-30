CREATE OR REPLACE VIEW core.security_nmv_bom_eom_v AS
WITH eom AS (
  SELECT
    instrument,
    date_trunc('month', date)::date AS period_start,
    MAX(date) AS eom_date
  FROM core.nmv_daily_v
  GROUP BY 1,2
),
eom_vals AS (
  SELECT
    e.instrument,
    e.period_start,
    (e.period_start + INTERVAL '1 month - 1 day')::date AS period_end,
    n.nmv::numeric AS nmv_eom
  FROM eom e
  JOIN core.nmv_daily_v n
    ON n.instrument = e.instrument
   AND n.date = e.eom_date
),
with_bom AS (
  SELECT
    instrument,
    period_start,
    period_end,
    nmv_eom,
    LAG(nmv_eom) OVER (PARTITION BY instrument ORDER BY period_start) AS nmv_bom
  FROM eom_vals
)
SELECT
  instrument,
  period_start,
  period_end,
  COALESCE(nmv_bom, 0)::numeric AS nmv_bom,
  nmv_eom
FROM with_bom;