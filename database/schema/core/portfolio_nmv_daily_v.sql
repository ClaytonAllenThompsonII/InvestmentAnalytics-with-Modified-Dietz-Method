CREATE OR REPLACE VIEW core.portfolio_nmv_daily_v AS
SELECT
  date,
  SUM(nmv)::numeric AS portfolio_nmv
FROM core.nmv_daily_v
GROUP BY date
ORDER BY date;