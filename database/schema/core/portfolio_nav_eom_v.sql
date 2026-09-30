CREATE OR REPLACE VIEW core.portfolio_nav_eom_v AS
WITH eom AS (
  SELECT
    date_trunc('month', date)::date AS period_start,
    MAX(date)::date AS nav_date
  FROM core.portfolio_nav_daily_v
  GROUP BY 1
)
SELECT
  e.period_start,
  (e.period_start + INTERVAL '1 month - 1 day')::date AS period_end,
  e.nav_date,
  p.portfolio_nav::numeric AS nav_value
FROM eom e
JOIN core.portfolio_nav_daily_v p
  ON p.date = e.nav_date
ORDER BY e.period_start;