CREATE OR REPLACE VIEW core.portfolio_nav_bom_eom_v AS
SELECT
  period_start,
  period_end,
  COALESCE(LAG(nav_value) OVER (ORDER BY period_start), 0)::numeric AS nav_bom,
  nav_value::numeric AS nav_eom
FROM core.portfolio_nav_eom_v
ORDER BY period_start;