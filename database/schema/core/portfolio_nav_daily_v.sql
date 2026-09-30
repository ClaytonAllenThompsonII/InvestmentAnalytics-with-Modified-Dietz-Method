CREATE OR REPLACE VIEW core.portfolio_nav_daily_v AS
SELECT
  n.date,
  COALESCE(n.portfolio_nmv, 0)::numeric AS portfolio_nmv,
  COALESCE(c.cash_balance, 0)::numeric AS cash_balance,
  (COALESCE(n.portfolio_nmv, 0) + COALESCE(c.cash_balance, 0))::numeric AS portfolio_nav
FROM core.portfolio_nmv_daily_v n
LEFT JOIN core.cash_balance_daily_v c
  ON c.date = n.date
ORDER BY n.date;