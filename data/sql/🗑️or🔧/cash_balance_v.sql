CREATE OR REPLACE VIEW core.cash_balance_v AS
SELECT
  date,
  SUM(net_cash_change) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )::numeric AS cash_balance
FROM core.daily_cash_change_v
ORDER BY date;



DROP VIEW core.cash_balance_v;