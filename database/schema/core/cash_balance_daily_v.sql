CREATE OR REPLACE VIEW core.cash_balance_daily_v AS
WITH cal AS (
  SELECT market_date AS date
  FROM core.market_calendar_v
),
deltas AS (
  -- assumes daily_cash_change_v is already one row per date with net_cash_change
  SELECT
    date::date AS date,
    net_cash_change::numeric AS net_cash_change
  FROM core.daily_cash_change_v
),
joined AS (
  SELECT
    c.date,
    COALESCE(d.net_cash_change, 0::numeric) AS net_cash_change
  FROM cal c
  LEFT JOIN deltas d
    ON d.date = c.date
)
SELECT
  date,
  SUM(net_cash_change) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )::numeric AS cash_balance
FROM joined
ORDER BY date;