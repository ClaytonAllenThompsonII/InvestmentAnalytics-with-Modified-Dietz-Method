CREATE OR REPLACE VIEW core.daily_cash_change_v AS
SELECT
  date,
  SUM(cash_amount)::numeric AS net_cash_change
FROM core.cash_events_v
GROUP BY date
ORDER BY date;

-- Alter Schema to core
ALTER VIEW public.daily_cash_change_v SET SCHEMA core;