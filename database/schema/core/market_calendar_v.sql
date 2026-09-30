-- View: core.market_calendar_v
-- DROP VIEW core.market_calendar_v;

CREATE OR REPLACE VIEW core.market_calendar_v
 AS
 SELECT DISTINCT price_date AS market_date
   FROM source.market_data_daily_adjusted
  WHERE price_date >= '2020-07-06'::date
  ORDER BY price_date;