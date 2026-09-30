-- View: core.daily_prices_v
-- DROP VIEW core.daily_prices_v;

CREATE OR REPLACE VIEW core.daily_prices_v
 AS
 SELECT m.price_date AS date,
    m.instrument,
    m.adjusted_close::numeric AS adj_close
   FROM source.market_data_daily_adjusted m
     JOIN core.market_calendar_v c ON c.market_date = m.price_date
  WHERE m.adjusted_close IS NOT NULL;
