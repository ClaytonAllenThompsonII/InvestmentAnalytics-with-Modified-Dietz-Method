DROP VIEW IF EXISTS public.split_events_v;

CREATE OR REPLACE VIEW core.split_events_v AS
SELECT
  instrument,
  price_date::date AS split_date,
  split_coefficient::numeric AS split_coeff
FROM source.market_data_daily_adjusted
WHERE split_coefficient IS NOT NULL
  AND split_coefficient <> 1
ORDER BY instrument, split_date;

