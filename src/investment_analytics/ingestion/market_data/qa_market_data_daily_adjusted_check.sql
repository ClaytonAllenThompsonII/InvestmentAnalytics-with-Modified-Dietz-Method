-- ============================================================================
-- qa_market_data_daily_adjusted_checks.sql
--
-- Purpose:
-- Quality checks for market_data_daily_adjusted (Alpha Vantage Daily Adjusted).
-- Use after each truncate + reload to validate completeness, uniqueness,
-- data freshness, null rates, and corporate-action adjustments.
--
-- Table:
--   market_data_daily_adjusted
--
-- Notes:
-- - "Adjusted close" should differ from "close" for instruments with dividends
--   and/or splits (e.g., SPY, QQQ). For many growth stocks with no dividends or
--   splits (e.g., TOST, NU), adjusted may equal close for all days.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CHECK 01 — Overall sanity: rowcount, date coverage, and freshness
-- ----------------------------------------------------------------------------
SELECT
  COUNT(*)                 AS rows,
  MIN(price_date)          AS min_price_date,
  MAX(price_date)          AS max_price_date,
  MAX(last_refreshed_date) AS max_last_refreshed_date
FROM market_data_daily_adjusted;


-- ----------------------------------------------------------------------------
-- CHECK 02 — Per-instrument coverage: rowcount, date range, last refreshed
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*)                 AS rows,
  MIN(price_date)          AS min_price_date,
  MAX(price_date)          AS max_price_date,
  MAX(last_refreshed_date) AS last_refreshed_date
FROM market_data_daily_adjusted
GROUP BY instrument
ORDER BY instrument;


-- ----------------------------------------------------------------------------
-- CHECK 03 — Duplicate check: should return ZERO rows
-- ----------------------------------------------------------------------------
SELECT instrument, price_date, COUNT(*) AS c
FROM market_data_daily_adjusted
GROUP BY instrument, price_date
HAVING COUNT(*) > 1;


-- ----------------------------------------------------------------------------
-- CHECK 04 — Null-rate checks (high signal for broken loads)
-- Expectation:
-- - close_price and adjusted_close should usually be non-null
-- - dividend_amount and split_coefficient should usually be present (often 0/1)
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  SUM((open_price IS NULL)::int)         AS open_nulls,
  SUM((high_price IS NULL)::int)         AS high_nulls,
  SUM((low_price IS NULL)::int)          AS low_nulls,
  SUM((close_price IS NULL)::int)        AS close_nulls,
  SUM((adjusted_close IS NULL)::int)     AS adj_close_nulls,
  SUM((volume IS NULL)::int)             AS volume_nulls,
  SUM((dividend_amount IS NULL)::int)    AS dividend_nulls,
  SUM((split_coefficient IS NULL)::int)  AS split_nulls,
  SUM((last_refreshed_date IS NULL)::int) AS last_refreshed_nulls
FROM market_data_daily_adjusted
GROUP BY instrument
ORDER BY instrument;


-- ----------------------------------------------------------------------------
-- CHECK 05 — Adjustment existence: how often adjusted_close differs from close
-- Interpretation:
-- - ETFs / dividend payers should show lots of differences
-- - Non-dividend / no-split stocks may show 0 differences (expected)
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*) FILTER (
    WHERE adjusted_close IS NOT NULL
      AND close_price IS NOT NULL
      AND adjusted_close <> close_price
  ) AS adj_diff_days,
  COUNT(*) AS total_days
FROM market_data_daily_adjusted
GROUP BY instrument
ORDER BY adj_diff_days DESC, instrument;


-- ----------------------------------------------------------------------------
-- CHECK 06 — Dividend events: confirm dividend days exist where expected
-- Use this to spot-check specific instruments.
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*) FILTER (WHERE dividend_amount IS NOT NULL AND dividend_amount > 0) AS dividend_days,
  MAX(dividend_amount) AS max_dividend
FROM market_data_daily_adjusted
WHERE instrument IN ('SPY','QQQ','XLK','XLY','EEM','IXUS','AAPL','TSM','GOOGL','TOST','NU','CPNG','PONY')
GROUP BY instrument
ORDER BY instrument;


-- ----------------------------------------------------------------------------
-- CHECK 07 — Split events: confirm split days exist where expected
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*) FILTER (WHERE split_coefficient IS NOT NULL AND split_coefficient <> 1) AS split_days,
  MIN(split_coefficient) AS min_split_coeff,
  MAX(split_coefficient) AS max_split_coeff
FROM market_data_daily_adjusted
WHERE instrument IN ('SPY','QQQ','XLK','XLY','EEM','IXUS','AAPL','TSM','GOOGL','TOST','NU','CPNG','PONY')
GROUP BY instrument
ORDER BY instrument;


-- ----------------------------------------------------------------------------
-- CHECK 08 — ETF adjustment gate (PASS/FAIL style):
-- ETFs should have at least 1 day where adjusted_close <> close_price.
-- Expected result: ZERO rows.
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*) FILTER (
    WHERE adjusted_close IS NOT NULL
      AND close_price IS NOT NULL
      AND adjusted_close <> close_price
  ) AS adj_diff_days
FROM market_data_daily_adjusted
WHERE instrument IN ('SPY','QQQ','XLK','XLY','EEM','IXUS')
GROUP BY instrument
HAVING COUNT(*) FILTER (
  WHERE adjusted_close IS NOT NULL
    AND close_price IS NOT NULL
    AND adjusted_close <> close_price
) = 0;


-- ----------------------------------------------------------------------------
-- CHECK 09 — Growth-stock “no corporate actions” expectation (informational)
-- These may legitimately have 0 dividend days and 0 split days.
-- ----------------------------------------------------------------------------
SELECT
  instrument,
  COUNT(*) FILTER (WHERE dividend_amount > 0) AS dividend_days,
  COUNT(*) FILTER (WHERE split_coefficient <> 1) AS split_days,
  COUNT(*) FILTER (WHERE adjusted_close <> close_price) AS adj_diff_days,
  COUNT(*) AS total_days
FROM market_data_daily_adjusted
WHERE instrument IN ('TOST','NU','CPNG','PONY')
GROUP BY instrument
ORDER BY instrument;


-- ----------------------------------------------------------------------------
-- CHECK 10 — Latest rows spot-check (human-readable)
-- ----------------------------------------------------------------------------
SELECT instrument, price_date, close_price, adjusted_close, dividend_amount, split_coefficient, last_refreshed_date
FROM market_data_daily_adjusted
WHERE instrument IN ('SPY','QQQ','AAPL','TOST')
ORDER BY instrument, price_date DESC
LIMIT 50;