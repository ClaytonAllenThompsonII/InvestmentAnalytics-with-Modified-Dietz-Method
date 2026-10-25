
CREATE OR REPLACE VIEW core.dividend_receivable_daily_v AS

WITH valuation_dates AS (
    -- Use the existing daily market valuation calendar
    SELECT DISTINCT
        date
    FROM core.daily_prices_v
)

SELECT
    v.date,
    d.instrument,

    -- Outstanding gross dividend entitlement
    SUM(d.gross_dividend_usd)::numeric
        AS dividend_receivable

FROM core.dividend_entitlements_v d

JOIN valuation_dates v
    ON v.date >= d.ex_dividend_date
   AND v.date < d.payment_date

WHERE d.ex_date_match_count = 1
  AND d.payment_date IS NOT NULL
  AND d.ex_dividend_date < d.payment_date

GROUP BY
    v.date,
    d.instrument;
