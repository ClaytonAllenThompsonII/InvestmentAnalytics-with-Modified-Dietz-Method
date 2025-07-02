CREATE OR REPLACE VIEW time_series AS

WITH portfolio_instruments AS (
    SELECT DISTINCT instrument
    FROM fifo_equity_lots
    WHERE open_quantity != 0
),
date_bounds AS (
    SELECT
        aetv.instrument,
        MIN(DATE_TRUNC('month', corrected_activity_date)) AS start_date,
        GREATEST(
            MAX(DATE_TRUNC('month', corrected_activity_date)),
            DATE_TRUNC('month', CURRENT_DATE)
        ) AS end_date
    FROM active_equity_transactions_view aetv
    INNER JOIN portfolio_instruments pi
        ON aetv.instrument = pi.instrument
    WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL', 'DFEE', 'DTAX', 'REC')
    GROUP BY aetv.instrument
),
time_series AS (
    SELECT
        instrument,
        generate_series(start_date, end_date, '1 month')::DATE AS period_start_date
    FROM date_bounds
)
SELECT
    instrument,
    period_start_date,
    period_start_date + INTERVAL '1 month - 1 day' AS period_end_date
FROM time_series
ORDER BY instrument, period_start_date;