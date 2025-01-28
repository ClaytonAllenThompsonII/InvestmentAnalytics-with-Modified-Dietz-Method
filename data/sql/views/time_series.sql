CREATE OR REPLACE VIEW time_series AS
WITH instrument_activity AS (
    SELECT
        instrument,
        DATE_TRUNC('month', MIN(period_start_date)) AS start_date,

        /* 
         * Check if total_buys - total_sells > 0 across all months 
         * in enriched_transactions_agg. 
         * If so, extend to current month. Otherwise, just go to the last transaction month.
         */
        CASE
            WHEN (SUM(total_buys) - SUM(total_sells)) > 0 THEN
                GREATEST(
                    DATE_TRUNC('month', MAX(period_start_date)),
                    DATE_TRUNC('month', CURRENT_DATE)
                )
            ELSE
                DATE_TRUNC('month', MAX(period_start_date))
        END AS end_date

    FROM enriched_transactions_agg
    GROUP BY instrument
),
time_series AS (
    -- Generate the time series for each instrument from start_date to end_date (monthly)
    SELECT
        ia.instrument,
        generate_series(
            ia.start_date,
            ia.end_date,
            '1 month'
        )::DATE AS period_start_date
    FROM instrument_activity ia
)
SELECT
    ts.instrument,
    ts.period_start_date,
    ts.period_start_date + INTERVAL '1 month - 1 day' AS period_end_date
FROM
    time_series ts
ORDER BY
    ts.instrument,
    ts.period_start_date;