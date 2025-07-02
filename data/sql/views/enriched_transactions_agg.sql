CREATE OR REPLACE VIEW enriched_transactions_agg AS

WITH time_index AS (
    SELECT 
        ts.instrument,
        ts.period_start_date,
        ts.period_end_date
    FROM time_series ts
),
filtered_data AS (
    SELECT *
    FROM active_equity_transactions_view
    WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL', 'DFEE', 'DTAX', 'REC')
),
joined_data AS (
    SELECT
        ti.instrument,
        ti.period_start_date,
        ti.period_end_date,
        fd.raw_trans_code,
        fd.quantity,
        fd.cash_flow,
        fd.weight,
        fd.transaction_id,
        fd.corrected_activity_date
    FROM time_index ti
    LEFT JOIN filtered_data fd
        ON  ti.instrument = fd.instrument
        AND fd.corrected_activity_date BETWEEN ti.period_start_date AND ti.period_end_date
)
SELECT
    instrument,
    period_start_date,
    period_end_date,

    COALESCE(SUM(CASE WHEN raw_trans_code = 'Buy'  THEN quantity ELSE 0 END), 0) AS total_buys,
    COALESCE(SUM(CASE WHEN raw_trans_code = 'Sell' THEN quantity ELSE 0 END), 0) AS total_sells,
    COALESCE(SUM(CASE WHEN raw_trans_code = 'SPL'  THEN quantity ELSE 0 END), 0) AS total_splits,

    COALESCE(SUM(CASE 
        WHEN raw_trans_code = 'Buy'  THEN quantity
        WHEN raw_trans_code = 'Sell' THEN -quantity
        WHEN raw_trans_code = 'SPL'  THEN quantity
        ELSE 0
    END), 0) AS net_quantity,

    COALESCE(SUM(cash_flow), 0) AS net_cash_flow,
    COALESCE(SUM(cash_flow * weight), 0) AS weighted_cash_flow,
    COALESCE(SUM(
        CASE WHEN raw_trans_code IN ('DFEE', 'DTAX') THEN cash_flow ELSE 0 END
        ), 0) AS fees_and_taxes,

    COALESCE(
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'trans_code', raw_trans_code,
                'date', TO_CHAR(corrected_activity_date, 'YYYY-MM-DD'),
                'cash_flow', cash_flow,
                'weight', ROUND(weight::numeric, 3),
                'quantity', quantity
            )
        ) FILTER (WHERE transaction_id IS NOT NULL),
        '[]'::JSON
    ) AS monthly_transactions

FROM joined_data
GROUP BY
    instrument,
    period_start_date,
    period_end_date
ORDER BY
    instrument,
    period_start_date;