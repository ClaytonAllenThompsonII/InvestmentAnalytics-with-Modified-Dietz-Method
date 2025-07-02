CREATE OR REPLACE VIEW  active_equity_transactions_view AS

WITH current_instruments AS (
    SELECT DISTINCT instrument
    FROM fifo_equity_lots
    WHERE open_quantity != 0
),
base_data AS (
    SELECT
        t.transaction_id,
        t.activity_date,
        t.process_date,
        t.settle_date,
        t.raw_trans_code,
        t.trans_code,
        t.instrument,
        t.description,
        t.quantity,
        t.price,
        t.amount,
        t.raw_quantity,
        t.raw_price,
        t.raw_amount,

        -- Extract record_date for CDIV
        CASE
            WHEN t.raw_trans_code = 'CDIV' AND t.description LIKE '%R/D%' THEN
                TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD')
            ELSE NULL
        END AS record_date,

        -- Extract payment_date for CDIV
        CASE
            WHEN t.raw_trans_code = 'CDIV' AND t.description LIKE '%P/D%' THEN
                TO_DATE(SUBSTRING(t.description, 'P/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD')
            ELSE NULL
        END AS payment_date,

        -- Extract expiration_date for options
        CASE
            WHEN t.raw_trans_code IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP') AND t.description ~ '\d{1,2}/\d{1,2}/\d{4}' THEN
                TO_DATE(SUBSTRING(t.description, '(\d{1,2}/\d{1,2}/\d{4})'), 'MM/DD/YYYY')
            ELSE NULL
        END AS expiration_date,

        -- Extract option_type
        CASE
            WHEN t.raw_trans_code IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP') THEN
                CASE
                    WHEN t.description ILIKE '%Call%' THEN 'Call'
                    WHEN t.description ILIKE '%Put%' THEN 'Put'
                    ELSE NULL
                END
            ELSE NULL
        END AS option_type,

        -- Extract strike_price
        CASE
            WHEN t.raw_trans_code IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP') AND t.description ~ '\$(\d+\.?\d*)' THEN
                SUBSTRING(t.description, '\$(\d+\.?\d*)')::NUMERIC
            ELSE NULL
        END AS strike_price,

        -- Period start and end
        DATE_TRUNC('month', COALESCE(
            TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD'),
            t.activity_date
        )) AS period_start_date,

        DATE_TRUNC('month', COALESCE(
            TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD'),
            t.activity_date
        )) + INTERVAL '1 month - 1 day' AS period_end_date,

        -- Cash flow
        CASE
            WHEN t.raw_trans_code = 'Buy' THEN ABS(t.amount)
            WHEN t.raw_trans_code = 'Sell' THEN -ABS(t.amount)
            ELSE t.amount
        END AS cash_flow
    FROM transactions t
    INNER JOIN current_instruments ci ON t.instrument = ci.instrument
	    WHERE t.raw_trans_code NOT IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP')

),
calculated_dimensions AS (
    SELECT
        bd.*,
        DATE_PART('day', bd.period_end_date - bd.period_start_date + INTERVAL '1 day') AS T,
        DATE_PART('day', COALESCE(bd.record_date, bd.activity_date) - bd.period_start_date) AS Ti,
        (
            DATE_PART('day', bd.period_end_date - COALESCE(bd.record_date, bd.activity_date) + INTERVAL '1 day')
            /
            DATE_PART('day', bd.period_end_date - bd.period_start_date + INTERVAL '1 day')
        ) AS weight
    FROM base_data bd
)
SELECT
    *,
    CASE
        WHEN raw_trans_code = 'CDIV' THEN record_date
        ELSE activity_date
    END AS corrected_activity_date
FROM calculated_dimensions;