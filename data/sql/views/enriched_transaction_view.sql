CREATE OR REPLACE VIEW enriched_transactions_view AS
WITH base_data AS (
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

        -- Extract option_type for options
        CASE
            WHEN t.raw_trans_code IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP') THEN
                CASE
                    WHEN t.description ILIKE '%Call%' THEN 'Call'
                    WHEN t.description ILIKE '%Put%' THEN 'Put'
                    ELSE NULL
                END
            ELSE NULL
        END AS option_type,

        -- Extract strike_price for options
        CASE
            WHEN t.raw_trans_code IN ('BTC', 'STC', 'BTO', 'STO', 'OCA', 'OEXP') AND t.description ~ '\$(\d+\.?\d*)' THEN
                SUBSTRING(t.description, '\$(\d+\.?\d*)')::NUMERIC
            ELSE NULL
        END AS strike_price,

        -- Calculate dividend period start date
        CASE
            WHEN t.raw_trans_code = 'CDIV' THEN
                DATE_TRUNC('month', TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD'))
            ELSE NULL
        END AS div_period_start_date,

        -- Calculate dividend period end date
        CASE
            WHEN t.raw_trans_code = 'CDIV' THEN
                DATE_TRUNC('month', TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD')) + INTERVAL '1 month - 1 day'
            ELSE NULL
        END AS div_period_end_date,

        -- Calculate period start and end dates
        DATE_TRUNC('month', COALESCE(TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD'), t.activity_date)) AS period_start_date,
        DATE_TRUNC('month', COALESCE(TO_DATE(SUBSTRING(t.description, 'R/D (\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD'), t.activity_date)) + INTERVAL '1 month - 1 day' AS period_end_date,

        -- Normalize cash flow: Buy -> positive, Sell -> negative
        CASE
            WHEN t.raw_trans_code = 'Buy' THEN ABS(t.amount)
            WHEN t.raw_trans_code = 'Sell' THEN -ABS(t.amount)
            ELSE t.amount
        END AS cash_flow
    FROM transactions t
),
calculated_dimensions AS (
    SELECT
        bd.*,
        -- Calculate T (total days in the period)
        DATE_PART('day', bd.period_end_date - bd.period_start_date + INTERVAL '1 day') AS T,

        -- Calculate Ti (days since period start, based on record_date for CDIV or activity_date for others)
        DATE_PART('day', COALESCE(bd.record_date, bd.activity_date) - bd.period_start_date) AS Ti,

        -- Calculate weight
        ((DATE_PART('day', bd.period_end_date - COALESCE(bd.record_date, bd.activity_date) + INTERVAL '1 day')) /
         DATE_PART('day', bd.period_end_date - bd.period_start_date + INTERVAL '1 day')) AS weight
    FROM base_data bd
)
SELECT
    transaction_id,
    activity_date,
    process_date,
    settle_date,
    raw_trans_code,
    trans_code,
    instrument,
    description,
    quantity,
    price,
    amount,
    raw_quantity,
    raw_price,
    raw_amount,
    record_date,
    payment_date,
    expiration_date,
    option_type,
    strike_price,
    div_period_start_date,
    div_period_end_date,
    period_start_date,
    period_end_date,
    cash_flow,
    T::INTEGER AS T,
    Ti::INTEGER AS Ti,
    weight,
     -- New column for corrected_activity_date
    CASE
        WHEN raw_trans_code = 'CDIV' THEN record_date
        ELSE activity_date
    END AS corrected_activity_date
FROM calculated_dimensions;