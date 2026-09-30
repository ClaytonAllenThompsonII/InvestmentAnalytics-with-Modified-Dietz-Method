-- View: stage.enriched_transactions_view
-- NOTE: Dividend econ-date logic updated to use PAYMENT DATE (P/D) instead of RECORD DATE (R/D)
--       This affects corrected_activity_date AND the ti/weight calculations.

CREATE OR REPLACE VIEW stage.enriched_transactions_view AS
WITH base_data AS (
    SELECT
        t.transaction_id,
        t.activity_date,
        t.process_date,
        t.settle_date,
        t.raw_trans_code,
        t.trans_code,
        t.instrument,
        CASE
            WHEN t.instrument::text = 'FB'::text THEN 'META'::character varying
            ELSE t.instrument
        END AS normalized_instrument,
        t.description,
        t.quantity,
        t.price,
        t.amount,
        t.raw_quantity,
        t.raw_price,
        t.raw_amount,

        CASE
            WHEN t.raw_trans_code::text = 'CDIV'::text
             AND t.description ~~ '%R/D%'::text
            THEN to_date(substring(t.description, 'R/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)
            ELSE NULL::date
        END AS record_date,

        CASE
            WHEN t.raw_trans_code::text = 'CDIV'::text
             AND t.description ~~ '%P/D%'::text
            THEN to_date(substring(t.description, 'P/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)
            ELSE NULL::date
        END AS payment_date,

        CASE
            WHEN (t.raw_trans_code::text = ANY (ARRAY[
                'BTC'::character varying,'STC'::character varying,'BTO'::character varying,
                'STO'::character varying,'OCA'::character varying,'OEXP'::character varying
            ]::text[]))
            AND t.description ~ '\d{1,2}/\d{1,2}/\d{4}'::text
            THEN to_date(substring(t.description, '(\d{1,2}/\d{1,2}/\d{4})'::text), 'MM/DD/YYYY'::text)
            ELSE NULL::date
        END AS expiration_date,

        CASE
            WHEN t.raw_trans_code::text = ANY (ARRAY[
                'BTC'::character varying,'STC'::character varying,'BTO'::character varying,
                'STO'::character varying,'OCA'::character varying,'OEXP'::character varying
            ]::text[])
            THEN CASE
                WHEN t.description ~~* '%Call%'::text THEN 'Call'::text
                WHEN t.description ~~* '%Put%'::text  THEN 'Put'::text
                ELSE NULL::text
            END
            ELSE NULL::text
        END AS option_type,

        CASE
            WHEN (t.raw_trans_code::text = ANY (ARRAY[
                'BTC'::character varying,'STC'::character varying,'BTO'::character varying,
                'STO'::character varying,'OCA'::character varying,'OEXP'::character varying
            ]::text[]))
            AND t.description ~ '\$(\d+\.?\d*)'::text
            THEN substring(t.description, '\$(\d+\.?\d*)'::text)::numeric
            ELSE NULL::numeric
        END AS strike_price,

        CASE
            WHEN t.raw_trans_code::text = 'CDIV'::text
            THEN date_trunc('month'::text,
                 to_date(substring(t.description, 'R/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)::timestamptz
            )
            ELSE NULL::timestamptz
        END AS div_period_start_date,

        CASE
            WHEN t.raw_trans_code::text = 'CDIV'::text
            THEN date_trunc('month'::text,
                 to_date(substring(t.description, 'R/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)::timestamptz
            ) + '1 mon -1 days'::interval
            ELSE NULL::timestamptz
        END AS div_period_end_date,

        -- IMPORTANT: for dividend events, period assignment should follow PAYMENT DATE for cash timing.
        date_trunc(
            'month'::text,
            COALESCE(
                CASE WHEN t.raw_trans_code::text = 'CDIV'::text
                     THEN to_date(substring(t.description, 'P/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)
                END,
                to_date(substring(t.description, 'R/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text),
                t.activity_date
            )::timestamptz
        ) AS period_start_date,

        date_trunc(
            'month'::text,
            COALESCE(
                CASE WHEN t.raw_trans_code::text = 'CDIV'::text
                     THEN to_date(substring(t.description, 'P/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text)
                END,
                to_date(substring(t.description, 'R/D (\d{4}-\d{2}-\d{2})'::text), 'YYYY-MM-DD'::text),
                t.activity_date
            )::timestamptz
        ) + '1 mon -1 days'::interval AS period_end_date,

        CASE
            WHEN t.raw_trans_code::text = 'Buy'::text  THEN abs(t.amount)
            WHEN t.raw_trans_code::text = 'Sell'::text THEN -abs(t.amount)
            ELSE t.amount
        END AS cash_flow

    FROM transactions t
),
calculated_dimensions AS (
    SELECT
        bd.*,

        -- "effective event date" for timing (PAYMENT DATE for dividends)
        COALESCE(
            CASE WHEN bd.raw_trans_code::text = 'CDIV'::text THEN bd.payment_date END,
            bd.record_date,
            bd.activity_date
        )::timestamptz AS event_ts,

        date_part('day'::text, bd.period_end_date - bd.period_start_date + '1 day'::interval) AS t,

        date_part('day'::text,
            COALESCE(
                CASE WHEN bd.raw_trans_code::text = 'CDIV'::text THEN bd.payment_date END,
                bd.record_date,
                bd.activity_date
            )::timestamptz - bd.period_start_date
        ) AS ti,

        date_part('day'::text,
            bd.period_end_date
            - COALESCE(
                CASE WHEN bd.raw_trans_code::text = 'CDIV'::text THEN bd.payment_date END,
                bd.record_date,
                bd.activity_date
            )::timestamptz
            + '1 day'::interval
        )
        / date_part('day'::text, bd.period_end_date - bd.period_start_date + '1 day'::interval) AS weight

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
    normalized_instrument,
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
    t::integer AS t,
    ti::integer AS ti,
    weight,

    -- corrected activity date: PAYMENT DATE for dividends, else activity date
    CASE
        WHEN raw_trans_code::text = 'CDIV'::text
            THEN COALESCE(payment_date, record_date, activity_date)::date
        ELSE activity_date::date
    END AS corrected_activity_date
FROM calculated_dimensions;

-- ALTER TABLE stage.enriched_transactions_view OWNER TO postgres;