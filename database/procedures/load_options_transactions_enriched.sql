CREATE OR REPLACE PROCEDURE load_options_transactions_enriched()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting load of enriched options transactions...';
    
    -- 1) Clear existing data in our new table.
    TRUNCATE TABLE options_transactions_enriched RESTART IDENTITY;
    
    -- 2) Insert rows from the enriched view in the desired order.
    INSERT INTO options_transactions_enriched (
        transaction_id,
        activity_date,
        process_date,
        settle_date,
        raw_trans_code,
        trans_code,
        instrument,
        normalized_instrument,  -- new column inserted after instrument
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
        T,
        Ti,
        weight,
        corrected_activity_date
    )
    SELECT
        transaction_id,
        activity_date,
        process_date,
        settle_date,
        raw_trans_code,
        trans_code,
        instrument,
        normalized_instrument,  -- select the normalized instrument from the view
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
        T::INTEGER,
        Ti::INTEGER,
        weight,
        corrected_activity_date
    FROM enriched_transactions_view
    WHERE raw_trans_code IN ('BTO','STO','STC','BTC','OEXP')
          OR option_type IS NOT NULL
          OR strike_price IS NOT NULL
    ORDER BY 
        expiration_date, 
        option_type, 
        strike_price, 
        description,
        CASE 
            WHEN raw_trans_code IN ('BTO', 'STO') THEN 1
            WHEN raw_trans_code IN ('STC', 'BTC') THEN 2
            WHEN raw_trans_code = 'OEXP' THEN 3
            ELSE 99
        END,
        transaction_id;
        
    RAISE NOTICE 'Load complete. Total rows loaded: %', (SELECT COUNT(*) FROM options_transactions_enriched);
END;
$$;