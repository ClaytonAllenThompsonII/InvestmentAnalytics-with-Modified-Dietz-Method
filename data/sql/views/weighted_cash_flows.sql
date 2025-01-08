CREATE OR REPLACE VIEW weighted_cash_flows AS
SELECT 
    t.activity_date,
    t.instrument,
    t.raw_trans_code AS event_type,
    t.quantity,
    t.amount,
    
    -- Total number of days in the period (T)
    DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day') AS total_days_in_period,
    
    -- Days since the beginning of the period (t_i)
    DATE_PART('day', t.activity_date) AS days_since_start,
    
    -- Difference (numerator for weight calculation): (T - t_i + 1)
    (DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day') - DATE_PART('day', t.activity_date) + 1) AS days_remaining,
    
    -- Weight calculation: (T - t_i + 1) / T
    ((DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day') 
      - DATE_PART('day', t.activity_date) + 1) /
     DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day')) 
    AS weight,
    
    -- Weighted Cash Flow
    (t.amount * ((DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day') 
                  - DATE_PART('day', t.activity_date) + 1) /
                 DATE_PART('day', DATE_TRUNC('month', t.activity_date) + INTERVAL '1 month' - INTERVAL '1 day'))) 
    AS weighted_cash_flow
FROM transactions t
WHERE t.raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL')
ORDER BY t.activity_date;