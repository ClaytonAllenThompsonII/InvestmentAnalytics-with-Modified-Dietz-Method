CREATE TABLE weighted_cash_flows (
    wcf_id SERIAL PRIMARY KEY,
    instrument        VARCHAR(50) NOT NULL,   -- E.g., AAPL
    as_of_date        DATE NOT NULL,          -- Last day of the month
    activity_date     DATE NOT NULL,          -- Date of the transaction
    event_type        VARCHAR(50) NOT NULL,   -- Buy, Sell, Dividend, Split, etc.
    amount            NUMERIC,                -- Cash flow amount (e.g., -445.21 for Buy)
    t_days            INT,                    -- Total days in the period (e.g., 31 for August)
    t_i_days          INT,                    -- Days before transaction in the period
    weight            NUMERIC,                -- Weight = t_i_days / t_days
    weighted_cf       NUMERIC,                -- Weighted Contribution = weight * amount
    
    UNIQUE (instrument, as_of_date, activity_date, event_type)
);