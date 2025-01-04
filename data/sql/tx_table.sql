CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    activity_date DATE,
    process_date DATE,
    settle_date DATE,
    instrument VARCHAR(50),         -- e.g. AAPL, AMD, etc.
    description TEXT,               -- Full text description
    trans_code VARCHAR(20),         -- e.g. Buy, Sell, BTO, STO, etc.
    quantity NUMERIC,               -- e.g. 5, 1, etc.
    price NUMERIC,                  -- e.g. 445.21
    amount NUMERIC,                 -- e.g. -58.19
    raw_quantity VARCHAR(50),       -- optional: store raw CSV value for debugging
    raw_price VARCHAR(50),          -- optional
    raw_amount VARCHAR(50)          -- optional
);