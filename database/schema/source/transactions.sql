DROP TABLE IF EXISTS transactions;

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    activity_date DATE,
    process_date DATE,
    settle_date DATE,
    
    raw_trans_code VARCHAR(20),  -- e.g. "ACH", "BTO", "CDIV", etc. (from CSV)
    trans_code VARCHAR(50),      -- e.g. "Automated Clearing House", "Buy to Open", "Dividend", etc.
    
    instrument VARCHAR(50),      -- e.g. "AAPL", "CASH", etc.
    description TEXT,            -- full text description from CSV
    
    quantity NUMERIC,            -- e.g. number of shares or contracts
    price NUMERIC,               -- e.g. cost basis or trade price
    amount NUMERIC,              -- e.g. -58.19 for a buy, 1.98 for a dividend, etc.
    
    raw_quantity VARCHAR(50),    -- original CSV string for debugging
    raw_price VARCHAR(50),       -- original CSV string
    raw_amount VARCHAR(50)       -- original CSV string
);