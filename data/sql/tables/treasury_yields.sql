DROP TABLE IF EXISTS treasury_yields;

CREATE TABLE treasury_yields (
    yield_date DATE NOT NULL,
    maturity TEXT NOT NULL,                -- e.g., '3month', '2year', '10year'
    yield_percent NUMERIC(5, 2) NOT NULL,  -- e.g., 4.57 (%)
    interval TEXT NOT NULL DEFAULT 'daily',-- 'daily', 'weekly', or 'monthly'
    source TEXT DEFAULT 'AlphaVantage',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (yield_date, maturity)     --  composite primary key
);