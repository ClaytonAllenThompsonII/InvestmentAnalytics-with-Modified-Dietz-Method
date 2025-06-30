CREATE TABLE IF NOT EXISTS options_market_data (
    options_data_id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    contract_id VARCHAR(30) NOT NULL,
    expiration DATE,
    strike NUMERIC,
    type VARCHAR(4),  -- 'call' or 'put'
    date DATE NOT NULL,
    last NUMERIC,
    mark NUMERIC,
    bid NUMERIC,
    ask NUMERIC,
    volume BIGINT,
    open_interest BIGINT,
    implied_volatility NUMERIC,
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,
    rho NUMERIC,
    CONSTRAINT uq_contract_date UNIQUE (contract_id, date)
);