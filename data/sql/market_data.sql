DROP TABLE IF EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data (
    market_data_id SERIAL PRIMARY KEY,
    
    -- We store only the "real" (mapped) symbol, e.g. "META" not "FB"
    instrument    VARCHAR(20) NOT NULL,  
    price_date    DATE        NOT NULL,

    open_price    NUMERIC,
    high_price    NUMERIC,
    low_price     NUMERIC,
    close_price   NUMERIC,
    adj_close     NUMERIC,
    volume        BIGINT,

    currency      VARCHAR(10),
    exchange      VARCHAR(20),

    -- Unique constraint so each (instrument, price_date) is unique
    CONSTRAINT uq_instrument_date UNIQUE (instrument, price_date)
);