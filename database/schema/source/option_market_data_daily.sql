-- Table: source.option_market_data_daily
--
-- Purpose:
-- Store historical options market data retrieved from
-- external providers, initially Alpha Vantage.
--
-- Notes:
-- 1. Preserve provider-specific contract identifiers.
-- 2. Retain original market quotations and Greeks.
-- 3. Missing numeric attributes are stored as NULL.
-- 4. Valuation-quality assessment occurs downstream.
-- 5. One observation per provider, contract, and date.
-- 6. Preserve the original API record as JSONB.

CREATE TABLE IF NOT EXISTS source.option_market_data_daily (

    option_market_data_id BIGINT
        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Provider identity
    data_source VARCHAR(32) NOT NULL
        DEFAULT 'alpha_vantage',

    contract_symbol VARCHAR(64) NOT NULL,
    underlying_symbol VARCHAR(32) NOT NULL,

    -- Contract attributes
    price_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    option_type VARCHAR(4) NOT NULL,
    strike_price NUMERIC(18,4) NOT NULL,

    -- Historical quotations
    last_price NUMERIC(18,6),
    mark_price NUMERIC(18,6),
    bid_price NUMERIC(18,6),
    ask_price NUMERIC(18,6),

    -- Market liquidity
    bid_size BIGINT,
    ask_size BIGINT,
    volume BIGINT,
    open_interest BIGINT,

    -- Implied volatility and Greeks
    implied_volatility NUMERIC(20,8),
    delta NUMERIC(20,8),
    gamma NUMERIC(20,8),
    theta NUMERIC(20,8),
    vega NUMERIC(20,8),
    rho NUMERIC(20,8),

    -- Original provider record
    raw_payload JSONB NOT NULL,

    -- Ingestion metadata
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_option_market_data_daily
        UNIQUE (
            data_source,
            contract_symbol,
            price_date
        ),

    CONSTRAINT chk_option_market_data_type
        CHECK (option_type IN ('call', 'put')),

    CONSTRAINT chk_option_market_data_strike
        CHECK (strike_price > 0)
);


-- Support historical date-range retrieval.

CREATE INDEX IF NOT EXISTS
    idx_option_market_data_daily_date
ON source.option_market_data_daily (
    price_date
);


-- Support underlying-level retrieval.

CREATE INDEX IF NOT EXISTS
    idx_option_market_data_daily_underlying_date
ON source.option_market_data_daily (
    underlying_symbol,
    price_date
);