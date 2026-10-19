-- Table: core.option_contracts
--
-- Purpose:
-- Maintain canonical option contract identities and terms
-- independently of brokerage transaction codes and
-- market-data provider identifiers.
--
-- Notes:
-- 1. Internal contract IDs remain stable across ticker changes.
-- 2. Historical contract symbols will be maintained separately.
-- 3. Standard equity options generally have a premium
--    multiplier of 100.
-- 4. Adjusted contracts may require nonstandard deliverables.
-- 5. Contract identities are independent of brokerage accounts.

CREATE TABLE IF NOT EXISTS core.option_contracts (

    option_contract_id BIGINT GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    -- Canonical underlying symbol
    underlying_symbol VARCHAR(32) NOT NULL,

    -- Contract terms
    expiration_date DATE NOT NULL,

    option_type VARCHAR(4) NOT NULL,

    strike_price NUMERIC(18,4) NOT NULL,

    -- Converts quoted option premium to contract value
    premium_multiplier NUMERIC(18,4) NOT NULL DEFAULT 100,

    -- Corporate action / adjusted contract information
    is_adjusted BOOLEAN NOT NULL DEFAULT FALSE,

    deliverable_description TEXT,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Validation
    CONSTRAINT chk_option_type
        CHECK (option_type IN ('call', 'put')),

    CONSTRAINT chk_option_strike
        CHECK (strike_price > 0),

    CONSTRAINT chk_option_premium_multiplier
        CHECK (premium_multiplier > 0)

);


-- Prevent duplicate standard contracts.
-- Adjusted contracts may require additional identifiers.

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_option_contracts_standard_unique

ON core.option_contracts (
    underlying_symbol,
    expiration_date,
    option_type,
    strike_price
)

WHERE is_adjusted = FALSE;