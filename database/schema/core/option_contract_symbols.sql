-- Table: core.option_contract_symbols
--
-- Purpose:
-- Map canonical option contracts to historical and
-- provider-specific contract identifiers.
--
-- Dependencies:
-- core.option_contracts
--
-- Notes:
-- 1. One canonical contract can have multiple symbols.
-- 2. Preserve historical symbols across ticker changes.
-- 3. Track the source system associated with each symbol.
-- 4. Effective dates are optional when not yet verified.

CREATE TABLE IF NOT EXISTS core.option_contract_symbols (

    option_contract_symbol_id BIGINT
        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    option_contract_id BIGINT NOT NULL
        REFERENCES core.option_contracts(option_contract_id),

    -- External contract identifier
    contract_symbol VARCHAR(64) NOT NULL,

    -- Identifier source
    source_system VARCHAR(32) NOT NULL,

    -- Historical validity
    valid_from DATE,
    valid_to DATE,

    -- Verification
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    verified_price_date DATE,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_option_symbol_dates
        CHECK (
            valid_to IS NULL
            OR valid_from IS NULL
            OR valid_to >= valid_from
        ),

    CONSTRAINT chk_option_symbol_verification
        CHECK (
            is_verified = FALSE
            OR verified_price_date IS NOT NULL
        ),

    CONSTRAINT uq_option_symbol_source
        UNIQUE (source_system, contract_symbol)
);

CREATE INDEX IF NOT EXISTS
    idx_option_contract_symbols_contract_id
ON core.option_contract_symbols(option_contract_id);