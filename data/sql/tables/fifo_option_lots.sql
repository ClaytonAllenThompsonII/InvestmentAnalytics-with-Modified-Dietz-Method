CREATE TABLE IF NOT EXISTS fifo_option_lots (
    lot_id               BIGSERIAL PRIMARY KEY,

    -- E.g. 'SNAP' or 'SQ' (the underlying ticker)
    instrument           TEXT       NOT NULL, 

    -- Full contract description if you like: 'SQ 8/7/2020 Put $119.00'
    description          TEXT       NOT NULL,

    expiration_date      DATE       NOT NULL,
    option_type          TEXT       NOT NULL,  -- 'Call' or 'Put'
    strike_price         NUMERIC    NOT NULL,

    -- Positive if a long position, negative if short
    open_contracts       NUMERIC    NOT NULL,

    -- The total cost or credit for these open contracts:
    --   e.g. +500 if a net debit (long), or -500 if a net credit (short).
    total_cost           NUMERIC    NOT NULL,

    -- cost per contract = total_cost / open_contracts
    avg_cost             NUMERIC    NOT NULL,

    open_date            DATE       NOT NULL,
    open_transaction_id  BIGINT     NOT NULL,

    created_at           TIMESTAMP  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fifo_option_lots_keys
  ON fifo_option_lots (
    instrument, expiration_date, option_type, strike_price
  );