CREATE TABLE IF NOT EXISTS option_realized_gains (
    realized_id          BIGSERIAL PRIMARY KEY,

    instrument           TEXT       NOT NULL,
    description          TEXT       NOT NULL,

    expiration_date      DATE       NOT NULL,
    option_type          TEXT       NOT NULL,
    strike_price         NUMERIC    NOT NULL,

    -- The transaction that triggered the close (STC, BTC, or OEXP)
    close_transaction_id BIGINT     NOT NULL,

    -- Which lot we took these contracts from:
    lot_id               BIGINT     NOT NULL
      REFERENCES fifo_option_lots(lot_id),

    -- How many contracts were closed:
    allocated_contracts  NUMERIC    NOT NULL,

    -- The portion of the original cost basis that belongs to these contracts:
    allocated_cost       NUMERIC    NOT NULL,

    -- The actual proceeds (could be negative if we pay to close a short):
    proceeds             NUMERIC    NOT NULL,

    -- realized_gain = proceeds - allocated_cost
    realized_gain        NUMERIC    NOT NULL,

    created_at           TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_option_realized_gains_keys
  ON option_realized_gains (
    instrument, expiration_date, option_type, strike_price
  );