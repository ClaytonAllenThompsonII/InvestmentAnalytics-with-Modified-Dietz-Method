CREATE TABLE IF NOT EXISTS fifo_equity_lots (
    lot_id                  BIGSERIAL PRIMARY KEY,
    instrument              TEXT       NOT NULL,
    buy_transaction_id      BIGINT     NOT NULL,   -- which 'Buy' transaction created this lot
    lot_open_date           DATE       NOT NULL,   -- date of the buy
    open_quantity           NUMERIC    NOT NULL,   -- how many shares remain in this lot
    total_cost              NUMERIC    NOT NULL,   -- total cost for the open shares
    avg_price               NUMERIC    NOT NULL,   -- convenience: total_cost / open_quantity
    created_at              TIMESTAMP  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fifo_equity_lots_instr 
    ON fifo_equity_lots (instrument);