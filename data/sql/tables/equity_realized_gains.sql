CREATE TABLE IF NOT EXISTS equity_realized_gains (
    realized_id         BIGSERIAL PRIMARY KEY,
    instrument          TEXT       NOT NULL,
    sell_transaction_id BIGINT     NOT NULL,       -- which 'Sell' triggered this realization
    lot_id              BIGINT     NOT NULL,       -- which lot we consumed
    allocated_quantity  NUMERIC    NOT NULL,       -- how many shares came out of that lot
    allocated_cost      NUMERIC    NOT NULL,       -- cost basis portion for these shares
    proceeds            NUMERIC    NOT NULL,       -- actual sale proceeds for these shares
    realized_gain       NUMERIC    NOT NULL,       -- proceeds - allocated_cost
    created_at          TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_equity_realized_gains_instr 
    ON equity_realized_gains (instrument);