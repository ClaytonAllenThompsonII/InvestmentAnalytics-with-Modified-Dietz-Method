DROP TABLE IF EXISTS asset_value;

CREATE TABLE asset_value (
    asset_value_id SERIAL PRIMARY KEY,
    as_of_date DATE NOT NULL,
    instrument VARCHAR(50) NOT NULL,
    shares_bom NUMERIC, -- Shares at the beginning of the month
    shares_eom NUMERIC, -- Shares at the end of the month
    price_bom NUMERIC, -- Beginning of month price
    price_eom NUMERIC, -- End of month price
    net_cf NUMERIC, -- Net cash flow during the period
    wcf NUMERIC, -- Weighted cash flow
    realized_pnl NUMERIC, -- Realized P&L for the period
    unrealized_pnl NUMERIC, -- Unrealized P&L due to price changes
    average_capital NUMERIC, -- Average capital for the period
    CONSTRAINT uq_asset_value UNIQUE (as_of_date, instrument)
);