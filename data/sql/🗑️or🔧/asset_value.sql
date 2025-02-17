-- Drop the table if it exists
DROP TABLE IF EXISTS asset_value;

-- Create the updated asset_value table
CREATE TABLE asset_value (
    asset_value_id SERIAL PRIMARY KEY, -- Auto-incrementing ID for each record
    as_of_date DATE NOT NULL, -- Monthly periods, always last day of the month
    instrument VARCHAR(50) NOT NULL, -- Instrument identifier
    shares_bom NUMERIC, -- Shares at the beginning of the month
    shares_eom NUMERIC, -- Shares at the end of the month
    price_bom NUMERIC, -- Beginning of month price
    price_eom NUMERIC, -- End of month price
    net_cf NUMERIC, -- Net cash flow during the period
    wcf NUMERIC, -- Weighted cash flow during the period
    nav_bom NUMERIC, -- NAV at the beginning of the month
    nav_eom NUMERIC, -- NAV at the end of the month
    pnl NUMERIC, -- Profit and Loss for the period
    average_capital NUMERIC, -- Average capital for the period
    md_return NUMERIC, -- Modified Dietz return
    total_buys NUMERIC, -- Total shares bought during the period
    total_sells NUMERIC, -- Total shares sold during the period
    total_splits NUMERIC, -- Total splits during the period
    realized_gains NUMERIC DEFAULT 0, -- Realized gains during the period
    unrealized_gains NUMERIC DEFAULT 0, -- Unrealized gains (based on eom price)
    position_status VARCHAR(10) DEFAULT 'Open', -- Open or Closed position status
    CONSTRAINT uq_asset_value UNIQUE (as_of_date, instrument) -- Ensure no duplicate records for the same date and instrument
);