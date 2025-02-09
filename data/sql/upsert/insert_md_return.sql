INSERT INTO asset_value (
    as_of_date,
    instrument,
    shares_bom,
    shares_eom,
    price_bom,
    price_eom,
    net_cf,
    wcf,
    nav_bom,
    nav_eom,
    pnl,
    average_capital,
    md_return,
    total_buys,
    total_sells,
    total_splits
)
SELECT
    period_end_date AS as_of_date,
    instrument,
    bom_shares_cumulative AS shares_bom,
    eom_shares_cumulative AS shares_eom,
    bom_price AS price_bom,
    eom_price AS price_eom,
    net_cash_flow AS net_cf,
    weighted_cash_flow AS wcf,
    nav_bom,
    nav_eom,
    pnl,
    average_capital,
    md_return,
    total_buys,
    total_sells,
    total_splits
FROM
    asset_value_view
WHERE instrument IS NOT NULL
ON CONFLICT (as_of_date, instrument)
DO UPDATE
SET
    shares_bom = EXCLUDED.shares_bom,
    shares_eom = EXCLUDED.shares_eom,
    price_bom = EXCLUDED.price_bom,
    price_eom = EXCLUDED.price_eom,
    net_cf = EXCLUDED.net_cf,
    wcf = EXCLUDED.wcf,
    nav_bom = EXCLUDED.nav_bom,
    nav_eom = EXCLUDED.nav_eom,
    pnl = EXCLUDED.pnl,
    average_capital = EXCLUDED.average_capital,
    md_return = EXCLUDED.md_return,
    total_buys = EXCLUDED.total_buys,
    total_sells = EXCLUDED.total_sells,
    total_splits = EXCLUDED.total_splits;