
WITH time_series_transactions AS (
    SELECT
        ts.instrument,
        ts.period_start_date,
        ts.period_end_date,
        COALESCE(SUM(CASE WHEN et.raw_trans_code = 'Buy' THEN et.quantity ELSE 0 END), 0) AS total_buys,
        COALESCE(SUM(CASE WHEN et.raw_trans_code = 'Sell' THEN et.quantity ELSE 0 END), 0) AS total_sells,
        COALESCE(SUM(CASE WHEN et.raw_trans_code = 'SPL' THEN et.quantity ELSE 0 END), 0) AS total_splits,
        -- Handle net cash flow and weighted cash flow
        COALESCE(SUM(et.cash_flow), 0) AS net_cash_flow,
        COALESCE(SUM(et.cash_flow * et.weight), 0) AS weighted_cash_flow
    FROM
        time_series ts
        LEFT JOIN enriched_transactions_view et
        ON ts.instrument = et.instrument
        AND (
            et.activity_date BETWEEN ts.period_start_date AND ts.period_end_date
        )
    GROUP BY
        ts.instrument, ts.period_start_date, ts.period_end_date
),
prices AS (
    SELECT
        md.instrument,
        md.price_date,
        md.close_price,
        FIRST_VALUE(md.close_price) OVER (
            PARTITION BY md.instrument, DATE_TRUNC('month', md.price_date)
            ORDER BY md.price_date
        ) AS bom_price_raw,
        LAST_VALUE(md.close_price) OVER (
            PARTITION BY md.instrument, DATE_TRUNC('month', md.price_date)
            ORDER BY md.price_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS eom_price_raw
    FROM
        market_data md
),
merged_data AS (
    SELECT
        tst.*,
        p.bom_price_raw,
        p.eom_price_raw,
        COALESCE(
            LAG(p.eom_price_raw) OVER (PARTITION BY tst.instrument ORDER BY tst.period_start_date),
            p.bom_price_raw
        ) AS bom_price,
        p.eom_price_raw AS eom_price
    FROM
        time_series_transactions tst
    LEFT JOIN prices p
    ON tst.instrument = p.instrument
    AND tst.period_start_date = DATE_TRUNC('month', p.price_date)
),
cumulative_shares AS (
    SELECT
        md.*,
        SUM(md.total_buys - md.total_sells + md.total_splits) OVER (
            PARTITION BY md.instrument
            ORDER BY md.period_start_date
        ) AS eom_shares_cumulative
    FROM
        merged_data md
),
final_data AS (
    SELECT
        cs.*,
        COALESCE(
            LAG(cs.eom_shares_cumulative) OVER (PARTITION BY cs.instrument ORDER BY cs.period_start_date),
            0
        ) AS bom_shares_cumulative,
        -- Realized gains calculation
        CASE
            WHEN cs.total_sells > 0 THEN
                cs.total_sells * (cs.bom_price - cs.weighted_cash_flow / GREATEST(cs.total_sells, 1)) -- Using cost basis approximation
            ELSE 0
        END AS realized_gains,
        -- Unrealized gains calculation
        CASE
            WHEN cs.eom_shares_cumulative > 0 THEN
                cs.eom_shares_cumulative * (cs.eom_price - cs.bom_price)
            ELSE 0
        END AS unrealized_gains,
        -- Position status calculation
        CASE
            WHEN cs.eom_shares_cumulative > 0 THEN 'Open'
            ELSE 'Closed'
        END AS position_status
    FROM
        cumulative_shares cs
)
SELECT
    fd.instrument,
    fd.period_start_date,
    fd.period_end_date,
    fd.total_buys,
    fd.total_sells,
    fd.total_splits,
    fd.net_cash_flow,
    fd.weighted_cash_flow,
    fd.bom_price,
    fd.eom_price,
    fd.bom_shares_cumulative,
    fd.eom_shares_cumulative,
    -- NAV calculations
    fd.bom_shares_cumulative * fd.bom_price AS nav_bom,
    fd.eom_shares_cumulative * fd.eom_price AS nav_eom,
    -- PNL calculation
    ROUND((fd.eom_shares_cumulative * fd.eom_price) - (fd.bom_shares_cumulative * fd.bom_price) - fd.net_cash_flow, 2) AS pnl,
    -- Average Capital calculation
    (fd.bom_shares_cumulative * fd.bom_price) + fd.weighted_cash_flow AS average_capital,
    -- Modified Dietz Return
    CASE 
        WHEN ((fd.bom_shares_cumulative * fd.bom_price) + fd.weighted_cash_flow) != 0 THEN 
            ROUND(
                (((fd.eom_shares_cumulative * fd.eom_price) - (fd.bom_shares_cumulative * fd.bom_price) - fd.net_cash_flow) /
                ((fd.bom_shares_cumulative * fd.bom_price) + fd.weighted_cash_flow))::numeric, 6
            )
        ELSE NULL
    END AS md_return,
    fd.realized_gains,
    fd.unrealized_gains,
    fd.position_status
FROM
    final_data fd
ORDER BY
    fd.instrument, fd.period_start_date;