CREATE OR REPLACE VIEW asset_value_view AS

WITH time_series_transactions AS (
    SELECT
        ts.instrument,
        ts.period_start_date,
        ts.period_end_date,
        -- Already aggregated columns
        COALESCE(ea.total_buys, 0)         AS total_buys,
        COALESCE(ea.total_sells, 0)        AS total_sells,
        COALESCE(ea.total_splits, 0)       AS total_splits,
        COALESCE(ea.net_cash_flow, 0)      AS net_cash_flow,
        COALESCE(ea.weighted_cash_flow, 0) AS weighted_cash_flow
        -- If you want the JSON array:
        -- COALESCE(ea.monthly_transactions, '[]'::json) AS monthly_transactions
    FROM time_series ts
    LEFT JOIN enriched_transactions_agg ea
        ON  ts.instrument        = ea.instrument
        AND ts.period_start_date = ea.period_start_date
        AND ts.period_end_date   = ea.period_end_date
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
            ORDER BY md.price_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS eom_price_raw
    FROM market_data md
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
    FROM time_series_transactions tst
    LEFT JOIN prices p
        ON  tst.instrument = p.instrument
        AND tst.period_start_date = DATE_TRUNC('month', p.price_date)
),
cumulative_shares AS (
    SELECT
        md.*,
        -- EOM shares after this month's net change:
        SUM(
            md.total_buys 
            - md.total_sells
            + md.total_splits
        ) OVER (
            PARTITION BY md.instrument
            ORDER BY md.period_start_date
        ) AS eom_shares_cumulative
    FROM merged_data md
),
final_data AS (
    SELECT
        cs.*,
        COALESCE(
            LAG(cs.eom_shares_cumulative) OVER (PARTITION BY cs.instrument ORDER BY cs.period_start_date),
            0
        ) AS bom_shares_cumulative
    FROM cumulative_shares cs
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

    -- PNL
    ROUND(
      (fd.eom_shares_cumulative * fd.eom_price)
      - (fd.bom_shares_cumulative * fd.bom_price)
      - fd.net_cash_flow
    , 2) AS pnl,

    -- Average Capital (Modified Dietz denominator)
    CASE
    -- First record: Opening (or re-opening) a position
    WHEN bom_shares_cumulative = 0 
         AND eom_shares_cumulative > 0
    THEN COALESCE(net_cash_flow, 0)

    -- Last record: Closing a position
    WHEN bom_shares_cumulative > 0 
         AND eom_shares_cumulative = 0
    THEN ABS(COALESCE(net_cash_flow, 0))

    -- Intermediate records: Ongoing position
    ELSE 
        COALESCE((bom_shares_cumulative * bom_price), 0) 
        + COALESCE(weighted_cash_flow, 0)
    END AS avg_capital,

     -- Updated Modified Dietz Return
    CASE 
        WHEN 
            /* Reuse that same CASE logic or nest the CASE inline. 
               For clarity, I'll just inline the same CASE expression 
               (or you can reference a subselect). 
            */
            CASE
                WHEN fd.bom_shares_cumulative = 0
                     AND fd.eom_shares_cumulative > 0
                THEN COALESCE(fd.net_cash_flow, 0)
                
                WHEN fd.bom_shares_cumulative > 0
                     AND fd.eom_shares_cumulative = 0
                THEN ABS(COALESCE(fd.net_cash_flow, 0))

                ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) 
                     + COALESCE(fd.weighted_cash_flow, 0)
            END 
            != 0
        THEN
            ROUND(
                (
                  (
                    (fd.eom_shares_cumulative * fd.eom_price)
                    - (fd.bom_shares_cumulative * fd.bom_price)
                    - fd.net_cash_flow
                  )
                  /
                  (
                    CASE
                      WHEN fd.bom_shares_cumulative = 0
                           AND fd.eom_shares_cumulative > 0
                      THEN COALESCE(fd.net_cash_flow, 0)
                      
                      WHEN fd.bom_shares_cumulative > 0
                           AND fd.eom_shares_cumulative = 0
                      THEN ABS(COALESCE(fd.net_cash_flow, 0))

                      ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0)
                           + COALESCE(fd.weighted_cash_flow, 0)
                    END
                  )
                )::numeric,
                6
            )
        ELSE NULL
    END AS md_return

FROM final_data fd
ORDER BY
    fd.instrument, fd.period_start_date;