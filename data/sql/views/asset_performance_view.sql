CREATE OR REPLACE VIEW asset_performance_view AS

WITH time_series_transactions AS (
    SELECT
        ts.instrument,
        ts.period_start_date,
        ts.period_end_date,
        COALESCE(ea.total_buys, 0)         AS total_buys,
        COALESCE(ea.total_sells, 0)        AS total_sells,
        COALESCE(ea.total_splits, 0)       AS total_splits,
        COALESCE(ea.net_cash_flow, 0)      AS net_cash_flow,
        COALESCE(ea.weighted_cash_flow, 0) AS weighted_cash_flow,
        COALESCE(ea.fees_and_taxes, 0)     AS fees_and_taxes
    FROM time_series ts
    LEFT JOIN enriched_transactions_agg ea
        ON  ts.instrument = ea.instrument
        AND ts.period_start_date = ea.period_start_date
        AND ts.period_end_date   = ea.period_end_date
),

.
prices AS (
    SELECT
        md.instrument,
        DATE_TRUNC('month', md.price_date) AS price_month,
        FIRST_VALUE(md.close_price) OVER (
            PARTITION BY md.instrument, DATE_TRUNC('month', md.price_date)
            ORDER BY md.price_date
        ) AS bom_price_raw,
        LAST_VALUE(md.close_price) OVER (
            PARTITION BY md.instrument, DATE_TRUNC('month', md.price_date)
            ORDER BY md.price_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS eom_price_raw,
        md.price_date
    FROM market_data md
),
merged_data AS (
    SELECT DISTINCT ON (tst.instrument, tst.period_start_date)
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
        AND tst.period_start_date = p.price_month
    ORDER BY tst.instrument, tst.period_start_date, p.price_date
),
cumulative_shares AS (
    SELECT
        md.*,
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

    -- Share flow
    fd.total_buys,
    fd.total_sells,
    fd.total_splits,

    -- Share positions
    fd.bom_shares_cumulative,
    fd.eom_shares_cumulative,

    -- Prices
    fd.bom_price,
    fd.eom_price,

    -- NAVs
    fd.bom_shares_cumulative * fd.bom_price AS nav_bom,
    fd.eom_shares_cumulative * fd.eom_price AS nav_eom,

    -- Cash flows
    fd.net_cash_flow,
    fd.weighted_cash_flow,
    fd.fees_and_taxes,

    -- PNL Gross
    (fd.eom_shares_cumulative * fd.eom_price)
    - (fd.bom_shares_cumulative * fd.bom_price)
    - fd.net_cash_flow
    + fd.fees_and_taxes
    AS pnl_gross,

    -- PNL Net
    (fd.eom_shares_cumulative * fd.eom_price)
    - (fd.bom_shares_cumulative * fd.bom_price)
    - fd.net_cash_flow
    AS pnl_net,

    -- Average Capital for Modified Dietz (Gross)
    CASE
        WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow
        WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow)
        ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + fd.weighted_cash_flow
    END AS avg_capital_gross,

    -- Average Capital for Modified Dietz (Net)
    CASE
        WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow - fd.fees_and_taxes
        WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow - fd.fees_and_taxes)
        ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + (fd.weighted_cash_flow - fd.fees_and_taxes)
    END AS avg_capital_net,

    -- Modified Dietz Return (Gross)
    CASE
        WHEN (
            CASE
                WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow
                WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow)
                ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + fd.weighted_cash_flow
            END
        ) != 0 THEN
            ROUND(
                (
                    (fd.eom_shares_cumulative * fd.eom_price) - (fd.bom_shares_cumulative * fd.bom_price) - fd.net_cash_flow
                ) /
                (
                    CASE
                        WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow
                        WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow)
                        ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + fd.weighted_cash_flow
                    END
                )::numeric,
                6
            )
        ELSE NULL
    END AS md_return_gross,

    -- Modified Dietz Return (Net)
    CASE
        WHEN (
            CASE
                WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow - fd.fees_and_taxes
                WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow - fd.fees_and_taxes)
                ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + (fd.weighted_cash_flow - fd.fees_and_taxes)
            END
        ) != 0 THEN
            ROUND(
                (
                    (fd.eom_shares_cumulative * fd.eom_price) - (fd.bom_shares_cumulative * fd.bom_price) - (fd.net_cash_flow - fd.fees_and_taxes)
                ) /
                (
                    CASE
                        WHEN fd.bom_shares_cumulative = 0 AND fd.eom_shares_cumulative > 0 THEN fd.net_cash_flow - fd.fees_and_taxes
                        WHEN fd.bom_shares_cumulative > 0 AND fd.eom_shares_cumulative = 0 THEN ABS(fd.net_cash_flow - fd.fees_and_taxes)
                        ELSE COALESCE(fd.bom_shares_cumulative * fd.bom_price, 0) + (fd.weighted_cash_flow - fd.fees_and_taxes)
                    END
                )::numeric,
                6
            )
        ELSE NULL
    END AS md_return_net

FROM final_data fd
ORDER BY fd.instrument, fd.period_start_date;