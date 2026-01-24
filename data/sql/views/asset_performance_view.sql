-- ============================================================================
-- patch_asset_performance_view_use_adjusted.sql
-- Update asset_performance_view to source prices from market_data_daily_adjusted
-- using adjusted_close (total-return series).
-- ============================================================================

CREATE OR REPLACE VIEW public.asset_performance_view AS
WITH time_series_transactions AS (
    SELECT
        ts.instrument,
        ts.period_start_date,
        ts.period_end_date,
        COALESCE(ea.total_buys, 0::numeric) AS total_buys,
        COALESCE(ea.total_sells, 0::numeric) AS total_sells,
        COALESCE(ea.total_splits, 0::numeric) AS total_splits,
        COALESCE(ea.net_cash_flow, 0::numeric) AS net_cash_flow,
        COALESCE(ea.weighted_cash_flow, 0::double precision) AS weighted_cash_flow,
        COALESCE(ea.fees_and_taxes, 0::numeric) AS fees_and_taxes
    FROM time_series ts
    LEFT JOIN enriched_transactions_agg ea
      ON ts.instrument::text = ea.instrument::text
     AND ts.period_start_date = ea.period_start_date
     AND ts.period_end_date   = ea.period_end_date
),
prices AS (
    SELECT
        md.instrument,
        date_trunc('month', md.price_date::timestamp with time zone) AS price_month,

        first_value(COALESCE(md.adjusted_close, md.close_price)) OVER (
            PARTITION BY md.instrument, date_trunc('month', md.price_date::timestamp with time zone)
            ORDER BY md.price_date
        ) AS bom_price_raw,

        last_value(COALESCE(md.adjusted_close, md.close_price)) OVER (
            PARTITION BY md.instrument, date_trunc('month', md.price_date::timestamp with time zone)
            ORDER BY md.price_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS eom_price_raw,

        md.price_date
    FROM market_data_daily_adjusted md
),
merged_data AS (
    SELECT DISTINCT ON (tst.instrument, tst.period_start_date)
        tst.instrument,
        tst.period_start_date,
        tst.period_end_date,
        tst.total_buys,
        tst.total_sells,
        tst.total_splits,
        tst.net_cash_flow,
        tst.weighted_cash_flow,
        tst.fees_and_taxes,
        p.bom_price_raw,
        p.eom_price_raw,
        COALESCE(
            lag(p.eom_price_raw) OVER (PARTITION BY tst.instrument ORDER BY tst.period_start_date),
            p.bom_price_raw
        ) AS bom_price,
        p.eom_price_raw AS eom_price
    FROM time_series_transactions tst
    LEFT JOIN prices p
      ON tst.instrument::text = p.instrument::text
     AND tst.period_start_date = p.price_month
    ORDER BY tst.instrument, tst.period_start_date, p.price_date
),
cumulative_shares AS (
    SELECT
        md.instrument,
        md.period_start_date,
        md.period_end_date,
        md.total_buys,
        md.total_sells,
        md.total_splits,
        md.net_cash_flow,
        md.weighted_cash_flow,
        md.fees_and_taxes,
        md.bom_price_raw,
        md.eom_price_raw,
        md.bom_price,
        md.eom_price,
        sum(md.total_buys - md.total_sells + md.total_splits)
          OVER (PARTITION BY md.instrument ORDER BY md.period_start_date) AS eom_shares_cumulative
    FROM merged_data md
),
final_data AS (
    SELECT
        cs.instrument,
        cs.period_start_date,
        cs.period_end_date,
        cs.total_buys,
        cs.total_sells,
        cs.total_splits,
        cs.net_cash_flow,
        cs.weighted_cash_flow,
        cs.fees_and_taxes,
        cs.bom_price_raw,
        cs.eom_price_raw,
        cs.bom_price,
        cs.eom_price,
        cs.eom_shares_cumulative,
        COALESCE(
            lag(cs.eom_shares_cumulative) OVER (PARTITION BY cs.instrument ORDER BY cs.period_start_date),
            0::numeric
        ) AS bom_shares_cumulative
    FROM cumulative_shares cs
)
SELECT
    instrument,
    period_start_date,
    period_end_date,
    total_buys,
    total_sells,
    total_splits,
    bom_shares_cumulative,
    eom_shares_cumulative,
    bom_price,
    eom_price,
    bom_shares_cumulative * bom_price AS nav_bom,
    eom_shares_cumulative * eom_price AS nav_eom,
    net_cash_flow,
    weighted_cash_flow,
    fees_and_taxes,
    eom_shares_cumulative * eom_price - bom_shares_cumulative * bom_price - net_cash_flow + fees_and_taxes AS pnl_gross,
    eom_shares_cumulative * eom_price - bom_shares_cumulative * bom_price - net_cash_flow AS pnl_net,

    CASE
        WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN net_cash_flow::double precision
        WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow)::double precision
        ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision + weighted_cash_flow
    END AS avg_capital_gross,

    CASE
        WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN (net_cash_flow - fees_and_taxes)::double precision
        WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow - fees_and_taxes)::double precision
        ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision
             + (weighted_cash_flow - fees_and_taxes::double precision)
    END AS avg_capital_net,

    CASE
        WHEN (
            CASE
                WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN net_cash_flow::double precision
                WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow)::double precision
                ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision + weighted_cash_flow
            END
        ) <> 0::double precision
        THEN round(
            (eom_shares_cumulative * eom_price - bom_shares_cumulative * bom_price - net_cash_flow) /
            (
                CASE
                    WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN net_cash_flow::double precision
                    WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow)::double precision
                    ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision + weighted_cash_flow
                END
            )::numeric,
            6
        )
        ELSE NULL::numeric
    END AS md_return_gross,

    CASE
        WHEN (
            CASE
                WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN (net_cash_flow - fees_and_taxes)::double precision
                WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow - fees_and_taxes)::double precision
                ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision
                     + (weighted_cash_flow - fees_and_taxes::double precision)
            END
        ) <> 0::double precision
        THEN round(
            (eom_shares_cumulative * eom_price - bom_shares_cumulative * bom_price - (net_cash_flow - fees_and_taxes)) /
            (
                CASE
                    WHEN bom_shares_cumulative = 0::numeric AND eom_shares_cumulative > 0::numeric THEN (net_cash_flow - fees_and_taxes)::double precision
                    WHEN bom_shares_cumulative > 0::numeric AND eom_shares_cumulative = 0::numeric THEN abs(net_cash_flow - fees_and_taxes)::double precision
                    ELSE COALESCE(bom_shares_cumulative * bom_price, 0::numeric)::double precision
                         + (weighted_cash_flow - fees_and_taxes::double precision)
                END
            )::numeric,
            6
        )
        ELSE NULL::numeric
    END AS md_return_net
FROM final_data
ORDER BY instrument, period_start_date;