CREATE OR REPLACE VIEW public.portfolio_performance_view AS
SELECT
    period_start_date,
    period_end_date,

    -- Aggregated NAVs
    SUM(nav_bom) AS nav_bom,
    SUM(nav_eom) AS nav_eom,

    -- Aggregated cash flows (net cash flow already includes dividends, fees, taxes, buys/sells, etc.)
    SUM(net_cash_flow)      AS net_cash_flow,
    SUM(weighted_cash_flow) AS weighted_cash_flow,
    SUM(fees_and_taxes)     AS fees_and_taxes,

    -- Portfolio PnL
    SUM(pnl_gross) AS pnl_gross,
    SUM(pnl_net)   AS pnl_net,

    -- Average Capital (single denominator for both gross & net returns)
    CASE
        -- Portfolio initiated during the month (no BOM NAV, positive EOM NAV)
        WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0
            THEN SUM(net_cash_flow)::double precision

        -- Portfolio fully liquidated during the month (positive BOM NAV, zero EOM NAV)
        WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0
            THEN ABS(SUM(net_cash_flow))::double precision

        -- Normal case: capital at risk = BOM NAV + time-weighted external cash flows
        ELSE COALESCE(SUM(nav_bom), 0)::double precision
           + COALESCE(SUM(weighted_cash_flow), 0)::double precision
    END AS avg_capital,

    -- Modified Dietz Return (Gross)
    CASE
        WHEN (
            CASE
                WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)::double precision
                WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))::double precision
                ELSE COALESCE(SUM(nav_bom), 0)::double precision + COALESCE(SUM(weighted_cash_flow), 0)::double precision
            END
        ) <> 0::double precision
        THEN ROUND((SUM(pnl_gross) /
            (
                CASE
                    WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)::double precision
                    WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))::double precision
                    ELSE COALESCE(SUM(nav_bom), 0)::double precision + COALESCE(SUM(weighted_cash_flow), 0)::double precision
                END
            ))::numeric, 6)
        ELSE NULL::numeric
    END AS md_return_gross,

    -- Modified Dietz Return (Net)
    CASE
        WHEN (
            CASE
                WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)::double precision
                WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))::double precision
                ELSE COALESCE(SUM(nav_bom), 0)::double precision + COALESCE(SUM(weighted_cash_flow), 0)::double precision
            END
        ) <> 0::double precision
        THEN ROUND((SUM(pnl_net) /
            (
                CASE
                    WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)::double precision
                    WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))::double precision
                    ELSE COALESCE(SUM(nav_bom), 0)::double precision + COALESCE(SUM(weighted_cash_flow), 0)::double precision
                END
            ))::numeric, 6)
        ELSE NULL::numeric
    END AS md_return_net

FROM public.asset_performance_view
GROUP BY period_start_date, period_end_date
ORDER BY period_end_date;