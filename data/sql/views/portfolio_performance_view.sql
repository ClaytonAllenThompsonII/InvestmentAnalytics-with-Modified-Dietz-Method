CREATE OR REPLACE VIEW portfolio_performance_view AS

-- Portfolio-level aggregate using asset_performance_view
SELECT
    period_start_date,
    period_end_date,

    -- Aggregated NAVs
    SUM(nav_bom) AS nav_bom,
    SUM(nav_eom) AS nav_eom,

    -- Aggregated cash flows
    SUM(net_cash_flow) AS net_cash_flow,
    SUM(weighted_cash_flow) AS weighted_cash_flow,
    SUM(fees_and_taxes) AS fees_and_taxes,

    -- Portfolio PnL (Gross & Net)
    SUM(pnl_gross) AS pnl_gross,
    SUM(pnl_net) AS pnl_net,

    -- Average Capital (Gross)
    CASE
        WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)
        WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))
        ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow)
    END AS avg_capital_gross,

    -- Average Capital (Net)
    CASE
        WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow - fees_and_taxes)
        WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow - fees_and_taxes))
        ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow - fees_and_taxes)
    END AS avg_capital_net,

    -- Modified Dietz Return (Gross)
    CASE
        WHEN (
            CASE
                WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)
                WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))
                ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow)
            END
        ) != 0 THEN
            ROUND(
                (
                    SUM(nav_eom) - SUM(nav_bom) - SUM(net_cash_flow)
                ) /
                (
                    CASE
                        WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow)
                        WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow))
                        ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow)
                    END
                )::numeric, 6
            )
        ELSE NULL
    END AS md_return_gross,

    -- Modified Dietz Return (Net)
    CASE
        WHEN (
            CASE
                WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow - fees_and_taxes)
                WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow - fees_and_taxes))
                ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow - fees_and_taxes)
            END
        ) != 0 THEN
            ROUND(
                (
                    SUM(nav_eom) - SUM(nav_bom) - SUM(net_cash_flow - fees_and_taxes)
                ) /
                (
                    CASE
                        WHEN SUM(nav_bom) = 0 AND SUM(nav_eom) > 0 THEN SUM(net_cash_flow - fees_and_taxes)
                        WHEN SUM(nav_bom) > 0 AND SUM(nav_eom) = 0 THEN ABS(SUM(net_cash_flow - fees_and_taxes))
                        ELSE COALESCE(SUM(nav_bom), 0) + SUM(weighted_cash_flow - fees_and_taxes)
                    END
                )::numeric, 6
            )
        ELSE NULL
    END AS md_return_net

FROM asset_performance_view
GROUP BY period_start_date, period_end_date
ORDER BY period_end_date;