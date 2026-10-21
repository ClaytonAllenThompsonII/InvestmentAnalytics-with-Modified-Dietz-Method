-- View: mart.portfolio_modified_dietz_v
--
-- Monthly Modified Dietz performance with reporting metadata.
-- Preserves the existing return formula and column structure.
--
-- period_status: complete / mtd
-- valuation_status: complete / partial

CREATE OR REPLACE VIEW mart.portfolio_modified_dietz_v AS

WITH boundaries AS (
    SELECT
        period_start,
        nav_date,
        LAG(nav_date) OVER (
            ORDER BY period_start
        ) AS prior_nav_date
    FROM core.portfolio_nav_eom_v
)

SELECT
    n.period_start,
    n.period_end,
    n.nav_bom,
    n.nav_eom,

    COALESCE(c.external_flow, 0)::numeric AS external_flow,
    COALESCE(c.weighted_external_flow, 0)::numeric AS weighted_external_flow,

    -- Investment P&L excluding external flows
    (n.nav_eom - n.nav_bom - COALESCE(c.external_flow, 0))::numeric
        AS period_pnl,

    -- Modified Dietz average capital
    (n.nav_bom + COALESCE(c.weighted_external_flow, 0))::numeric
        AS avg_capital,

    CASE
        WHEN (n.nav_bom + COALESCE(c.weighted_external_flow, 0)) = 0
            THEN NULL
        ELSE (
            (n.nav_eom - n.nav_bom - COALESCE(c.external_flow, 0))
            / (n.nav_bom + COALESCE(c.weighted_external_flow, 0))
        )::numeric
    END AS md_return,

    -- Reporting metadata
    b.nav_date,

    CASE
        WHEN n.period_end >= CURRENT_DATE THEN 'mtd'
        ELSE 'complete'
    END AS period_status,

    CASE
        WHEN EXISTS (
            SELECT 1
            FROM core.portfolio_nav_daily_v d
            WHERE d.date BETWEEN n.period_start AND b.nav_date
              AND d.valuation_status IS DISTINCT FROM 'complete'
        )
        OR EXISTS (
            SELECT 1
            FROM core.portfolio_nav_daily_v d
            WHERE d.date = b.prior_nav_date
              AND d.valuation_status IS DISTINCT FROM 'complete'
        )
        THEN 'partial'
        ELSE 'complete'
    END AS valuation_status

FROM core.portfolio_nav_bom_eom_v n

JOIN boundaries b
    ON b.period_start = n.period_start

LEFT JOIN core.portfolio_period_cashflows_v c
    ON c.period_start = n.period_start

ORDER BY n.period_start;