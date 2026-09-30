-- move to mart from core. 

CREATE OR REPLACE VIEW mart.portfolio_modified_dietz_v AS
SELECT
  n.period_start,
  n.period_end,
  n.nav_bom,
  n.nav_eom,

  COALESCE(c.external_flow, 0)::numeric AS external_flow,
  COALESCE(c.weighted_external_flow, 0)::numeric AS weighted_external_flow,

  -- PnL = change in NAV minus external flows
  (n.nav_eom - n.nav_bom - COALESCE(c.external_flow, 0))::numeric AS period_pnl,

  -- Avg capital per Dietz
  (n.nav_bom + COALESCE(c.weighted_external_flow, 0))::numeric AS avg_capital,

  CASE
    WHEN (n.nav_bom + COALESCE(c.weighted_external_flow, 0)) = 0 THEN NULL
    ELSE
      ((n.nav_eom - n.nav_bom - COALESCE(c.external_flow, 0))
        / (n.nav_bom + COALESCE(c.weighted_external_flow, 0))
      )::numeric
  END AS md_return

FROM core.portfolio_nav_bom_eom_v n
LEFT JOIN core.portfolio_period_cashflows_v c
  ON c.period_start = n.period_start
ORDER BY n.period_start;