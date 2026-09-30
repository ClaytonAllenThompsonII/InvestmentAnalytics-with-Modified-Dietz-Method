CREATE OR REPLACE VIEW mart.security_modified_dietz_v AS
SELECT
  n.instrument,
  n.period_start,
  n.period_end,
  n.nmv_bom,
  n.nmv_eom,

  COALESCE(c.trade_cash, 0)::numeric AS trade_cash,
  COALESCE(c.dividend_cash, 0)::numeric AS dividend_cash,              -- net dividend received
  COALESCE(c.tax_fee_cash, 0)::numeric AS tax_fee_cash,                -- typically negative

  COALESCE(c.weighted_trade_cash, 0)::numeric AS weighted_trade_cash,
  COALESCE(c.weighted_dividend_cash, 0)::numeric AS weighted_dividend_cash,
  COALESCE(c.weighted_tax_fee_cash, 0)::numeric AS weighted_tax_fee_cash,

  -- derived gross dividend (add back taxes/fees)
  (COALESCE(c.dividend_cash, 0) - COALESCE(c.tax_fee_cash, 0))::numeric AS gross_dividend_cash,
  (COALESCE(c.weighted_dividend_cash, 0) - COALESCE(c.weighted_tax_fee_cash, 0))::numeric AS weighted_gross_dividend_cash,

  /* -------------------------
     NET (includes taxes/fees drag)
     ------------------------- */
  (n.nmv_eom - n.nmv_bom
   - COALESCE(c.trade_cash, 0)
   - COALESCE(c.tax_fee_cash, 0)
   + COALESCE(c.dividend_cash, 0)
  )::numeric AS net_period_pnl,

  (n.nmv_bom
   + COALESCE(c.weighted_trade_cash, 0)
   + COALESCE(c.weighted_tax_fee_cash, 0)
   + COALESCE(c.weighted_dividend_cash, 0)
  )::numeric AS net_avg_capital,

  CASE
    WHEN (n.nmv_bom
          + COALESCE(c.weighted_trade_cash, 0)
          + COALESCE(c.weighted_tax_fee_cash, 0)
          + COALESCE(c.weighted_dividend_cash, 0)) = 0
      THEN NULL
    ELSE
      ((n.nmv_eom - n.nmv_bom
        - COALESCE(c.trade_cash, 0)
        - COALESCE(c.tax_fee_cash, 0)
        + COALESCE(c.dividend_cash, 0))
       /
       (n.nmv_bom
        + COALESCE(c.weighted_trade_cash, 0)
        + COALESCE(c.weighted_tax_fee_cash, 0)
        + COALESCE(c.weighted_dividend_cash, 0))
      )::numeric
  END AS net_md_return,

  /* -------------------------
     GROSS (add back taxes/fees)
     ------------------------- */
  (n.nmv_eom - n.nmv_bom
   - COALESCE(c.trade_cash, 0)
   + (COALESCE(c.dividend_cash, 0) - COALESCE(c.tax_fee_cash, 0))
  )::numeric AS gross_period_pnl,

  (n.nmv_bom
   + COALESCE(c.weighted_trade_cash, 0)
   + (COALESCE(c.weighted_dividend_cash, 0) - COALESCE(c.weighted_tax_fee_cash, 0))
  )::numeric AS gross_avg_capital,

  CASE
    WHEN (n.nmv_bom
          + COALESCE(c.weighted_trade_cash, 0)
          + (COALESCE(c.weighted_dividend_cash, 0) - COALESCE(c.weighted_tax_fee_cash, 0))) = 0
      THEN NULL
    ELSE
      ((n.nmv_eom - n.nmv_bom
        - COALESCE(c.trade_cash, 0)
        + (COALESCE(c.dividend_cash, 0) - COALESCE(c.tax_fee_cash, 0)))
       /
       (n.nmv_bom
        + COALESCE(c.weighted_trade_cash, 0)
        + (COALESCE(c.weighted_dividend_cash, 0) - COALESCE(c.weighted_tax_fee_cash, 0)))
      )::numeric
  END AS gross_md_return

FROM core.security_nmv_bom_eom_v n
LEFT JOIN core.security_period_cashflows_v c
  ON c.instrument = n.instrument
 AND c.period_start = n.period_start;