# Investment Analytics Architecture Review

## Current Architecture

The database currently contains two largely independent analytical paths
sharing common transaction and market-data sources.

### V2 – Valuation / Performance
`source → stage → core → mart`

Purpose:
- reconstruct equity positions through time
- daily security and portfolio valuation
- cash balances and external cash flows
- Modified Dietz portfolio/security performance
- downstream risk analytics

No current `core` or `mart` view depends on objects in the `public` schema.

### V1 – Lot / P&L and Legacy Performance

The `public` schema contains the older FIFO / cost-basis architecture,
including equity and option lots, realized gains, and older performance views.

The current Python transaction ingestion script still invokes three V1
procedures after loading transactions.

The lot-accounting capability may remain valuable for:
- realized P&L
- unrealized P&L
- cost basis
- option trade analysis

However, the existing V1 implementation should be reviewed separately
before being incorporated into the current architecture.

## Design Direction Under Consideration

Maintain separate but complementary engines for:

1. Valuation / performance
2. Lot accounting / P&L
3. Strategy attribution / sleeves

Potential strategy sleeves:
- Core Equity
- Options – Hedge
- Options – Income
- Options – Directional