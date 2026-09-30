CREATE OR REPLACE VIEW core.position_qty_eod_v AS
WITH RECURSIVE
events AS (
  -- trade deltas by day
  SELECT
    date,
    instrument,
    qty_delta::numeric AS trade_delta,
    NULL::numeric AS split_ratio
  FROM core.equity_trade_deltas_v

  UNION ALL

  -- split events by day (ratio = 1 + quantity)
  SELECT
    t.econ_date AS date,
    t.normalized_instrument AS instrument,
    0::numeric AS trade_delta,
    (1 + COALESCE(t.quantity, 0))::numeric AS split_ratio
  FROM stage.txn_classified_v t
  WHERE t.raw_trans_code = 'SPL'
    AND t.normalized_instrument IS NOT NULL
    AND t.normalized_instrument <> 'CASH'
),
daily AS (
  -- one row per instrument/day
  SELECT
    date,
    instrument,
    SUM(trade_delta)::numeric AS trade_delta,
    MAX(split_ratio)::numeric AS split_ratio
  FROM events
  GROUP BY 1, 2
),
ordered AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY date) AS rn
  FROM daily d
),
recur AS (
  -- seed
  SELECT
    instrument,
    date,
    rn,
    trade_delta,
    split_ratio,
    (0::numeric * COALESCE(split_ratio, 1::numeric) + trade_delta)::numeric AS eod_qty
  FROM ordered
  WHERE rn = 1

  UNION ALL

  -- step
  SELECT
    o.instrument,
    o.date,
    o.rn,
    o.trade_delta,
    o.split_ratio,
    (r.eod_qty * COALESCE(o.split_ratio, 1::numeric) + o.trade_delta)::numeric AS eod_qty
  FROM recur r
  JOIN ordered o
    ON o.instrument = r.instrument
   AND o.rn = r.rn + 1
)
SELECT
  date,
  instrument,
  trade_delta AS qty_delta,
  split_ratio,
  eod_qty
FROM recur
ORDER BY instrument, date;