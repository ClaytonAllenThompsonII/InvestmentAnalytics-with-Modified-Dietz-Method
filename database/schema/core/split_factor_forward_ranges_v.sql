CREATE OR REPLACE VIEW core.split_factor_forward_ranges_v AS
WITH events AS (
  SELECT
    instrument,
    split_date::date,
    split_coeff::numeric
  FROM core.split_events_v
),
suffix AS (
  SELECT
    instrument,
    split_date,
    split_coeff,
    EXP(
      SUM(LN(split_coeff::double precision))
      OVER (
        PARTITION BY instrument
        ORDER BY split_date DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    )::numeric AS factor_before_this_split
  FROM events
),
ranges AS (
  SELECT
    instrument,
    COALESCE(
      LAG(split_date) OVER (PARTITION BY instrument ORDER BY split_date),
      DATE '1900-01-01'
    ) AS start_date,
    (split_date - INTERVAL '1 day')::date AS end_date,
    factor_before_this_split AS split_factor_forward
  FROM suffix
),
final_row AS (
  SELECT
    instrument,
    MAX(split_date) AS start_date,
    DATE '9999-12-31' AS end_date,
    1::numeric AS split_factor_forward
  FROM events
  GROUP BY instrument
)
SELECT * FROM ranges
UNION ALL
SELECT * FROM final_row;