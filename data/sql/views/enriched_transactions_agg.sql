CREATE OR REPLACE VIEW enriched_transactions_agg AS

WITH filtered_data AS (
    SELECT *
    FROM enriched_transactions_view
    WHERE raw_trans_code IN ('Buy', 'Sell', 'CDIV', 'SPL')
)
SELECT
    /* 
     * Grouping dimensions:
     * One row per (instrument, period_start_date, period_end_date)
     */
    instrument,
    period_start_date,
    period_end_date,

    -- Total buy quantity (only 'Buy' exist in filtered_data anyway)
    COALESCE(SUM(
        CASE WHEN raw_trans_code = 'Buy' THEN quantity ELSE 0 END
    ), 0) AS total_buys,

    -- Total sell quantity
    COALESCE(SUM(
        CASE WHEN raw_trans_code = 'Sell' THEN quantity ELSE 0 END
    ), 0) AS total_sells,

    COALESCE(SUM(CASE WHEN raw_trans_code = 'SPL' THEN quantity ELSE 0 END), 0) AS total_splits,


    -- Net quantity = buys - sells (CDIV has no quantity effect)
    COALESCE(SUM(
        CASE
            WHEN raw_trans_code = 'Buy'  THEN quantity
            WHEN raw_trans_code = 'Sell' THEN -quantity
		    WHEN raw_trans_code = 'SPL'  THEN quantity

            ELSE 0
        END
    ), 0) AS net_quantity,

    -- Net cash flow: sum of Buy, Sell, CDIV (only those exist in filtered_data)
    COALESCE(SUM(cash_flow), 0) AS net_cash_flow,

    -- Weighted cash flow
    COALESCE(SUM(cash_flow * weight), 0) AS weighted_cash_flow,

    /*
     * JSON list-of-lists of filtered transactions in this monthly bucket.
     * Each transaction is [raw_trans_code, corrected_activity_date, cash_flow].
     */
    COALESCE(
      JSON_AGG(
        JSON_BUILD_ARRAY(
          raw_trans_code,
          TO_CHAR(corrected_activity_date, 'YYYY-MM-DD'),
          cash_flow,
          weight
        )
      ) FILTER (WHERE transaction_id IS NOT NULL),
      '[]'::JSON
    ) AS monthly_transactions

FROM filtered_data
GROUP BY
    instrument,
    period_start_date,
    period_end_date
ORDER BY
    instrument,
    period_start_date;