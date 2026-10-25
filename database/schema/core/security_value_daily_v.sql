
-- View: core.security_value_daily_v
--
-- Purpose:
-- Calculate daily economic value of each equity security,
-- including outstanding dividend receivables.
--
-- Security Value = Equity NMV + Dividend Receivable

CREATE OR REPLACE VIEW core.security_value_daily_v AS

SELECT
    COALESCE(n.date, d.date) AS date,
    COALESCE(n.instrument, d.instrument) AS instrument,

    -- Equity valuation detail
    n.qty_raw,
    n.close_price,
    COALESCE(n.nmv, 0)::numeric AS equity_nmv,

    -- Accrued dividend income
    COALESCE(d.dividend_receivable, 0)::numeric
        AS dividend_receivable,

    -- Total security economic value
    CASE
        WHEN n.date IS NOT NULL AND n.nmv IS NULL
            THEN NULL::numeric
        ELSE (
            COALESCE(n.nmv, 0)
            + COALESCE(d.dividend_receivable, 0)
        )::numeric
    END AS security_value,

    CASE
        WHEN n.date IS NULL THEN 'receivable_only'
        ELSE n.valuation_status
    END AS valuation_status

FROM core.nmv_daily_v n

FULL OUTER JOIN core.dividend_receivable_daily_v d
    ON d.date = n.date
   AND d.instrument = n.instrument;
