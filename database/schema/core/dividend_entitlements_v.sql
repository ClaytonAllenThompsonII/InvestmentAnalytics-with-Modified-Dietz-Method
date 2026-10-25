
CREATE OR REPLACE VIEW core.dividend_entitlements_v AS

WITH dividends AS (
    SELECT
        e.transaction_id,
        e.normalized_instrument AS instrument,

        e.record_date::date AS record_date,
        e.payment_date::date AS payment_date,

        -- Robinhood is authoritative for the USD amount
        e.amount::numeric AS gross_dividend_usd,

        -- Extract dividend entitlement details
        (
            regexp_match(
                e.description,
                '([0-9]+[.]?[0-9]*) shares? at ([0-9]+[.]?[0-9]*)'
            )
        )[1]::numeric AS reported_shares,

        (
            regexp_match(
                e.description,
                '([0-9]+[.]?[0-9]*) shares? at ([0-9]+[.]?[0-9]*)'
            )
        )[2]::numeric AS robinhood_rate

    FROM stage.enriched_transactions_view e
    WHERE e.raw_trans_code = 'CDIV'
)

SELECT
    d.*,

    -- Only accept an unambiguous ex-dividend date
    CASE
        WHEN m.match_count = 1 THEN m.ex_date
        ELSE NULL
    END AS ex_dividend_date,

    -- Reference only; not used for accounting amounts
    CASE
        WHEN m.match_count = 1 THEN m.market_rate
        ELSE NULL
    END AS market_dividend_rate,

    m.match_count AS ex_date_match_count

FROM dividends d

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::integer AS match_count,
        MIN(p.price_date)::date AS ex_date,
        MIN(p.dividend_amount)::numeric AS market_rate

    FROM source.market_data_daily_adjusted p

    WHERE p.instrument = d.instrument
      AND p.price_date BETWEEN d.record_date - 7
                           AND d.record_date + 2
      AND p.dividend_amount > 0

) m ON TRUE;
