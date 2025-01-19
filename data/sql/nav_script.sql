WITH recent_market_data AS (
    SELECT
        instrument,
        MAX(price_date) AS most_recent_date
    FROM
        market_data
    GROUP BY
        instrument
),
instrument_prices AS (
    SELECT
        md.instrument,
        md.price_date AS price_date,
        md.close_price AS closing_price
    FROM
        market_data md
    INNER JOIN
        recent_market_data rmd
        ON md.instrument = rmd.instrument AND md.price_date = rmd.most_recent_date
),
cumulative_shares_data AS (
    SELECT
        instrument,
        description,
        SUM(CASE 
                WHEN trans_code = 'Buy' THEN quantity
                WHEN trans_code = 'Sell' THEN -quantity
                ELSE 0
            END) AS cumulative_shares
    FROM 
        transactions
    WHERE 
        trans_code IN ('Buy', 'Sell')
    GROUP BY 
        instrument, description
)
SELECT
    csd.instrument,
    csd.description,
    csd.cumulative_shares,
    ip.closing_price,
    csd.cumulative_shares * ip.closing_price AS nav
FROM
    cumulative_shares_data csd
LEFT JOIN
    instrument_prices ip
    ON csd.instrument = ip.instrument
ORDER BY
    nav DESC;