-- View: core.option_nmv_daily_v
--
-- Purpose:
-- Calculate historical end-of-day net market value (NMV)
-- for each option contract with a nonzero EOD position.
--
-- Dependencies:
-- core.option_position_qty_calendar_v
-- core.option_contracts
-- core.option_contract_symbols
-- source.option_market_data_daily
--
-- Valuation:
-- NMV = eod_qty * premium_multiplier * mark_price
--
-- Notes:
-- Long positions produce positive NMV.
-- Short positions produce negative NMV.
-- Missing or ambiguous market data produces NULL NMV.

CREATE OR REPLACE VIEW core.option_nmv_daily_v AS

WITH held_positions AS (

    SELECT
        p.date,
        p.option_contract_id,
        p.eod_qty,

        c.underlying_symbol,
        c.expiration_date,
        c.option_type,
        c.strike_price,
        c.premium_multiplier

    FROM core.option_position_qty_calendar_v p

    JOIN core.option_contracts c
        ON c.option_contract_id = p.option_contract_id

    WHERE p.eod_qty <> 0

),

resolved_symbols AS (

    SELECT
        p.*,

        sm.mapping_count,

        CASE
            WHEN sm.mapping_count = 1
                THEN sm.contract_symbol
            ELSE NULL
        END AS contract_symbol

    FROM held_positions p

    LEFT JOIN LATERAL (

        SELECT
            COUNT(*) AS mapping_count,
            MIN(s.contract_symbol) AS contract_symbol

        FROM core.option_contract_symbols s

        WHERE s.option_contract_id = p.option_contract_id
          AND s.source_system = 'alpha_vantage'

          AND (
              s.valid_from IS NULL
              OR p.date >= s.valid_from
          )

          AND (
              s.valid_to IS NULL
              OR p.date <= s.valid_to
          )

    ) sm ON TRUE

)

SELECT
    r.date,
    r.option_contract_id,
    r.underlying_symbol,
    r.expiration_date,
    r.option_type,
    r.strike_price,

    r.eod_qty,
    r.premium_multiplier,

    r.contract_symbol,

    md.bid_price,
    md.ask_price,
    md.last_price,
    md.mark_price,

    CASE
        WHEN r.mapping_count = 0
            THEN 'missing_symbol'

        WHEN r.mapping_count > 1
            THEN 'ambiguous_symbol'

        WHEN md.option_market_data_id IS NULL
            THEN 'missing_market_data'

        WHEN md.mark_price IS NULL
            THEN 'missing_mark'

        ELSE 'priced'
    END AS valuation_status,

    CASE
        WHEN r.mapping_count = 1
         AND md.mark_price IS NOT NULL

        THEN
            r.eod_qty
            * r.premium_multiplier
            * md.mark_price

        ELSE NULL
    END AS nmv

FROM resolved_symbols r

LEFT JOIN source.option_market_data_daily md
    ON md.contract_symbol = r.contract_symbol
   AND md.price_date = r.date
   AND md.data_source = 'alpha_vantage';