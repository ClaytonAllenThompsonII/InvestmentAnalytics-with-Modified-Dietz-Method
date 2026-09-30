-- ============================================================================
-- market_data_daily_adjusted
-- Source: Alpha Vantage - Daily Time Series with Splits and Dividend Events
-- Function: TIME_SERIES_DAILY_ADJUSTED
--
-- Contains:
--   - Raw (as-traded) OHLCV
--   - Adjusted close (split/dividend adjusted series)
--   - Dividend amount and split coefficient events
--   - "Last Refreshed" date from API meta data for freshness checks in UI
-- ============================================================================

DROP TABLE IF EXISTS market_data_daily_adjusted;

CREATE TABLE market_data_daily_adjusted (
    market_data_id       BIGSERIAL PRIMARY KEY,

    -- Mapped / canonical ticker (e.g., META not FB)
    instrument           VARCHAR(20) NOT NULL,

    -- Trading date
    price_date           DATE NOT NULL,

    -- 1-4 Raw OHLC (as traded)
    open_price           NUMERIC(18,6),
    high_price           NUMERIC(18,6),
    low_price            NUMERIC(18,6),
    close_price          NUMERIC(18,6),

    -- 5 Adjusted close (split + dividend adjusted)
    adjusted_close       NUMERIC(18,6),

    -- 6 Volume
    volume               BIGINT,

    -- 7 Dividend amount (cash dividend on that date)
    dividend_amount      NUMERIC(18,6),

    -- 8 Split coefficient (e.g., 2.0 for 2-for-1; 1.0 for none)
    split_coefficient    NUMERIC(18,6),

    -- From "Meta Data" -> "3. Last Refreshed"
    last_refreshed_date  DATE,

    -- Optional: keep a record of source + load timestamps (cheap + helpful)
    data_source          VARCHAR(50) NOT NULL DEFAULT 'alpha_vantage',
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_mkt_adj_instrument_date UNIQUE (instrument, price_date),

    CONSTRAINT chk_split_positive CHECK (split_coefficient IS NULL OR split_coefficient > 0),
    CONSTRAINT chk_prices_nonnegative CHECK (
        (open_price IS NULL OR open_price >= 0) AND
        (high_price IS NULL OR high_price >= 0) AND
        (low_price  IS NULL OR low_price  >= 0) AND
        (close_price IS NULL OR close_price >= 0) AND
        (adjusted_close IS NULL OR adjusted_close >= 0) AND
        (dividend_amount IS NULL OR dividend_amount >= 0)
    )
);

CREATE INDEX idx_mkt_adj_instrument_date
    ON market_data_daily_adjusted (instrument, price_date);

CREATE INDEX idx_mkt_adj_price_date
    ON market_data_daily_adjusted (price_date);