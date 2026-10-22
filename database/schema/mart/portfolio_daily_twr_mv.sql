-- Materialized reporting layer for daily portfolio TWR.
-- Source of truth: mart.portfolio_daily_twr_v

CREATE MATERIALIZED VIEW mart.portfolio_daily_twr_mv AS
SELECT *
FROM mart.portfolio_daily_twr_v;



-- REFRESH MATERIALIZED VIEW mart.portfolio_daily_twr_mv;