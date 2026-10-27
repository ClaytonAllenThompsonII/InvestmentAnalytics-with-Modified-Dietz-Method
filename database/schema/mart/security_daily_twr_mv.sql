
CREATE MATERIALIZED VIEW mart.security_daily_twr_mv AS
SELECT *
FROM mart.security_daily_twr_v;

CREATE UNIQUE INDEX security_daily_twr_mv_uidx
ON mart.security_daily_twr_mv (
    instrument,
    date
);

ANALYZE mart.security_daily_twr_mv;


-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.security_daily_twr_mv;