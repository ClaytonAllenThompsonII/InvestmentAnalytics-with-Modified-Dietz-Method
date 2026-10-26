
CREATE MATERIALIZED VIEW mart.security_modified_dietz_accrual_mv AS
SELECT *
FROM mart.security_modified_dietz_accrual_v;

CREATE UNIQUE INDEX security_modified_dietz_accrual_mv_uidx
ON mart.security_modified_dietz_accrual_mv (
    instrument,
    period_start
);

ANALYZE mart.security_modified_dietz_accrual_mv;


-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.security_modified_dietz_accrual_mv;