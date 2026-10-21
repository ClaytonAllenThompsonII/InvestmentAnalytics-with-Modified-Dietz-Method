-- Materialized reporting layer for monthly portfolio performance.
-- Source of truth: mart.portfolio_modified_dietz_v

CREATE MATERIALIZED VIEW mart.portfolio_modified_dietz_mv AS
SELECT *
FROM mart.portfolio_modified_dietz_v;


-- run
-- REFRESH MATERIALIZED VIEW mart.portfolio_modified_dietz_mv;
-- You should also refresh it when a reporting month transitions from MTD to complete, even if no new transactions were recorded.