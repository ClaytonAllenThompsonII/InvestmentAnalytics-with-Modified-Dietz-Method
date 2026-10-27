
"""
Refresh materialized views used by investment performance reporting.

Designed for reuse after market-data or transaction ingestion.
All four refreshes execute in a single database transaction.
"""

import logging
import os
from time import perf_counter

import psycopg
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

REPORTING_MVS = (
    "mart.portfolio_daily_twr_mv",
    "mart.portfolio_modified_dietz_mv",
    "mart.security_daily_twr_mv",
    "mart.security_modified_dietz_accrual_mv",
)


def refresh_reporting_mvs() -> None:
    """
    Refresh reporting materialized views in dependency order.

    PostgreSQL commits the refreshes together when all succeed.
    An exception rolls back the transaction and propagates to
    the calling ingestion pipeline.
    """

    start = perf_counter()

    with psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    ) as conn:

        with conn.cursor() as cur:
            for mv in REPORTING_MVS:
                step_start = perf_counter()

                logger.info("Refreshing %s", mv)

                # Safe: identifiers come from the fixed list above.
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv};")

                logger.info(
                    "Refreshed %s in %.2f seconds",
                    mv,
                    perf_counter() - step_start,
                )

    logger.info(
        "All reporting materialized views committed in %.2f seconds",
        perf_counter() - start,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    refresh_reporting_mvs()
