"""
Shared PostgreSQL connection utilities for the Investment Analytics Platform.
"""

import os
from collections.abc import Sequence
import pandas as pd


import psycopg
from dotenv import load_dotenv


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection() -> psycopg.Connection:
    """Create a Psycopg connection to the portfolio database."""

    return psycopg.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def query_dataframe(
    query: str,
    params: Sequence | None = None,
) -> pd.DataFrame:
    """
    Execute a read-only SQL query and return the result as a DataFrame.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [column.name for column in cur.description]

    return pd.DataFrame(
        rows,
        columns=columns,
    )