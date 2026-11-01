"""Shared setup for the examples: the connection string and a small demo table."""

import os
import sys

import arrowsql

TABLE = "dbo.arrowsql_example_trips"


def connection_string() -> str:
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        sys.exit(
            "Set DB_CONNECTION_STRING, e.g.\n"
            "  export DB_CONNECTION_STRING='Server=localhost,1433;Database=tempdb;"
            "Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes'"
        )
    return conn_str


def create_demo_table(conn_str: str, rows: int = 200_000, primary_key: bool = True) -> None:
    """(Re)create TABLE with `rows` synthetic taxi-like trips, generated server-side.

    Without `primary_key` the table is a heap, which parallel bulk loads can fill
    over several connections at once."""
    with arrowsql.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {TABLE} (
                trip_id      int            NOT NULL{' PRIMARY KEY' if primary_key else ''},
                pickup_at    datetime2(0)   NOT NULL,
                zone         nvarchar(20)   NOT NULL,
                passengers   tinyint        NULL,
                distance_km  decimal(9, 2)  NOT NULL,
                fare         money          NOT NULL
            )"""
        )
        cur.execute(
            f"""
            WITH n AS (
                SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b
            )
            INSERT INTO {TABLE}
            SELECT i,
                   DATEADD(MINUTE, i % 44640, '2024-01-01'),
                   CHOOSE(i % 4 + 1, N'Manhattan', N'Brooklyn', N'Queens', N'Bronx'),
                   NULLIF(i % 5, 0),
                   (i % 3000) / 100.0,
                   3 + (i % 3000) / 40.0
            FROM n"""
        )


def drop_demo_table(conn_str: str) -> None:
    with arrowsql.connect(conn_str, autocommit=True) as conn:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {TABLE}")
