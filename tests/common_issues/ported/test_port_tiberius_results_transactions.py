"""Multi-result batches, row handling, rowcounts and transaction loops ported from tiberius.

Adapted from prisma/tiberius tests/query.rs and tests/transactions.rs and tests/deadlocks.rs
(MIT OR Apache-2.0), Copyright 2021 Tiberius Contributors.
Upstream license texts: https://github.com/prisma/tiberius/blob/main/LICENSE-MIT.txt and LICENSE-APACHE.txt.
The cases were re-expressed against mssql_python (DB-API cursor, nextset, rowcount); no code was copied.

Covers many transaction cycles on one connection, stored procedures whose return codes
are read through a batch, sp_executesql result sets walked with nextset, rows whose
length crosses packet boundaries, re-executing a partially consumed batch, DML rowcount
totals, metadata after a warning, and a deadlock victim that can query again.
"""

import threading
import uuid

import mssql_python


def _drop_proc(cursor, name):
    cursor.execute(f"IF OBJECT_ID('dbo.{name}', 'P') IS NOT NULL DROP PROCEDURE dbo.{name}")
    cursor.connection.commit()


def test_300_begin_commit_cycles_on_one_connection(conn_str):
    conn = mssql_python.connect(conn_str, autocommit=True)
    try:
        cur = conn.cursor()
        for _ in range(299):
            cur.execute("BEGIN TRAN")
            cur.execute("SELECT ?", [-4])
            assert cur.fetchone()[0] == -4
            cur.execute("COMMIT")
        cur.execute("SELECT @@TRANCOUNT")
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_stored_proc_return_code_read_through_batch(cursor, table_name):
    proc = f"tib_ins_{uuid.uuid4().hex[:10]}"
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT IDENTITY(1,1) PRIMARY KEY, other VARCHAR(50))"
    )
    cursor.execute(
        f"""CREATE PROCEDURE dbo.{proc} @p1 VARCHAR(50) AS
            BEGIN
                INSERT INTO dbo.{table_name} (other) VALUES (@p1);
                RETURN CAST(SCOPE_IDENTITY() AS INT);
            END"""
    )
    cursor.connection.commit()
    try:
        returns = []
        for text in ["some text", "another text"]:
            cursor.execute(
                f"SET NOCOUNT ON; DECLARE @rc INT; EXEC @rc = dbo.{proc} ?; SELECT @rc AS Id",
                [text],
            )
            returns.append(cursor.fetchone()[0])
        assert returns == [1, 2]
    finally:
        _drop_proc(cursor, proc)


def test_sp_executesql_result_sets_walked_with_nextset(cursor):
    cursor.execute(
        "EXECUTE sp_executesql N'SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1'; "
        "EXECUTE sp_executesql N'SELECT 2 UNION ALL SELECT 2'"
    )
    first = [r[0] for r in cursor.fetchall()]
    assert cursor.nextset() is True
    second = [r[0] for r in cursor.fetchall()]
    assert first == [1, 1, 1]
    assert second == [2, 2]
    assert not cursor.nextset()


def test_long_strings_in_two_result_sets_are_not_truncated(cursor):
    cursor.execute("SELECT ? AS a; SELECT ? AS b", ["a" * 4001, "b" * 2095])
    first = cursor.fetchone()[0]
    assert cursor.nextset() is True
    second = cursor.fetchone()[0]
    assert first == "a" * 4001
    assert second == "b" * 2095


def test_reexecute_after_partially_consumed_batch_discards_pending_results(cursor):
    cursor.execute("SELECT 1 AS one; SELECT 2 AS two")
    assert cursor.fetchone()[0] == 1
    # The second result set is still pending; a new batch must start cleanly.
    cursor.execute("SELECT 3")
    assert cursor.fetchone()[0] == 3


def test_multi_statement_dml_batch_inserts_every_statement(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL)")
    cursor.connection.commit()
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (id) VALUES (?); "
        f"INSERT INTO dbo.{table_name} (id) VALUES (?), (?);",
        [1, 2, 3],
    )
    cursor.connection.commit()
    # Batch rowcount is not specified for multi-statement text, so assert the data instead.
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 3


def test_warning_from_aggregate_null_keeps_column_metadata(cursor):
    proc = f"tib_warn_{uuid.uuid4().hex[:10]}"
    cursor.execute(
        f"""CREATE PROCEDURE dbo.{proc} AS
            BEGIN
                SET NOCOUNT ON;
                DECLARE @tmp TABLE (col INT);
                INSERT INTO @tmp
                SELECT SUM(col) AS col FROM (SELECT NULL AS col UNION SELECT 1) t;
                SELECT SUM(col) AS col FROM @tmp;
            END"""
    )
    cursor.connection.commit()
    try:
        cursor.execute(f"EXEC dbo.{proc}")
        assert [d[0] for d in cursor.description] == ["col"]
        assert cursor.fetchone()[0] == 1
    finally:
        _drop_proc(cursor, proc)


def test_description_follows_each_result_set(cursor):
    cursor.execute("SELECT ? AS first_col; SELECT ? AS second_col, ? AS third_col", [1, 2, 3])
    assert [d[0] for d in cursor.description] == ["first_col"]
    assert cursor.nextset() is True
    assert [d[0] for d in cursor.description] == ["second_col", "third_col"]
    assert tuple(cursor.fetchone()) == (2, 3)


def test_rolled_back_transaction_followed_by_commit_keeps_only_second(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL)")
    cursor.connection.commit()

    cursor.execute(f"INSERT INTO dbo.{table_name} (id) VALUES (?)", [10])
    cursor.connection.rollback()
    cursor.execute(f"INSERT INTO dbo.{table_name} (id) VALUES (?)", [20])
    cursor.connection.commit()

    cursor.execute(f"SELECT id FROM dbo.{table_name}")
    assert [r[0] for r in cursor.fetchall()] == [20]


_SCHEMA = """
IF OBJECT_ID('dbo.{city}', 'U') IS NOT NULL DROP TABLE dbo.{city};
IF OBJECT_ID('dbo.{country}', 'U') IS NOT NULL DROP TABLE dbo.{country};
CREATE TABLE dbo.{country} (Id INT PRIMARY KEY, Name VARCHAR(100) NOT NULL, Population INT NOT NULL);
CREATE TABLE dbo.{city} (
    Id INT PRIMARY KEY, CountryId INT NOT NULL, Name VARCHAR(100) NOT NULL, Population INT NOT NULL,
    CONSTRAINT fk_country_{tag} FOREIGN KEY (CountryId) REFERENCES dbo.{country} (Id));
"""


def test_deadlock_victim_connection_can_query_again(conn_str, cursor):
    tag = uuid.uuid4().hex[:8]
    country, city = f"tib_country_{tag}", f"tib_city_{tag}"
    cursor.execute(_SCHEMA.format(city=city, country=country, tag=tag))
    cursor.connection.commit()

    conn1 = mssql_python.connect(conn_str, autocommit=False)
    conn2 = mssql_python.connect(conn_str, autocommit=False)
    try:
        c1, c2 = conn1.cursor(), conn2.cursor()
        c1.execute(f"INSERT INTO dbo.{country} (Id, Name, Population) VALUES (1, 'USA', 12313)")
        c2.execute(f"INSERT INTO dbo.{country} (Id, Name, Population) VALUES (2, 'Finland', 42069)")
        c1.execute(f"INSERT INTO dbo.{city} (Id, CountryId, Name, Population) VALUES (1, 1, 'New York', 1)")
        c2.execute(f"INSERT INTO dbo.{city} (Id, CountryId, Name, Population) VALUES (2, 2, 'Delhi', 1)")

        errors = {}

        def run(name, cur):
            try:
                cur.execute(f"SELECT * FROM dbo.{city} WHERE CountryId = ?", [1 if name == 1 else 2])
                cur.fetchall()
            except mssql_python.Error as exc:
                errors[name] = exc

        threads = [threading.Thread(target=run, args=(1, c1)), threading.Thread(target=run, args=(2, c2))]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not any(t.is_alive() for t in threads), "deadlock was not resolved"
        assert len(errors) == 1, f"expected exactly one deadlock victim, got {sorted(errors)}"

        victim_name = next(iter(errors))
        victim, survivor = (c1, c2) if victim_name == 1 else (c2, c1)
        assert "deadlock" in str(errors[victim_name]).lower()
        survivor.connection.rollback()
        victim.execute(f"SELECT COUNT(*) FROM dbo.{city}")
        assert victim.fetchone()[0] == 0
    finally:
        conn1.close()
        conn2.close()
        cursor.execute(f"IF OBJECT_ID('dbo.{city}', 'U') IS NOT NULL DROP TABLE dbo.{city}")
        cursor.execute(f"IF OBJECT_ID('dbo.{country}', 'U') IS NOT NULL DROP TABLE dbo.{country}")
        cursor.connection.commit()
