"""MERGE: OUTPUT $action / inserted / deleted, rowcount, parameters, statement terminator.

Research sources (issue trackers and docs consulted for real-world failure modes):
- https://github.com/sqlalchemy/sqlalchemy/issues/4062  pyodbc cannot give a usable rowcount
  for UPDATE/DELETE/MERGE when OUTPUT is in effect (rowcount is -1 because a result set is
  returned); SQLAlchemy added sane_rowcount_w_returning for this
- https://github.com/mkleehammer/pyodbc/issues/700  fetch works only after nextset() when
  count messages arrive first; SET NOCOUNT ON is the usual fix
- https://github.com/microsoft/mssql-python/issues/863  Cursor.nextset() does not update
  rowcount for the next result
- https://github.com/microsoft/mssql-python/pull/864  rowcount per result set after nextset()
- https://github.com/jakesylvestre/pyodbc/issues/372  executemany() with MERGE runs the
  statement once per parameter row, so duplicate source keys do not raise the set-based error
- https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver17
  MERGE reference: statement must end with a semicolon; $action returns INSERT/UPDATE/DELETE;
  inserted/deleted are NULL for the side that does not apply; source row matching several
  target rows for UPDATE/DELETE raises error 8672
- https://database.guide/fix-a-merge-statement-must-be-terminated-by-a-semi-colon-in-sql-server-error-10713/
  error 10713 "A MERGE statement must be terminated by a semi-colon (;)"; with OUTPUT the
  semicolon goes after the OUTPUT clause
- https://github.com/lokander/hubro/pull/207  a script splitter dropped the ';' so every MERGE
  was rejected with 10713 (a client-side statement-splitting hazard)
- https://github.com/TableProApp/TablePro/pull/3115  the ';' must survive inside dynamic SQL
  (EXEC sp_executesql N'MERGE ...') as well

Behaviors a native-TDS reimplementation can get wrong:
- $action is a text column (nvarchar) with exactly 'INSERT', 'UPDATE' or 'DELETE', not an int.
- An OUTPUT clause makes the MERGE return a result set; the driver must expose it through
  description/fetch rather than treating the statement as a row-count-only command.
- rowcount for a MERGE without OUTPUT counts insert + update + delete rows, and skips
  WHEN MATCHED rows whose AND-condition is false.
- MERGE is terminated only by ';'. A missing terminator is a server error (10713), not a
  client-side success, and must surface as a DB-API exception.
- Parameters inside the USING source (VALUES (?, ?)) must bind by position.
"""

import pytest
from mssql_python import DataError, ProgrammingError


MERGE_UPSERT = """
MERGE #tgt AS t
USING (VALUES (?, ?)) AS s (id, v)
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED BY TARGET THEN INSERT (id, v) VALUES (s.id, s.v)
OUTPUT $action AS act, inserted.id AS new_id, deleted.id AS old_id, deleted.v AS old_v, inserted.v AS new_v;
"""


@pytest.fixture
def merge_tables(cursor):
    cursor.execute("CREATE TABLE #tgt (id INT PRIMARY KEY, v NVARCHAR(20) NULL)")
    cursor.execute("INSERT INTO #tgt (id, v) VALUES (1, N'one'), (2, N'two'), (3, N'three')")
    cursor.connection.commit()
    yield


def test_output_action_values_and_types(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (VALUES (2, N'TWO'), (4, N'four')) AS s (id, v)
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = s.v
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, v) VALUES (s.id, s.v)
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        OUTPUT $action AS act;
        """
    )
    # Target 1 and 3 are not in the source -> DELETE; 2 matches -> UPDATE; 4 is new -> INSERT.
    rows = sorted(tuple(r) for r in cursor.fetchall())
    assert rows == [("DELETE",), ("DELETE",), ("INSERT",), ("UPDATE",)]
    assert all(type(r[0]) is str for r in rows)


def test_output_inserted_and_deleted_pairs(cursor, merge_tables):
    cursor.execute(MERGE_UPSERT, (2, "TWO"))
    row = cursor.fetchone()
    assert tuple(row) == ("UPDATE", 2, 2, "two", "TWO")
    assert cursor.fetchone() is None


def test_output_insert_has_null_deleted_side(cursor, merge_tables):
    cursor.execute(MERGE_UPSERT, (9, "nine"))
    row = cursor.fetchone()
    assert tuple(row) == ("INSERT", 9, None, None, "nine")


def test_output_description_names_and_result_set(cursor, merge_tables):
    cursor.execute(MERGE_UPSERT, (1, "uno"))
    names = [d[0] for d in cursor.description]
    assert names == ["act", "new_id", "old_id", "old_v", "new_v"]
    assert cursor.fetchall()[0][0] == "UPDATE"


def test_output_into_table_returns_no_result_set(cursor, merge_tables):
    cursor.execute("CREATE TABLE #audit (act NVARCHAR(10), id INT)")
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (VALUES (1, N'x'), (50, N'y')) AS s (id, v)
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
        OUTPUT $action, inserted.id INTO #audit (act, id);
        """
    )
    assert cursor.description is None
    cursor.execute("SELECT act, id FROM #audit ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [("UPDATE", 1), ("INSERT", 50)]


def test_rowcount_without_output_counts_all_dml(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (VALUES (1, N'one'), (2, N'changed'), (7, N'seven'), (8, N'eight')) AS s (id, v)
        ON t.id = s.id
        WHEN MATCHED AND t.v <> s.v THEN UPDATE SET v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
        WHEN NOT MATCHED BY SOURCE THEN DELETE;
        """
    )
    # id 1 matches with identical value -> no-op, not counted.
    # id 2 updated, 7 and 8 inserted, 3 deleted (not in source) -> 4 rows.
    assert cursor.rowcount == 4
    cursor.execute("SELECT COUNT(*) FROM #tgt")
    assert cursor.fetchone()[0] == 4


def test_rowcount_zero_for_empty_source(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (SELECT id, v FROM (VALUES (1, N'x')) AS z (id, v) WHERE 1 = 0) AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
        OUTPUT $action AS act;
        """
    )
    assert cursor.fetchall() == []
    assert [d[0] for d in cursor.description] == ["act"]


def test_server_rowcount_matches_output_row_count(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (VALUES (1, N'a'), (2, N'b'), (60, N'c')) AS s (id, v)
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        OUTPUT $action AS act;
        """
    )
    out_rows = cursor.fetchall()
    assert len(out_rows) == 4  # 2 updates, 1 insert, 1 delete
    cursor.execute("SELECT @@ROWCOUNT")
    assert cursor.fetchone()[0] == 4


def test_merge_with_positional_params_in_source(cursor, merge_tables):
    cursor.execute(MERGE_UPSERT, (3, None))
    assert tuple(cursor.fetchone()) == ("UPDATE", 3, 3, "three", None)
    cursor.execute("SELECT v FROM #tgt WHERE id = 3")
    assert cursor.fetchone()[0] is None


def test_merge_params_unicode_and_rowcount(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (VALUES (?, ?)) AS s (id, v)
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
        """,
        (100, "日本語 ✓"),
    )
    assert cursor.rowcount == 1
    cursor.execute("SELECT v FROM #tgt WHERE id = 100")
    assert cursor.fetchone()[0] == "日本語 ✓"


def test_merge_params_in_on_and_when_clauses(cursor, merge_tables):
    cursor.execute(
        """
        MERGE #tgt AS t
        USING (SELECT 1 AS dummy) AS s
        ON t.id = ?
        WHEN MATCHED AND t.v <> ? THEN UPDATE SET v = ?
        OUTPUT $action AS act, inserted.v AS v;
        """,
        (2, "other", "deux"),
    )
    assert tuple(cursor.fetchone()) == ("UPDATE", "deux")


def test_merge_params_wrong_count_raises(cursor, merge_tables):
    with pytest.raises(ProgrammingError):
        cursor.execute(MERGE_UPSERT, (1,))


def test_merge_without_terminating_semicolon_is_rejected(cursor, merge_tables):
    # Server error 10713; the driver message carries the text, not the number.
    with pytest.raises(ProgrammingError, match="MERGE statement must be terminated by a semi-colon"):
        cursor.execute(
            """
            MERGE #tgt AS t
            USING (VALUES (1, N'x')) AS s (id, v)
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET v = s.v
            """
        )


def test_merge_with_output_missing_semicolon_is_rejected(cursor, merge_tables):
    with pytest.raises(ProgrammingError, match="MERGE statement must be terminated by a semi-colon"):
        cursor.execute(
            """
            MERGE #tgt AS t
            USING (VALUES (1, N'x')) AS s (id, v)
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET v = s.v
            OUTPUT $action
            """
        )


def test_merge_with_semicolon_inside_dynamic_sql(cursor, merge_tables):
    cursor.execute(
        "EXEC sp_executesql N'MERGE #tgt AS t USING (VALUES (1, N''z'')) AS s (id, v) "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v OUTPUT $action AS act;'"
    )
    assert cursor.fetchone()[0] == "UPDATE"
    cursor.execute("SELECT v FROM #tgt WHERE id = 1")
    assert cursor.fetchone()[0] == "z"


def test_merge_duplicate_source_match_raises_8672(cursor, merge_tables):
    # Server error 8672; the driver message carries the text, not the number.
    with pytest.raises(ProgrammingError, match="UPDATE or DELETE the same row more than once"):
        cursor.execute(
            """
            MERGE #tgt AS t
            USING (VALUES (1, N'a'), (1, N'b')) AS s (id, v)
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET v = s.v;
            """
        )


def test_merge_failed_statement_rolls_back_target(cursor, merge_tables):
    # Second source row cannot convert to INT (SQLSTATE 22018 -> DataError): whole statement undone.
    with pytest.raises(DataError):
        cursor.execute(
            """
            MERGE #tgt AS t
            USING (VALUES (1, N'changed'), (N'not-an-int', N'bad')) AS s (id, v)
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET v = s.v
            WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
            """
        )
    cursor.execute("SELECT v FROM #tgt WHERE id = 1")
    assert cursor.fetchone()[0] == "one"


def test_executemany_merge_upsert_final_state(cursor, merge_tables):
    cursor.executemany(
        """
        MERGE #tgt AS t
        USING (VALUES (?, ?)) AS s (id, v)
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
        """,
        [(1, "A"), (5, "E"), (6, "F")],
    )
    cursor.execute("SELECT id, v FROM #tgt WHERE id IN (1, 5, 6) ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "A"), (5, "E"), (6, "F")]


def test_output_identity_values_from_inserts(cursor):
    cursor.execute("CREATE TABLE #ident (id INT IDENTITY(10,5) PRIMARY KEY, v INT NOT NULL)")
    cursor.execute("CREATE TABLE #srcv (k INT NOT NULL, v INT NOT NULL)")
    cursor.execute("INSERT INTO #srcv (k, v) VALUES (1, 100), (2, 200)")
    cursor.connection.commit()
    cursor.execute(
        """
        MERGE #ident AS t
        USING #srcv AS s
        ON 1 = 0
        WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v)
        OUTPUT $action AS act, inserted.id AS new_id, inserted.v AS new_v;
        """
    )
    rows = sorted(tuple(r) for r in cursor.fetchall())
    assert [r[0] for r in rows] == ["INSERT", "INSERT"]
    assert sorted(r[2] for r in rows) == [100, 200]
    assert sorted(r[1] for r in rows) == [10, 15]


def test_output_decimal_and_datetime_types(cursor):
    from decimal import Decimal
    import datetime as dt

    cursor.execute(
        "CREATE TABLE #money (id INT PRIMARY KEY, amt DECIMAL(12,2) NOT NULL, "
        "at DATETIME2(3) NULL)"
    )
    cursor.execute("INSERT INTO #money VALUES (1, 10.50, NULL)")
    cursor.connection.commit()
    stamp = dt.datetime(2026, 3, 4, 5, 6, 7, 891000)
    cursor.execute(
        """
        MERGE #money AS t
        USING (VALUES (1, ?, ?)) AS s (id, amt, at)
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET amt = s.amt, at = s.at
        OUTPUT $action AS act, deleted.amt AS old_amt, inserted.amt AS new_amt, inserted.at AS new_at;
        """,
        (Decimal("99.99"), stamp),
    )
    act, old_amt, new_amt, new_at = cursor.fetchone()
    assert act == "UPDATE"
    assert old_amt == Decimal("10.50") and isinstance(old_amt, Decimal)
    assert new_amt == Decimal("99.99") and isinstance(new_amt, Decimal)
    assert new_at == stamp and isinstance(new_at, dt.datetime)
