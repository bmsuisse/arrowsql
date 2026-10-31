"""System-versioned temporal tables: AS OF with datetime2 params, hidden period columns,
history after UPDATE/DELETE, and UTC semantics.

Research sources (issue trackers and docs; no mssql-python or pyodbc issue on this exact
topic was found, so the failure modes below come from the SQL Server docs and from
adjacent reports):
- https://learn.microsoft.com/en-us/sql/relational-databases/tables/querying-data-in-a-system-versioned-temporal-table
- https://learn.microsoft.com/en-us/sql/relational-databases/tables/modifying-data-in-a-system-versioned-temporal-table
- https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal/considerations-limitations
- https://github.com/Schema-Smith/SchemaSmith/issues/523 (hidden period columns re-deployed as visible)
- https://github.com/peopleworks/SqlSchemaDiff/pull/9 (period columns still GENERATED ALWAYS, explicit INSERT refused)
- https://github.com/pongbiphang/pbps/issues/1176 (system-versioned and history tables skipped during schema pull)
- https://github.com/microsoft/data-formulator/issues/473 (mssql_python + datetime2(7) period columns, rowversion)
- https://github.com/sqlalchemy/sqlalchemy/discussions/11444 (unexpected txn start times on temporal tables)

Failure modes asserted here: hidden period columns must not leak into SELECT * or
cursor.description; they must still be readable when named; datetime2(7) period values
must come back as naive datetime.datetime in UTC; AS OF boundaries are inclusive on
ValidFrom and exclusive on ValidTo; datetime params must be honored at microsecond
resolution; explicit writes to GENERATED ALWAYS columns must raise ProgrammingError; the
history table must receive the previous version on UPDATE and DELETE; rollback must discard
version changes. A version's boundary is the start time of the transaction that wrote it,
so a native-TDS driver must not begin transactions before the statement that writes.

Temporal tables cannot be #temp tables, so every test uses a unique permanent table and
its history table, both dropped by the fixture.
"""

import datetime as dt
import time

import pytest
from mssql_python import ProgrammingError

FAR_FUTURE = dt.datetime(9999, 12, 31, 23, 59, 59, 999999)


@pytest.fixture
def temporal(cursor, conn, table_name):
    """Create dbo.<table_name> as a system-versioned table with HIDDEN period columns."""
    hist = f"{table_name}_hist"
    cursor.execute(
        f"""
        CREATE TABLE dbo.{table_name} (
            id INT NOT NULL PRIMARY KEY,
            val NVARCHAR(50) NULL,
            ValidFrom DATETIME2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
            ValidTo DATETIME2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
        )
        WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.{hist}))
        """
    )
    conn.commit()
    yield table_name, hist
    try:
        conn.rollback()
        cursor.execute(
            f"IF OBJECT_ID('dbo.{table_name}') IS NOT NULL "
            f"ALTER TABLE dbo.{table_name} SET (SYSTEM_VERSIONING = OFF)"
        )
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{hist}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _next_version():
    """Let the system clock move on before the next versioning change.

    Two commits inside one clock tick give the superseded version a zero-length
    period, which FOR SYSTEM_TIME ALL leaves out. Both drivers are fast enough
    to hit that on this server, so tests that count versions wait a little.
    """
    time.sleep(0.02)


def _rows(cursor):
    """Row objects from fetchall() as plain tuples."""
    return [tuple(r) for r in cursor.fetchall()]


def _current_row(cursor, name, row_id):
    cursor.execute(f"SELECT id, val FROM dbo.{name} WHERE id = ?", (row_id,))
    return tuple(cursor.fetchone())


def test_select_star_hides_period_columns(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"SELECT * FROM dbo.{name}")
    assert [d[0] for d in cursor.description] == ["id", "val"]
    assert _rows(cursor) == [(1, "a")]


def test_named_hidden_period_columns_are_readable_with_naive_utc_datetimes(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"SELECT ValidFrom, ValidTo FROM dbo.{name}")
    valid_from, valid_to = cursor.fetchone()
    assert type(valid_from) is dt.datetime
    assert type(valid_to) is dt.datetime
    assert valid_from.tzinfo is None
    assert valid_to == FAR_FUTURE


def test_sys_columns_flag_period_columns_hidden_and_generated(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(
        "SELECT c.name, c.is_hidden, c.generated_always_type_desc "
        "FROM sys.columns c WHERE c.object_id = OBJECT_ID(?) ORDER BY c.column_id",
        (f"dbo.{name}",),
    )
    assert _rows(cursor) == [
        ("id", False, "NOT_APPLICABLE"),
        ("val", False, "NOT_APPLICABLE"),
        ("ValidFrom", True, "AS_ROW_START"),
        ("ValidTo", True, "AS_ROW_END"),
    ]


def test_period_value_is_utc_now_at_insert(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"SELECT ValidFrom, SYSUTCDATETIME() FROM dbo.{name} WHERE id = 1")
    valid_from, server_utc = cursor.fetchone()
    assert type(valid_from) is dt.datetime
    # ValidFrom is a UTC timestamp: it must be within seconds of SYSUTCDATETIME().
    assert abs(server_utc - valid_from) < dt.timedelta(seconds=5)


def test_explicit_insert_into_period_columns_raises_programming_error(cursor, conn, temporal):
    name, _ = temporal
    with pytest.raises(ProgrammingError, match="GENERATED ALWAYS"):
        cursor.execute(
            f"INSERT INTO dbo.{name} (id, val, ValidFrom, ValidTo) "
            f"VALUES (1, 'x', SYSUTCDATETIME(), SYSUTCDATETIME())"
        )
    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
    assert cursor.fetchone()[0] == 0


def test_update_of_period_column_raises_programming_error(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    with pytest.raises(ProgrammingError, match="GENERATED ALWAYS"):
        cursor.execute(f"UPDATE dbo.{name} SET ValidFrom = SYSUTCDATETIME() WHERE id = 1")
    conn.rollback()
    assert _current_row(cursor, name, 1) == (1, "a")


def test_update_moves_previous_version_to_history_with_matching_boundary(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    cursor.execute(f"SELECT id, val FROM dbo.{hist}")
    assert _rows(cursor) == [(1, "a")]
    # The superseded version closes exactly when the current version opens.
    cursor.execute(
        f"SELECT CASE WHEN h.ValidTo = c.ValidFrom THEN 1 ELSE 0 END "
        f"FROM dbo.{hist} h JOIN dbo.{name} c ON c.id = h.id"
    )
    assert cursor.fetchone()[0] == 1


@pytest.mark.upstream_bug(
    reason=(
        "upstream opens a transaction eagerly on commit() (@@TRANCOUNT is 1 right after "
        "commit), so system-time boundaries are stamped at the previous commit instead of "
        "the UPDATE; the superseded version gets a zero-length window"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_version_window_reflects_when_update_ran(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    time.sleep(1.0)
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    cursor.execute(f"SELECT ValidFrom, ValidTo FROM dbo.{hist} WHERE id = 1")
    valid_from, valid_to = cursor.fetchone()
    assert valid_to - valid_from >= dt.timedelta(milliseconds=500)


def test_as_of_string_boundary_is_inclusive_on_valid_from(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    # Full 100ns precision comes from the server and is bound back as a string, so no
    # Python-side truncation can move the boundary.
    cursor.execute(f"SELECT CONVERT(VARCHAR(40), ValidTo, 121) FROM dbo.{hist} WHERE id = 1")
    boundary = cursor.fetchone()[0]
    cursor.execute(f"SELECT CONVERT(VARCHAR(40), ValidFrom, 121) FROM dbo.{name} WHERE id = 1")
    assert cursor.fetchone()[0] == boundary
    cursor.execute(f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?", (boundary,))
    assert _rows(cursor) == [("b",)]


def test_as_of_python_datetime_param_selects_version_in_window(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    time.sleep(0.5)
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    cursor.execute(f"SELECT ValidFrom FROM dbo.{name} WHERE id = 1")
    current_from = cursor.fetchone()[0]
    assert type(current_from) is dt.datetime

    # Well after the current version opened: the current row answers.
    cursor.execute(
        f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?",
        (current_from + dt.timedelta(milliseconds=500),),
    )
    assert _rows(cursor) == [("b",)]
    # Long before any version existed: nothing.
    cursor.execute(
        f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?",
        (current_from - dt.timedelta(days=1),),
    )
    assert _rows(cursor) == []


def test_as_of_before_first_version_returns_no_rows(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(
        f"SELECT id, val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?",
        (dt.datetime(2000, 1, 1, 0, 0, 0),),
    )
    assert _rows(cursor) == []


def test_as_of_iso_string_param_resolves_like_datetime(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(
        f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?", ("2000-01-01T00:00:00",)
    )
    assert _rows(cursor) == []
    cursor.execute(
        f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME AS OF ?", ("9999-12-31T23:59:59",)
    )
    assert _rows(cursor) == [("a",)]


@pytest.mark.upstream_bug(
    reason=(
        "upstream's eager transaction on commit() stamps each version at the previous "
        "commit, so a version can still get a zero-length window and drop out of "
        "FOR SYSTEM_TIME ALL (see test_version_window_reflects_when_update_ran)"
    )
)
@pytest.mark.xfail(strict=False, reason="upstream bug, timing dependent")
def test_for_system_time_all_returns_every_version_and_current(cursor, conn, temporal):
    name, _ = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    _next_version()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    _next_version()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'c' WHERE id = 1")
    conn.commit()
    cursor.execute(
        f"SELECT val, ValidTo FROM dbo.{name} FOR SYSTEM_TIME ALL ORDER BY ValidFrom"
    )
    rows = _rows(cursor)
    assert [r[0] for r in rows] == ["a", "b", "c"]
    assert rows[-1][1] == FAR_FUTURE


@pytest.mark.upstream_bug(
    reason=(
        "upstream's eager transaction on commit() can give version 'a' a zero-length "
        "window, which CONTAINED IN never returns"
    )
)
@pytest.mark.xfail(strict=False, reason="upstream bug, timing dependent")
def test_contained_in_with_datetime2_params_returns_closed_versions_only(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    _next_version()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.commit()
    cursor.execute(f"SELECT ValidFrom, ValidTo FROM dbo.{hist} WHERE id = 1")
    a_from, a_to = cursor.fetchone()
    # The window covers version 'a' completely; 'b' runs to 9999-12-31, so it is not contained.
    cursor.execute(
        f"SELECT val FROM dbo.{name} FOR SYSTEM_TIME CONTAINED IN (?, ?)",
        (a_from - dt.timedelta(seconds=1), a_to + dt.timedelta(seconds=1)),
    )
    assert _rows(cursor) == [("a",)]


@pytest.mark.upstream_bug(
    reason=(
        "upstream's eager transaction on commit() can give the deleted version a "
        "zero-length window, so FOR SYSTEM_TIME ALL leaves it out"
    )
)
@pytest.mark.xfail(strict=False, reason="upstream bug, timing dependent")
def test_delete_moves_row_to_history(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    _next_version()
    cursor.execute(f"DELETE FROM dbo.{name} WHERE id = 1")
    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
    assert cursor.fetchone()[0] == 0
    cursor.execute(f"SELECT id, val FROM dbo.{hist}")
    assert _rows(cursor) == [(1, "a")]
    cursor.execute(f"SELECT id, val FROM dbo.{name} FOR SYSTEM_TIME ALL")
    assert _rows(cursor) == [(1, "a")]


def test_rollback_discards_version_change(cursor, conn, temporal):
    name, hist = temporal
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a')")
    conn.commit()
    cursor.execute(f"UPDATE dbo.{name} SET val = 'b' WHERE id = 1")
    conn.rollback()
    assert _current_row(cursor, name, 1) == (1, "a")
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{hist}")
    assert cursor.fetchone()[0] == 0


def test_executemany_insert_gives_each_row_an_open_period(cursor, conn, temporal):
    name, _ = temporal
    cursor.executemany(
        f"INSERT INTO dbo.{name} (id, val) VALUES (?, ?)",
        [(1, "a"), (2, "b"), (3, "c")],
    )
    conn.commit()
    cursor.execute(f"SELECT id, val, ValidTo FROM dbo.{name} ORDER BY id")
    rows = _rows(cursor)
    assert [(r[0], r[1]) for r in rows] == [(1, "a"), (2, "b"), (3, "c")]
    assert all(r[2] == FAR_FUTURE for r in rows)
