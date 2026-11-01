"""Row API, cursor.description, fetch interleaving and rowcount.

Common-issue coverage:
- index, key and attribute access on Row, including column names with spaces,
  dots, reserved words, duplicates and unnamed (expression) columns;
- cursor.description tuple layout (name, type_code, display_size,
  internal_size, precision, scale, null_ok) and Python type codes;
- fetchone / fetchmany / fetchall / iteration interleaving and arraysize;
- rowcount for SELECT (-1 until rows are consumed) and for DML.

Ground truth is the upstream ODBC driver (.venv-upstream).
"""

import datetime
import decimal
import uuid

import pytest
from mssql_python.exceptions import ProgrammingError
from mssql_python.row import Row


# ---------------------------------------------------------------------------
# Row access: index, key, attribute
# ---------------------------------------------------------------------------


def test_index_key_and_attribute_access_agree(cursor):
    cursor.execute("CREATE TABLE #acc (id INT, name NVARCHAR(20))")
    cursor.execute("INSERT INTO #acc VALUES (7, N'seven')")
    cursor.execute("SELECT id, name FROM #acc")
    row = cursor.fetchone()

    assert row[0] == 7
    assert row["id"] == 7
    assert row.id == 7
    assert row[1] == "seven"
    assert row["name"] == "seven"
    assert row.name == "seven"


def test_negative_index_addresses_from_end(cursor):
    cursor.execute("SELECT 1 AS a, N'mid' AS b, 3.5 AS c")
    row = cursor.fetchone()

    assert row[-1] == decimal.Decimal("3.5")
    assert row[-2] == "mid"
    assert row[-3] == 1
    assert len(row) == 3
    assert tuple(row) == (1, "mid", decimal.Decimal("3.5"))




def test_column_name_with_spaces_accessible_by_key_and_getattr(cursor):
    cursor.execute("CREATE TABLE #sp ([full name] NVARCHAR(20), [a.b] INT)")
    cursor.execute("INSERT INTO #sp VALUES (N'Ada Lovelace', 5)")
    cursor.execute("SELECT [full name], [a.b] FROM #sp")

    assert [d[0] for d in cursor.description] == ["full name", "a.b"]
    row = cursor.fetchone()
    assert row["full name"] == "Ada Lovelace"
    assert getattr(row, "full name") == "Ada Lovelace"
    assert row["a.b"] == 5
    assert getattr(row, "a.b") == 5


def test_reserved_word_and_non_ascii_column_names(cursor):
    cursor.execute("SELECT 1 AS [select], 2 AS [Größe], 3 AS [count]")
    row = cursor.fetchone()

    assert getattr(row, "select") == 1
    assert getattr(row, "Größe") == 2
    assert row["Größe"] == 2
    # A column named like a list method still resolves to the column value.
    assert row["count"] == 3
    assert getattr(row, "count") == 3


def test_duplicate_column_names_are_positionally_exact(cursor):
    cursor.execute("SELECT 10 AS dup, 20 AS dup")
    row = cursor.fetchone()

    assert [d[0] for d in cursor.description] == ["dup", "dup"]
    assert row[0] == 10
    assert row[1] == 20


def test_duplicate_column_name_lookup_returns_last_occurrence(cursor):
    # Name lookup is ambiguous for duplicates; upstream resolves to the last
    # occurrence for both key and attribute access. Positional access is
    # covered by the test above.
    cursor.execute("SELECT 10 AS dup, 20 AS dup")
    row = cursor.fetchone()

    assert row["dup"] == 20
    assert row.dup == 20


def test_unnamed_expression_column_has_empty_name(cursor):
    cursor.execute("SELECT 1 + 1, N'lit' AS label")
    names = [d[0] for d in cursor.description]
    row = cursor.fetchone()

    assert names == ["", "label"]
    assert row[0] == 2
    assert row["label"] == "lit"


def test_unnamed_column_positional_and_duplicate_alias(cursor):
    cursor.execute("SELECT 1, 2 AS x, 3 AS x")
    row = cursor.fetchone()

    assert [d[0] for d in cursor.description] == ["", "x", "x"]
    assert tuple(row) == (1, 2, 3)
    assert row[0] == 1
    assert row.x == 3


def test_missing_key_attribute_and_index_raise_expected_exceptions(cursor):
    cursor.execute("SELECT 1 AS id")
    row = cursor.fetchone()

    with pytest.raises(KeyError):
        row["nope"]
    with pytest.raises(AttributeError):
        row.nope
    with pytest.raises(IndexError):
        row[99]


def test_column_name_lookup_is_case_sensitive(cursor):
    cursor.execute("SELECT 1 AS Identifier")
    row = cursor.fetchone()

    assert row["Identifier"] == 1
    with pytest.raises(KeyError):
        row["identifier"]


@pytest.mark.upstream_bug(
    reason=(
        "Row attribute lookup resolves the driver's internal attributes "
        "(_values, _cursor) before column names, so a column named "
        "_values returns the internal value list instead of the column value. "
        "_mapping is not tested here: it is a documented property (see Row._mapping), "
        "so a column named _mapping is read with row['_mapping']."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
@pytest.mark.parametrize("column", ["_values", "_cursor"])
def test_underscore_column_name_is_not_shadowed_by_internals(cursor, column):
    cursor.execute(f"SELECT 42 AS [{column}]")
    row = cursor.fetchone()

    assert row[column] == 42
    assert getattr(row, column) == 42


# ---------------------------------------------------------------------------
# NULL handling across access paths
# ---------------------------------------------------------------------------


def test_null_is_none_via_every_access_path_and_empty_string_is_not_null(cursor):
    cursor.execute("CREATE TABLE #nul (id INT, note VARCHAR(5) NULL)")
    cursor.execute("INSERT INTO #nul VALUES (1, NULL), (2, '')")
    cursor.execute("SELECT id, note FROM #nul ORDER BY id")
    null_row = cursor.fetchone()
    empty_row = cursor.fetchone()

    assert null_row[1] is None
    assert null_row["note"] is None
    assert null_row.note is None
    assert empty_row[1] == ""
    assert empty_row.note == ""
    assert empty_row.note is not None


# ---------------------------------------------------------------------------
# cursor.description
# ---------------------------------------------------------------------------


def test_description_type_codes_are_python_types_and_sizes_exact(cursor):
    cursor.execute(
        "CREATE TABLE #types ("
        " id INT NOT NULL,"
        " name NVARCHAR(50) NULL,"
        " amt DECIMAL(10,2) NULL,"
        " flt FLOAT NULL,"
        " dt DATETIME2(3) NULL,"
        " b VARBINARY(8) NULL,"
        " bit_ BIT NULL,"
        " vc VARCHAR(12) NULL)"
    )
    cursor.execute("SELECT * FROM #types")

    assert list(cursor.description) == [
        ("id", int, None, 10, 10, 0, False),
        ("name", str, None, 50, 50, 0, True),
        ("amt", decimal.Decimal, None, 10, 10, 2, True),
        ("flt", float, None, 53, 53, 0, True),
        ("dt", datetime.datetime, None, 23, 23, 3, True),
        ("b", bytes, None, 8, 8, 0, True),
        ("bit_", bool, None, 1, 1, 0, True),
        ("vc", str, None, 12, 12, 0, True),
    ]


def test_description_for_less_common_types(cursor):
    cursor.execute(
        "SELECT CAST(1 AS BIGINT) AS bi, CAST(1 AS SMALLINT) AS si,"
        " CAST(1 AS TINYINT) AS ti, CAST(1.5 AS REAL) AS re,"
        " CAST('2024-05-06' AS DATE) AS d, CAST(NEWID() AS UNIQUEIDENTIFIER) AS u"
    )
    types = {d[0]: d[1] for d in cursor.description}

    assert types == {
        "bi": int,
        "si": int,
        "ti": int,
        "re": float,
        "d": datetime.date,
        "u": uuid.UUID,
    }




def test_description_nullability_from_permanent_table(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, label NVARCHAR(10) NULL)"
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT id, label FROM dbo.{table_name}")

    assert cursor.description[0][:2] == ("id", int)
    assert cursor.description[0][6] is False
    assert cursor.description[1][:2] == ("label", str)
    assert cursor.description[1][6] is True


def test_description_present_for_empty_result_set(cursor):
    cursor.execute("CREATE TABLE #empty (id INT, name NVARCHAR(5))")
    cursor.execute("SELECT id, name FROM #empty")

    assert [d[0] for d in cursor.description] == ["id", "name"]
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []




# ---------------------------------------------------------------------------
# fetchone / fetchmany / fetchall / iteration
# ---------------------------------------------------------------------------


@pytest.fixture
def seven_rows(cursor):
    cursor.execute("CREATE TABLE #seven (id INT NOT NULL, v NVARCHAR(10) NULL)")
    cursor.executemany(
        "INSERT INTO #seven VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 8)]
    )
    return 7


def test_arraysize_defaults_to_one_and_drives_fetchmany(cursor, seven_rows):
    assert cursor.arraysize == 1

    cursor.execute("SELECT id FROM #seven ORDER BY id")
    assert [tuple(r) for r in cursor.fetchmany()] == [(1,)]

    cursor.arraysize = 3
    assert [tuple(r) for r in cursor.fetchmany()] == [(2,), (3,), (4,)]


def test_fetchmany_partial_final_batch_then_empty(cursor, seven_rows):
    cursor.execute("SELECT id FROM #seven ORDER BY id")

    assert [tuple(r) for r in cursor.fetchmany(2)] == [(1,), (2,)]
    assert [tuple(r) for r in cursor.fetchmany(2)] == [(3,), (4,)]
    assert [tuple(r) for r in cursor.fetchmany(2)] == [(5,), (6,)]
    assert [tuple(r) for r in cursor.fetchmany(2)] == [(7,)]
    assert cursor.fetchone() is None
    assert cursor.fetchmany(2) == []
    assert cursor.fetchall() == []
    assert cursor.fetchmany(0) == []


def test_fetchone_fetchmany_fetchall_interleave_without_loss(cursor, seven_rows):
    cursor.execute("SELECT id, v FROM #seven ORDER BY id")

    assert tuple(cursor.fetchone()) == (1, "v1")
    assert [tuple(r) for r in cursor.fetchmany(2)] == [(2, "v2"), (3, "v3")]
    assert tuple(cursor.fetchone()) == (4, "v4")
    rest = cursor.fetchall()
    assert [tuple(r) for r in rest] == [(5, "v5"), (6, "v6"), (7, "v7")]




def test_iteration_yields_rows_and_stops(cursor, seven_rows):
    cursor.execute("SELECT id FROM #seven WHERE id <= 2 ORDER BY id")
    rows = list(cursor)

    assert [tuple(r) for r in rows] == [(1,), (2,)]
    assert all(isinstance(r, Row) for r in rows)
    with pytest.raises(StopIteration):
        next(cursor)


def test_iteration_shares_position_with_fetchone(cursor, seven_rows):
    cursor.execute("SELECT id FROM #seven ORDER BY id")
    it = iter(cursor)

    assert next(it)[0] == 1
    assert cursor.fetchone()[0] == 2
    assert [r[0] for r in it] == [3, 4, 5, 6, 7]


# ---------------------------------------------------------------------------
# rowcount
# ---------------------------------------------------------------------------


def test_rowcount_is_minus_one_for_select_until_rows_consumed(cursor, seven_rows):
    cursor.execute("SELECT id FROM #seven")
    assert cursor.rowcount == -1

    assert len(cursor.fetchall()) == 7
    assert cursor.rowcount == 7


def test_rowcount_for_dml_and_no_fetch_after_dml(cursor, seven_rows):
    cursor.execute("UPDATE #seven SET v = v WHERE id <= 4")
    assert cursor.rowcount == 4
    assert cursor.description is None

    with pytest.raises(ProgrammingError):
        cursor.fetchone()


def _plain_row():
    from arrowsql.row import Row

    row = Row([1, "x"], {"a": 0, "b": 1}, None, [], None, None, ("a", "b"))
    row.user_attr = 5
    return row


def test_row_copy_pickle_deepcopy_round_trip():
    import copy
    import pickle

    row = _plain_row()
    for clone in (copy.copy(row), copy.deepcopy(row), pickle.loads(pickle.dumps(row))):
        assert type(clone) is type(row)
        assert clone == row
        assert clone.a == 1 and clone["b"] == "x"
        assert clone.user_attr == 5
        assert dict(clone._mapping) == {"a": 1, "b": "x"}


def test_row_unset_fields_do_not_recurse():
    from arrowsql.row import Row

    row = Row.__new__(Row)
    with pytest.raises(AttributeError):
        row.anything
