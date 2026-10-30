"""String/binary truncation: 2628 vs legacy 8152 messages, parameter overflow,
silent explicit CAST, trailing-space rule, UTF-16 unit counting, exception class.

Research sources (issue trackers and docs; the rewrite must match the
behavior the ODBC driver shows through TDS):
- https://techcommunity.microsoft.com/blog/sqlserver/string-or-binary-data-would-be-truncated-replacing-the-infamous-error-8152/386014
  (error 2628 names table/column/value; 8152 is the legacy message; trace flag 460
  and VERBOSE_TRUNCATION_WARNINGS switch between them; compat level 150+ defaults to 2628)
- https://www.brentozar.com/archive/2019/03/how-to-fix-the-error-string-or-binary-data-would-be-truncated/
  (2628 vs 8152 changes the error number, so code that matches 8152 breaks)
- https://github.com/mkleehammer/pyodbc/issues/480 (fast_executemany silently
  inserted truncated varchar values when the non-fast path raised 8152)
- https://github.com/microsoft/mssql-python/issues/435 (VARCHAR round-trip at the exact
  column size; cited in test_rs_varchar_codepages.py)
- https://learn.microsoft.com/en-us/troubleshoot/power-platform/dataverse/tables-fields-and-relationships/string-or-binary-data-would-be-truncated-error
  (SQLSTATE 22001; the implicit-conversion failure happens on INSERT/UPDATE, not on CAST)
- trailing-space and UTF-16 length rules: observed on upstream while writing these
  tests (no single source URL retrieved for the trailing-space rule)
- https://www.sqlservercentral.com/articles/getting-more-information-from-the-string-or-binary-data-truncated-error

Behaviors a native-TDS reimplementation can easily get wrong:
- the error must be a truncation error (SQLSTATE 22001), not a generic one, and the
  statement must leave no partial row behind
- the exception class should be DataError (PEP 249 data errors); upstream currently
  raises ProgrammingError, so that test is an upstream_bug strict xfail
- explicit CAST(... AS VARCHAR(n)) truncates silently, while implicit INSERT/UPDATE
  of the same value raises
- trailing spaces that overflow a varchar are dropped without error
- nvarchar length counts UTF-16 code units, so one emoji needs NVARCHAR(2)
- the 2628 vs 8152 message text depends on VERBOSE_TRUNCATION_WARNINGS
"""

import uuid

import pytest
from mssql_python import connect, DataError, DatabaseError


def _create(cursor, ddl):
    cursor.execute(ddl)


def _count(cursor, table):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0]


@pytest.fixture
def scoped_config_restored(conn_str):
    """Reset VERBOSE_TRUNCATION_WARNINGS to ON after a test that turns it OFF.

    Uses its own connection because the setting is database-wide and must not
    be left OFF for other tests in the private database.
    """
    yield
    restore = connect(conn_str, autocommit=True)
    try:
        restore.cursor().execute(
            "ALTER DATABASE SCOPED CONFIGURATION SET VERBOSE_TRUNCATION_WARNINGS = ON"
        )
    finally:
        restore.close()


def test_literal_insert_too_long_varchar_names_table_and_column(cursor):
    _create(cursor, "CREATE TABLE #tr_lit (id INT NOT NULL, v VARCHAR(5) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #tr_lit (id, v) VALUES (1, 'abcdefgh')")
    msg = str(info.value)
    # 2628-style message: names the column and shows the value that did not fit.
    assert "String or binary data would be truncated" in msg
    assert "column 'v'" in msg
    assert "Truncated value" in msg
    assert "8152" not in msg


def test_param_too_long_varchar_raises_truncation_and_stores_nothing(cursor):
    _create(cursor, "CREATE TABLE #tr_p (id INT NOT NULL, v VARCHAR(5) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #tr_p (id, v) VALUES (?, ?)", 1, "abcdefgh")
    assert "would be truncated" in str(info.value)
    assert "column 'v'" in str(info.value)
    assert _count(cursor, "#tr_p") == 0


@pytest.mark.upstream_bug(
    reason=(
        "A parameter that overflows a varchar column (SQLSTATE 22001) raises "
        "ProgrammingError upstream. PEP 249 classifies data errors as DataError, and "
        "numeric overflow in the same driver already raises DataError."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_param_truncation_raises_data_error_class(cursor):
    _create(cursor, "CREATE TABLE #tr_cls (v VARCHAR(5) NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #tr_cls (v) VALUES (?)", "abcdefgh")


def test_param_exactly_fitting_varchar_is_accepted(cursor):
    _create(cursor, "CREATE TABLE #tr_fit (v VARCHAR(5) NULL)")
    cursor.execute("INSERT INTO #tr_fit (v) VALUES (?)", "abcde")
    cursor.execute("SELECT v, LEN(v) FROM #tr_fit")
    row = cursor.fetchone()
    assert row[0] == "abcde"
    assert row[1] == 5


def test_trailing_spaces_overflowing_varchar_are_dropped_without_error(cursor):
    _create(cursor, "CREATE TABLE #tr_sp (v VARCHAR(3) NULL)")
    cursor.execute("INSERT INTO #tr_sp (v) VALUES (?)", "abc  ")
    cursor.execute("SELECT v, DATALENGTH(v) FROM #tr_sp")
    row = cursor.fetchone()
    assert row[0] == "abc"
    assert row[1] == 3


def test_explicit_cast_varchar_truncates_silently(cursor):
    cursor.execute("SELECT CAST('abcdefgh' AS VARCHAR(3)) AS x")
    row = cursor.fetchone()
    assert row[0] == "abc"
    assert isinstance(row[0], str)


def test_explicit_cast_nvarchar_truncates_silently_to_units(cursor):
    cursor.execute("SELECT CAST(N'abcdefgh' AS NVARCHAR(3)) AS x")
    assert cursor.fetchone()[0] == "abc"


def test_explicit_cast_varbinary_truncates_silently_to_bytes(cursor):
    cursor.execute("SELECT CAST(0x0102030405 AS VARBINARY(2)) AS x")
    value = cursor.fetchone()[0]
    assert value == b"\x01\x02"
    assert isinstance(value, bytes)


def test_implicit_insert_select_of_wider_value_raises_where_cast_did_not(cursor):
    # Same data as the CAST tests above: no explicit CAST here, so the value is
    # converted implicitly into the narrower column and the statement fails.
    _create(cursor, "CREATE TABLE #tr_sel (v VARCHAR(3) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #tr_sel (v) SELECT v FROM (SELECT 'abcdefgh' AS v) s")
    assert "would be truncated" in str(info.value)
    assert _count(cursor, "#tr_sel") == 0


def test_binary_param_too_long_varbinary_raises(cursor):
    _create(cursor, "CREATE TABLE #tr_bin (b VARBINARY(4) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #tr_bin (b) VALUES (?)", b"\x01\x02\x03\x04\x05")
    assert "would be truncated" in str(info.value)
    assert _count(cursor, "#tr_bin") == 0


def test_update_to_too_long_value_raises_and_keeps_old_value(cursor):
    _create(cursor, "CREATE TABLE #tr_upd (id INT NOT NULL, v VARCHAR(5) NULL)")
    cursor.execute("INSERT INTO #tr_upd (id, v) VALUES (1, 'short')")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("UPDATE #tr_upd SET v = ? WHERE id = 1", "abcdefgh")
    assert "would be truncated" in str(info.value)
    cursor.execute("SELECT v FROM #tr_upd WHERE id = 1")
    assert cursor.fetchone()[0] == "short"


def test_nvarchar_counts_utf16_units_emoji_fits_only_in_two(cursor):
    _create(cursor, "CREATE TABLE #tr_u16 (n NVARCHAR(2) NULL)")
    emoji = "\U0001F600"  # one code point, two UTF-16 units
    cursor.execute("INSERT INTO #tr_u16 (n) VALUES (?)", emoji)
    cursor.execute("SELECT n, DATALENGTH(n) FROM #tr_u16")
    row = cursor.fetchone()
    assert row[0] == emoji
    assert row[1] == 4  # two UTF-16 units in NVARCHAR


def test_nvarchar_surrogate_pair_plus_letter_overflows_two_units(cursor):
    _create(cursor, "CREATE TABLE #tr_u16b (n NVARCHAR(2) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #tr_u16b (n) VALUES (?)", "a\U0001F600")
    assert "would be truncated" in str(info.value)
    assert _count(cursor, "#tr_u16b") == 0


def test_truncation_error_leaves_connection_usable(cursor):
    _create(cursor, "CREATE TABLE #tr_after (v VARCHAR(3) NULL)")
    cursor.execute("INSERT INTO #tr_after (v) VALUES ('ok')")
    with pytest.raises(DatabaseError):
        cursor.execute("INSERT INTO #tr_after (v) VALUES (?)", "toolong")
    cursor.execute("INSERT INTO #tr_after (v) VALUES ('yes')")
    cursor.execute("SELECT v FROM #tr_after ORDER BY v")
    assert [r[0] for r in cursor.fetchall()] == ["ok", "yes"]


@pytest.mark.parametrize(
    "setting, expect_verbose",
    [("OFF", False), ("ON", True)],
)
def test_verbose_truncation_setting_switches_2628_and_8152_message(
    conn_str, setting, expect_verbose, scoped_config_restored
):
    setup = connect(conn_str, autocommit=True)
    try:
        setup.cursor().execute(
            f"ALTER DATABASE SCOPED CONFIGURATION SET VERBOSE_TRUNCATION_WARNINGS = {setting}"
        )
    finally:
        setup.close()

    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE #tr_verb (v VARCHAR(3) NULL)")
        with pytest.raises(DatabaseError) as info:
            cur.execute("INSERT INTO #tr_verb (v) VALUES (?)", "abcdefgh")
        msg = str(info.value)
        assert "would be truncated" in msg
        if expect_verbose:
            assert "column 'v'" in msg
            assert "8152" not in msg
        else:
            # Legacy 8152 text carries no table/column/value detail; the number itself
            # is not in the message string, so assert the terse form instead.
            assert msg.rstrip().endswith("would be truncated.")
            assert "column 'v'" not in msg
            assert "Truncated value" not in msg
    finally:
        conn.close()


def test_long_varchar_max_param_is_not_truncated(cursor):
    _create(cursor, "CREATE TABLE #tr_max (v VARCHAR(MAX) NULL)")
    payload = "x" * 9000
    cursor.execute("INSERT INTO #tr_max (v) VALUES (?)", payload)
    cursor.execute("SELECT LEN(v) FROM #tr_max")
    assert cursor.fetchone()[0] == 9000


def test_empty_string_into_narrow_varchar_is_not_truncation(cursor):
    _create(cursor, "CREATE TABLE #tr_empty (v VARCHAR(1) NULL)")
    cursor.execute("INSERT INTO #tr_empty (v) VALUES (?)", "")
    cursor.execute("SELECT v, DATALENGTH(v) FROM #tr_empty")
    row = cursor.fetchone()
    assert row[0] == ""
    assert row[1] == 0
