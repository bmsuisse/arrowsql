"""Binary and GUID round-trips for the mssql_python driver.

Covers bytes/bytearray/memoryview parameters, VARBINARY(MAX) with zero bytes,
BINARY(n) zero padding, uniqueidentifier round-trips as uuid.UUID and str,
NEWID() and comparisons of UUID parameters.
"""

import uuid

import mssql_python
import pytest
from mssql_python import connect

ALL_BYTES = bytes(range(256))


def _one(cursor, sql, params=()):
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return tuple(row) if row is not None else None


def _all(cursor, sql, params=()):
    cursor.execute(sql, params)
    return [tuple(row) for row in cursor.fetchall()]


@pytest.fixture
def bin_table(cursor):
    cursor.execute(
        "CREATE TABLE #bin ("
        " id int NOT NULL PRIMARY KEY,"
        " v varbinary(max) NULL,"
        " f binary(8) NULL,"
        " s varbinary(10) NULL)"
    )
    return "#bin"


@pytest.fixture
def guid_table(cursor):
    cursor.execute(
        "CREATE TABLE #guid ("
        " id int NOT NULL PRIMARY KEY,"
        " g uniqueidentifier NULL,"
        " g2 uniqueidentifier NOT NULL DEFAULT NEWID())"
    )
    return "#guid"


# ---------------------------------------------------------------------------
# bytes / bytearray / memoryview parameters
# ---------------------------------------------------------------------------


def test_bytes_all_256_values_roundtrip_exactly(cursor, bin_table):
    cursor.execute(f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (ALL_BYTES,))
    got = _one(cursor, f"SELECT v, DATALENGTH(v) FROM {bin_table} WHERE id = 1")
    assert type(got[0]) is bytes
    assert got[0] == ALL_BYTES
    assert got[1] == 256


def test_bytearray_param_is_stored_and_returned_as_bytes(cursor, bin_table):
    cursor.execute(
        f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (bytearray(b"\x00ab\xff"),)
    )
    got = _one(cursor, f"SELECT v FROM {bin_table} WHERE id = 1")[0]
    assert type(got) is bytes
    assert got == b"\x00ab\xff"


@pytest.mark.upstream_bug(
    reason=(
        "memoryview is a buffer-protocol binary type (PEP 249 BINARY); upstream "
        "rejects it with TypeError 'Unsupported parameter type' instead of binding it"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_memoryview_param_binds_as_binary(cursor, bin_table):
    cursor.execute(
        f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (memoryview(b"\x00mv\x00"),)
    )
    got = _one(cursor, f"SELECT v FROM {bin_table} WHERE id = 1")[0]
    assert got == b"\x00mv\x00"


# ---------------------------------------------------------------------------
# VARBINARY(MAX): zero bytes, empty, large payloads
# ---------------------------------------------------------------------------


def test_varbinary_max_keeps_leading_trailing_and_embedded_zero_bytes(
    cursor, bin_table
):
    payloads = {
        1: b"\x00",
        2: b"\x00" * 5,
        3: b"a\x00b\x00c",
        4: b"\x00\x01\x00",
    }
    for row_id, value in payloads.items():
        cursor.execute(
            f"INSERT INTO {bin_table} (id, v) VALUES (?, ?)", (row_id, value)
        )
    rows = _all(
        cursor,
        f"SELECT id, v, DATALENGTH(v) FROM {bin_table} ORDER BY id",
    )
    assert [(r[0], r[1], r[2]) for r in rows] == [
        (1, b"\x00", 1),
        (2, b"\x00" * 5, 5),
        (3, b"a\x00b\x00c", 5),
        (4, b"\x00\x01\x00", 3),
    ]


def test_varbinary_max_empty_bytes_is_not_null(cursor, bin_table):
    cursor.execute(f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (b"",))
    got = _one(
        cursor,
        f"SELECT v, CASE WHEN v IS NULL THEN 1 ELSE 0 END, DATALENGTH(v) "
        f"FROM {bin_table} WHERE id = 1",
    )
    assert got == (b"", 0, 0)


@pytest.mark.parametrize("size", [8001, 100_000])
def test_varbinary_max_large_payload_exact(cursor, bin_table, size):
    value = (b"\x00\xfe" * (size // 2 + 1))[:size]
    cursor.execute(f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (value,))
    got = _one(cursor, f"SELECT v FROM {bin_table} WHERE id = 1")[0]
    assert len(got) == size
    assert got == value


# ---------------------------------------------------------------------------
# BINARY(n): zero padding and overflow
# ---------------------------------------------------------------------------


def test_binary_n_pads_short_value_with_trailing_zero_bytes(cursor, bin_table):
    cursor.execute(f"INSERT INTO {bin_table} (id, f) VALUES (1, ?)", (b"\xff",))
    got = _one(cursor, f"SELECT f, DATALENGTH(f) FROM {bin_table} WHERE id = 1")
    assert got == (b"\xff" + b"\x00" * 7, 8)


def test_binary_n_overflow_raises_truncation_and_stores_nothing(cursor, bin_table):
    with pytest.raises(mssql_python.DatabaseError, match="truncated"):
        cursor.execute(
            f"INSERT INTO {bin_table} (id, f) VALUES (1, ?)", (b"0123456789",)
        )
    assert _one(cursor, f"SELECT COUNT(*) FROM {bin_table}") == (0,)


# ---------------------------------------------------------------------------
# NULL and binary comparisons
# ---------------------------------------------------------------------------


def test_literal_null_in_binary_columns_reads_back_none(cursor, bin_table):
    cursor.execute(
        f"INSERT INTO {bin_table} (id, v, f, s) VALUES (1, NULL, NULL, NULL)"
    )
    got = _one(cursor, f"SELECT v, f, s FROM {bin_table} WHERE id = 1")
    assert got == (None, None, None)


@pytest.mark.upstream_bug(
    reason=(
        "Python None bound as a parameter is sent as a varchar NULL, so inserting it into "
        "a varbinary/binary column fails with 'Implicit conversion from data type varchar "
        "to varbinary(max) is not allowed'; it should bind as a typed SQL NULL"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_none_param_into_varbinary_max_stores_null(cursor, bin_table):
    cursor.execute(f"INSERT INTO {bin_table} (id, v) VALUES (1, ?)", (None,))
    got = _one(cursor, f"SELECT v FROM {bin_table} WHERE id = 1")[0]
    assert got is None


def test_bytes_where_equality_selects_exact_row(cursor, bin_table):
    cursor.execute(
        f"INSERT INTO {bin_table} (id, v) VALUES (1, ?), (2, ?), (3, ?)",
        (b"\x00\x01", b"\x00\x02", b"\x00\x01\x00"),
    )
    rows = _all(cursor, f"SELECT id FROM {bin_table} WHERE v = ?", (b"\x00\x02",))
    assert rows == [(2,)]


def test_binary_n_equality_matches_zero_padded_column(cursor, bin_table):
    # SQL Server stores b"ab" in BINARY(8) as b"ab" + 6 zero bytes, and a
    # BINARY equality against the shorter literal still matches that row.
    cursor.execute(f"INSERT INTO {bin_table} (id, f) VALUES (1, ?)", (b"ab",))
    rows = _all(cursor, f"SELECT id FROM {bin_table} WHERE f = ?", (b"ab",))
    assert rows == [(1,)]


# ---------------------------------------------------------------------------
# uniqueidentifier: round-trips as uuid.UUID and str
# ---------------------------------------------------------------------------


def test_uuid_param_roundtrips_as_uuid_object(cursor, guid_table):
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value,))
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert type(got) is uuid.UUID
    assert got == value
    assert str(got) == "12345678-1234-5678-1234-567812345678"


@pytest.mark.parametrize("text", ["ABCDEF01-2345-6789-ABCD-EF0123456789"])
def test_guid_string_param_is_returned_as_uuid_object(cursor, guid_table, text):
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (text,))
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert type(got) is uuid.UUID
    assert got == uuid.UUID(text)


def test_null_guid_roundtrips_as_none(cursor, guid_table):
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (None,))
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert got is None


def test_executemany_uuid_objects_roundtrip(cursor, guid_table):
    values = [uuid.UUID(int=n) for n in (1, 2, 3)]
    cursor.executemany(
        f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)",
        [(i, v) for i, v in enumerate(values, start=1)],
    )
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    assert rows == [(1, values[0]), (2, values[1]), (3, values[2])]
    assert all(type(r[1]) is uuid.UUID for r in rows)


# ---------------------------------------------------------------------------
# uniqueidentifier comparisons
# ---------------------------------------------------------------------------


def test_uuid_comparison_matches_uuid_and_string_forms(cursor, guid_table):
    value = uuid.UUID("ABCDEF01-2345-6789-ABCD-EF0123456789")
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value,))
    cursor.execute(
        f"INSERT INTO {guid_table} (id, g) VALUES (2, ?)", (uuid.UUID(int=7),)
    )

    by_uuid = _all(cursor, f"SELECT id FROM {guid_table} WHERE g = ?", (value,))
    by_upper = _all(
        cursor,
        f"SELECT id FROM {guid_table} WHERE g = ?",
        ("ABCDEF01-2345-6789-ABCD-EF0123456789",),
    )
    by_lower = _all(
        cursor,
        f"SELECT id FROM {guid_table} WHERE g = ?",
        ("abcdef01-2345-6789-abcd-ef0123456789",),
    )
    assert by_uuid == [(1,)]
    assert by_upper == [(1,)]
    assert by_lower == [(1,)]


def test_uuid_comparison_with_unknown_value_returns_no_rows(cursor, guid_table):
    cursor.execute(
        f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (uuid.UUID(int=1),)
    )
    rows = _all(cursor, f"SELECT id FROM {guid_table} WHERE g = ?", (uuid.UUID(int=2),))
    assert rows == []


def test_uuid_in_list_matches_each_bound_value(cursor, guid_table):
    for n in range(1, 5):
        cursor.execute(
            f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", (n, uuid.UUID(int=n))
        )
    rows = _all(
        cursor,
        f"SELECT id FROM {guid_table} WHERE g IN (?, ?) ORDER BY id",
        (uuid.UUID(int=2), uuid.UUID(int=4)),
    )
    assert rows == [(2,), (4,)]


def test_invalid_guid_string_raises_and_stores_nothing(cursor, conn_str, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int NOT NULL PRIMARY KEY, g uniqueidentifier NULL)"
    )
    cursor.connection.commit()
    with pytest.raises(mssql_python.DatabaseError, match="uniqueidentifier"):
        cursor.execute(
            f"INSERT INTO dbo.{table_name} (id, g) VALUES (1, ?)", ("not-a-guid",)
        )
    # The failed statement may roll back the open transaction, so count from a fresh connection.
    fresh = connect(conn_str)
    try:
        assert _one(fresh.cursor(), f"SELECT COUNT(*) FROM dbo.{table_name}") == (0,)
    finally:
        fresh.close()


@pytest.mark.upstream_bug(
    reason=(
        "Invalid uniqueidentifier conversion is a data error (SQLSTATE 22018, DataError "
        "per PEP 249); upstream surfaces it as ProgrammingError"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_invalid_guid_string_raises_dataerror(cursor, guid_table):
    with pytest.raises(mssql_python.DataError):
        cursor.execute(
            f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", ("not-a-guid",)
        )


# ---------------------------------------------------------------------------
# NEWID()
# ---------------------------------------------------------------------------


def test_newid_select_returns_uuid_objects(cursor):
    first = _one(cursor, "SELECT NEWID()")[0]
    second = _one(cursor, "SELECT NEWID()")[0]
    assert type(first) is uuid.UUID
    assert type(second) is uuid.UUID
    assert first != second


def test_newid_default_populates_each_inserted_row(cursor, guid_table):
    cursor.execute(f"INSERT INTO {guid_table} (id) VALUES (1)")
    cursor.execute(f"INSERT INTO {guid_table} (id) VALUES (2)")
    rows = _all(cursor, f"SELECT id, g2 FROM {guid_table} ORDER BY id")
    assert [r[0] for r in rows] == [1, 2]
    assert all(type(r[1]) is uuid.UUID for r in rows)
    assert rows[0][1] != rows[1][1]


# ---------------------------------------------------------------------------
# Permanent table: committed binary and GUID values survive a new connection
# ---------------------------------------------------------------------------


def test_committed_binary_and_guid_visible_from_new_connection(
    conn, conn_str, table_name
):
    value = uuid.UUID("0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0")
    payload = b"\x00\x01\x00\xfe"
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int NOT NULL PRIMARY KEY, v varbinary(max) NULL, g uniqueidentifier NULL)"
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (id, v, g) VALUES (1, ?, ?)", (payload, value)
    )
    conn.commit()

    fresh = connect(conn_str)
    try:
        got = _one(fresh.cursor(), f"SELECT v, g FROM dbo.{table_name} WHERE id = 1")
    finally:
        fresh.close()
    assert got == (payload, value)
    assert type(got[0]) is bytes
    assert type(got[1]) is uuid.UUID
