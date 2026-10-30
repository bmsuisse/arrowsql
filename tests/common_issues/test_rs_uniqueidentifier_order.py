"""uniqueidentifier ordering, parameter binding and byte layout for mssql_python.

Research sources (issue trackers and docs consulted for real-world failure modes):
- https://github.com/microsoft/mssql-python/issues/241  fetchone vs fetchall
  inconsistency when coercing uniqueidentifier values (raw little-endian bytes)
- https://github.com/microsoft/mssql-python/issues/447  native_uuid option; users
  migrating from pyodbc depend on str GUIDs and break on uuid.UUID return values
- https://github.com/microsoft/mssql-python/issues/850  SQL Server GUID sort order
  fragments indexes for application-generated UUIDv7 values
- https://github.com/adbc-drivers/mssql/issues/15 and
  https://github.com/adbc-drivers/mssql/issues/18  mixed-endian byte order when
  UNIQUEIDENTIFIER is exposed as raw bytes (first 4/2/2 fields byte-swapped)
- https://github.com/mkleehammer/pyodbc/commit/2ad7a9ced7c9c33232c173668c0830a484cc92f3
  pyodbc accepts uuid.UUID params; pyodbc.native_uuid controls the return type
- https://github.com/michiya/django-pyodbc-azure/issues/75  "Conversion failed when
  converting from a character string to uniqueidentifier" for malformed strings
- https://learn.microsoft.com/en-us/sql/t-sql/functions/newsequentialid-transact-sql
  NEWSEQUENTIALID only valid as a DEFAULT on a uniqueidentifier column
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql
  comparison semantics; byte order is not the bit pattern
- https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/comparing-guid-and-uniqueidentifier-values
  SqlGuid comparison: the last six bytes are the most significant
- https://techcommunity.microsoft.com/blog/azuredbsupport/lesson-learned-497understanding-the-ordering-of-uniqueidentifier-in-sql-server/4156066
  worked T-SQL examples of uniqueidentifier ordering

SQL Server compares uniqueidentifier values over the stored (mixed-endian) bytes in
the order 10..15, 8..9, 6..7, 4..5, 3..0. Python's uuid.UUID orders by its big-endian
integer value, so the two orders differ in general.
"""

import random
import uuid

import pytest

SQL_COMPARE_ORDER = (10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 3, 2, 1, 0)


def _sql_sort_key(u: uuid.UUID) -> bytes:
    """Sort key matching SQL Server's uniqueidentifier ordering."""
    stored = u.bytes_le
    return bytes(stored[i] for i in SQL_COMPARE_ORDER)


def _all(cursor, sql, params=()):
    cursor.execute(sql, params)
    return [tuple(row) for row in cursor.fetchall()]


def _one(cursor, sql, params=()):
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return tuple(row) if row is not None else None


@pytest.fixture
def guid_table(cursor):
    cursor.execute(
        "CREATE TABLE #guid ("
        " id int NOT NULL PRIMARY KEY,"
        " g uniqueidentifier NULL,"
        " g2 uniqueidentifier NOT NULL DEFAULT NEWSEQUENTIALID())"
    )
    return "#guid"


# ---------------------------------------------------------------------------
# Parameter binding and return types
# ---------------------------------------------------------------------------


def test_uuid_param_roundtrip_returns_uuid_type(cursor, guid_table):
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value,))
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert type(got) is uuid.UUID
    assert got == value


def test_str_param_is_stored_as_uuid_and_returned_as_uuid(cursor, guid_table):
    cursor.execute(
        f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)",
        ("12345678-9abc-def0-1122-334455667788",),
    )
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert type(got) is uuid.UUID
    assert str(got) == "12345678-9abc-def0-1122-334455667788"


def test_uppercase_and_braced_str_params_are_accepted(cursor, guid_table):
    cursor.execute(
        f"INSERT INTO {guid_table} (id, g) VALUES (1, ?), (2, ?)",
        ("12345678-9ABC-DEF0-1122-334455667788", "{12345678-9abc-def0-1122-334455667788}"),
    )
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    expected = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    assert rows == [(1, expected), (2, expected)]
    assert all(type(r[1]) is uuid.UUID for r in rows)


def test_null_uuid_and_null_str_params_store_sql_null(cursor, guid_table):
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?), (2, ?)", (None, None))
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    assert rows == [(1, None), (2, None)]


def test_fetchone_and_fetchall_return_the_same_uuid_objects(cursor, guid_table):
    # Regression guard for microsoft/mssql-python#241 (fetchone vs fetchall divergence).
    values = [uuid.UUID(int=i * 0x0101_0101) for i in range(1, 6)]
    for i, v in enumerate(values, start=1):
        cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", (i, v))
    cursor.execute(f"SELECT g FROM {guid_table} ORDER BY id")
    via_fetchall = [r[0] for r in cursor.fetchall()]
    cursor.execute(f"SELECT g FROM {guid_table} ORDER BY id")
    via_fetchone = [cursor.fetchone()[0] for _ in range(len(values))]
    assert via_fetchall == values
    assert via_fetchone == values
    assert all(type(x) is uuid.UUID for x in via_fetchall + via_fetchone)


def test_uuid_and_str_params_match_in_where_clause(cursor, guid_table):
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?), (2, NULL)", (value,))
    by_uuid = _all(cursor, f"SELECT id FROM {guid_table} WHERE g = ?", (value,))
    by_str = _all(cursor, f"SELECT id FROM {guid_table} WHERE g = ?", (str(value).upper(),))
    assert by_uuid == [(1,)]
    assert by_str == [(1,)]


def test_executemany_all_uuid_params(cursor, guid_table):
    a = uuid.UUID("00000000-0000-0000-0000-000000000001")
    b = uuid.UUID("00000000-0000-0000-0000-000000000002")
    cursor.executemany(f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", [(1, a), (2, b)])
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    assert rows == [(1, a), (2, b)]


def test_executemany_all_str_params(cursor, guid_table):
    cursor.executemany(
        f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)",
        [(1, "00000000-0000-0000-0000-000000000001"), (2, "00000000-0000-0000-0000-000000000002")],
    )
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    assert rows == [
        (1, uuid.UUID("00000000-0000-0000-0000-000000000001")),
        (2, uuid.UUID("00000000-0000-0000-0000-000000000002")),
    ]


@pytest.mark.upstream_bug(
    reason=(
        "executemany with a uuid.UUID and a str bound to the same uniqueidentifier "
        "column raises RuntimeError 'Parameter's object type does not match parameter's "
        "C type ... SQL_C_GUID'; single execute accepts both, so the batch should too"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_mixed_uuid_and_str_params(cursor, guid_table):
    a = uuid.UUID("00000000-0000-0000-0000-000000000001")
    b = "00000000-0000-0000-0000-000000000002"
    cursor.executemany(f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", [(1, a), (2, b)])
    rows = _all(cursor, f"SELECT id, g FROM {guid_table} ORDER BY id")
    assert rows == [(1, a), (2, uuid.UUID(b))]


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


def test_malformed_guid_string_raises_programming_error(cursor, guid_table):
    from mssql_python.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError, match="Conversion failed"):
        cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", ("not-a-guid",))


def test_uuid_hex_form_without_hyphens_is_rejected(cursor, guid_table):
    # uuid.UUID.hex is 32 chars with no hyphens; SQL Server does not parse it.
    # Seen as "Conversion failed ... (8169)" reports in pyodbc/django-pyodbc-azure
    # issues. The driver should surface the same error class rather than guess.
    from mssql_python.exceptions import ProgrammingError

    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    with pytest.raises(ProgrammingError, match="Conversion failed"):
        cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value.hex,))


# ---------------------------------------------------------------------------
# Sort order versus Python uuid ordering
# ---------------------------------------------------------------------------


def test_order_by_guid_follows_sql_byte_order_not_python_order(cursor, guid_table):
    # Python order: 00..01 < 0000000a-.. < ffff.. ; SQL order: 0000000a-.. < 00..01 < ffff..
    low = uuid.UUID("00000000-0000-0000-0000-000000000001")
    mid = uuid.UUID("0000000a-0000-0000-0000-000000000000")
    high = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    assert sorted([low, mid, high]) == [low, mid, high]  # Python: low first

    cursor.execute(
        f"INSERT INTO {guid_table} (id, g) VALUES (1, ?), (2, ?), (3, ?)", (low, mid, high)
    )
    got = _all(cursor, f"SELECT id FROM {guid_table} WHERE g IS NOT NULL ORDER BY g")
    assert got == [(2,), (1,), (3,)]


def test_order_by_guid_matches_sql_compare_key_on_random_values(cursor, guid_table):
    rng = random.Random(20240601)
    values = [uuid.UUID(bytes=bytes(rng.getrandbits(8) for _ in range(16))) for _ in range(60)]
    for i, v in enumerate(values, start=1):
        cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", (i, v))
    got = [r[0] for r in _all(cursor, f"SELECT g FROM {guid_table} WHERE g IS NOT NULL ORDER BY g")]
    expected = sorted(values, key=_sql_sort_key)
    assert got == expected


def test_sql_order_differs_from_python_order_for_random_values(cursor, guid_table):
    # Guards the claim in the module docstring: a Python-side sort is not a
    # substitute for ORDER BY on the server.
    rng = random.Random(7)
    values = [uuid.UUID(bytes=bytes(rng.getrandbits(8) for _ in range(16))) for _ in range(30)]
    for i, v in enumerate(values, start=1):
        cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (?, ?)", (i, v))
    sql_order = [r[0] for r in _all(cursor, f"SELECT g FROM {guid_table} WHERE g IS NOT NULL ORDER BY g")]
    assert sql_order != sorted(values)


# ---------------------------------------------------------------------------
# Raw byte layout (mixed-endian)
# ---------------------------------------------------------------------------


def test_cast_guid_to_binary16_is_mixed_endian_layout(cursor):
    # SQL Server stores the first three fields little-endian, so the 16-byte
    # binary form equals uuid.bytes_le, NOT uuid.bytes.
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    got = _one(cursor, "SELECT CAST(CAST(? AS uniqueidentifier) AS binary(16))", (str(value),))[0]
    assert type(got) is bytes
    assert got == value.bytes_le
    assert got != value.bytes
    assert got.hex() == "78563412bc9af0de1122334455667788"


def test_binary16_cast_back_to_guid_uses_mixed_endian_layout(cursor):
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    got = _one(cursor, "SELECT CAST(? AS uniqueidentifier)", (value.bytes_le,))[0]
    assert type(got) is uuid.UUID
    assert got == value


def test_binary16_in_big_endian_form_gives_a_different_guid(cursor):
    # Callers that pass uuid.bytes (RFC 4122 big-endian) get a different GUID.
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    got = _one(cursor, "SELECT CAST(? AS uniqueidentifier)", (value.bytes,))[0]
    assert got == uuid.UUID(bytes_le=value.bytes)
    assert got != value


def test_guid_column_as_varbinary_roundtrips_16_bytes(cursor, guid_table):
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value,))
    raw, length = _one(cursor, f"SELECT CAST(g AS varbinary(16)), DATALENGTH(g) FROM {guid_table} WHERE id = 1")
    assert type(raw) is bytes
    assert raw == value.bytes_le
    assert length == 16


# ---------------------------------------------------------------------------
# Server-side generation: NEWSEQUENTIALID / NEWID
# ---------------------------------------------------------------------------


def test_newsequentialid_default_is_increasing_within_session(cursor):
    cursor.execute(
        "CREATE TABLE #seq (id int identity(1,1) PRIMARY KEY,"
        " g uniqueidentifier NOT NULL DEFAULT NEWSEQUENTIALID())"
    )
    for _ in range(6):
        cursor.execute("INSERT INTO #seq DEFAULT VALUES")
    by_insert = [r[0] for r in _all(cursor, "SELECT g FROM #seq ORDER BY id")]
    by_sort = [r[0] for r in _all(cursor, "SELECT g FROM #seq ORDER BY g")]
    assert all(type(g) is uuid.UUID for g in by_insert)
    assert len(set(by_insert)) == 6
    assert by_sort == by_insert


def test_newid_default_produces_distinct_uuid_values(cursor):
    cursor.execute(
        "CREATE TABLE #rid (id int NOT NULL PRIMARY KEY,"
        " g uniqueidentifier NOT NULL DEFAULT NEWID())"
    )
    cursor.execute("INSERT INTO #rid (id) VALUES (1), (2), (3)")
    values = [r[0] for r in _all(cursor, "SELECT g FROM #rid ORDER BY id")]
    assert all(type(v) is uuid.UUID for v in values)
    assert all(v.version == 4 for v in values)
    assert len(set(values)) == 3


def test_newsequentialid_outside_default_is_a_syntax_error(cursor):
    from mssql_python.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError, match="DEFAULT"):
        cursor.execute("SELECT NEWSEQUENTIALID()")


# ---------------------------------------------------------------------------
# Output shape and metadata
# ---------------------------------------------------------------------------


def test_cast_guid_to_nvarchar_gives_uppercase_hyphenated_text(cursor):
    value = uuid.UUID("12345678-9abc-def0-1122-334455667788")
    got = _one(cursor, "SELECT CAST(CAST(? AS uniqueidentifier) AS nvarchar(40))", (str(value),))[0]
    assert type(got) is str
    assert got == "12345678-9ABC-DEF0-1122-334455667788"


def test_uuid_v7_value_roundtrips_exactly(cursor, guid_table):
    # uuid.uuid7 is Python 3.14+; build the v7 layout by hand to stay portable.
    raw = bytearray(16)
    raw[0:6] = (0x018A_2B3C_4D5E).to_bytes(6, "big")
    raw[6] = 0x70 | 0x0A
    raw[7] = 0x12
    raw[8] = 0x80 | 0x34
    raw[9:16] = bytes.fromhex("56789abcdef012")
    value = uuid.UUID(bytes=bytes(raw))
    assert value.version == 7
    cursor.execute(f"INSERT INTO {guid_table} (id, g) VALUES (1, ?)", (value,))
    got = _one(cursor, f"SELECT g FROM {guid_table} WHERE id = 1")[0]
    assert type(got) is uuid.UUID
    assert got == value
    assert got.version == 7
