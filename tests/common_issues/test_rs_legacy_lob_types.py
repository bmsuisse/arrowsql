"""Legacy text / ntext / image columns: fetch, parameter binding, NULLs, large values,
TEXTSIZE behaviour, and the error classes raised for illegal operations on them.

Sources (research for the cases below):
- https://github.com/mkleehammer/pyodbc/issues/1058  "The text, ntext, and image data types
  cannot be compared or sorted" (error 306 when reading/sorting TEXT columns)
- https://github.com/mkleehammer/pyodbc/issues/130   rows silently missing with large text
  columns (right truncation 01004 on SQLGetData)
- https://github.com/mkleehammer/pyodbc/issues/835   long string params bound as
  *LONGVARCHAR -> text/ntext; setinputsizes size 0 switches to nvarchar(max)
- https://github.com/mkleehammer/pyodbc/issues/480   fast_executemany truncation on wide columns
- https://github.com/microsoft/mssql-rs/pull/700     empty IMAGE value must be b"", not NULL
  (zero-length IMAGE vs NULL is distinguished by the legacy LOB wire format)
- https://github.com/microsoft/mssql-rs/pull/771     legacy LOB RPC metadata / payload length /
  NULL framing for TEXT/NTEXT/IMAGE parameters
- https://github.com/tiberius-rs/tiberius/issues/56  TEXT metadata has an extra table-name
  section and different header layout than NTEXT/IMAGE; TEXT parsing broke for a long time
- https://github.com/tiberius-rs/tiberius/issues/352 bulk insert of String into NTEXT rejected
- https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/t-sql/statements/set-textsize-transact-sql.md
  SET TEXTSIZE: -1 unlimited, 0 resets to 4 KB; drivers set the connection default
- https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/t-sql/data-types/ntext-text-and-image-transact-sql.md
  text/ntext/image deprecated; compare/sort/= not allowed; use (n)varchar(max)/varbinary(max)
- https://github.com/typeorm/typeorm/issues/2770    text/ntext/image deprecation, CAST to nvarchar(max)

Upstream ODBC driver (.venv-upstream) is ground truth. Every test passes on it.
"""

import pytest
from mssql_python import DataError, ProgrammingError

BIG_TEXT_CHARS = 2_000_000
NTEXT_CHARS = 30_000
IMAGE_BYTES = 102_400


def _image_payload(n):
    """Deterministic bytes covering the full 0..255 range; any shift or truncation shows."""
    return bytes(i % 256 for i in range(n))


@pytest.fixture
def legacy_table(cursor):
    cursor.execute("CREATE TABLE #legacy (id int NOT NULL PRIMARY KEY, t text NULL, nt ntext NULL, im image NULL)")
    return "#legacy"


@pytest.fixture
def perm_legacy_table(cursor, table_name):
    # Permanent table: untyped None into IMAGE needs sp_describe_undeclared_parameters, which cannot
    # see #temp tables (upstream pins a ProgrammingError there, test_gh627_temp_table_null_varbinary_raises).
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL PRIMARY KEY, t text NULL, nt ntext NULL, im image NULL)")
    return f"dbo.{table_name}"


# --- Round trips and Python types -----------------------------------------------------


def test_text_roundtrip_returns_str(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, ?)", [" hello world  "])
    cursor.execute("SELECT t, DATALENGTH(t) FROM #legacy WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert type(value) is str
    assert value == " hello world  "  # trailing spaces survive (no CHAR-style padding)
    assert nbytes == len(" hello world  ")


def test_ntext_roundtrip_bmp_unicode(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, ?)", ["wörld 中文 Ω"])
    cursor.execute("SELECT nt, DATALENGTH(nt) FROM #legacy WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert type(value) is str
    assert value == "wörld 中文 Ω"
    # ntext is UTF-16LE: 2 bytes per BMP code unit
    assert nbytes == 2 * len("wörld 中文 Ω")


def test_ntext_roundtrip_supplementary_plane_surrogate_pair(cursor, legacy_table):
    # U+1F600 is outside the BMP -> a UTF-16 surrogate pair (4 bytes). Naive per-codepoint
    # 2-byte encoding or a 1-char-per-unit decoder corrupts this.
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, ?)", ["a😀b"])
    cursor.execute("SELECT nt, DATALENGTH(nt) FROM #legacy WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert value == "a😀b"
    assert nbytes == 2 + 4 + 2


def test_image_roundtrip_returns_bytes_full_byte_range(cursor, legacy_table):
    payload = _image_payload(256)
    cursor.execute("INSERT INTO #legacy (id, im) VALUES (1, ?)", [payload])
    cursor.execute("SELECT im FROM #legacy WHERE id = 1")
    (value,) = cursor.fetchone()
    assert type(value) is bytes
    assert value == payload


def test_image_roundtrip_bytearray_param_preserves_nul_bytes(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, im) VALUES (1, ?)", [bytearray(b"\xff\x00\x00\x7f")])
    cursor.execute("SELECT im, DATALENGTH(im) FROM #legacy WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert value == b"\xff\x00\x00\x7f"
    assert nbytes == 4


# --- Empty vs NULL ----------------------------------------------------------------------


def test_empty_text_and_ntext_are_empty_string_not_null(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t, nt) VALUES (1, ?, ?)", ["", ""])
    cursor.execute("SELECT t, nt, DATALENGTH(t), DATALENGTH(nt) FROM #legacy WHERE id = 1")
    t, nt, tb, ntb = cursor.fetchone()
    assert t == "" and nt == ""
    assert tb == 0 and ntb == 0


def test_empty_image_is_empty_bytes_not_null(cursor, legacy_table):
    # Regression shape from microsoft/mssql-rs PR #700: a zero-length IMAGE must not
    # decode to None; NULL and zero length are different on the legacy LOB wire format.
    cursor.execute("INSERT INTO #legacy (id, im) VALUES (1, ?)", [b""])
    cursor.execute("SELECT im, DATALENGTH(im) FROM #legacy WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert value is not None
    assert value == b""
    assert type(value) is bytes
    assert nbytes == 0


def test_null_in_all_legacy_columns_fetches_none(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t, nt, im) VALUES (1, NULL, NULL, NULL)")
    cursor.execute("SELECT t, nt, im FROM #legacy WHERE id = 1")
    assert tuple(cursor.fetchone()) == (None, None, None)


def test_null_parameters_bound_as_none(cursor, perm_legacy_table):
    cursor.execute(f"INSERT INTO {perm_legacy_table} (id, t, nt, im) VALUES (1, ?, ?, ?)", [None, None, None])
    cursor.execute(f"SELECT t, nt, im FROM {perm_legacy_table} WHERE id = 1")
    assert tuple(cursor.fetchone()) == (None, None, None)


def test_is_null_filter_on_legacy_columns(cursor, legacy_table):
    cursor.execute(
        "INSERT INTO #legacy (id, t, im) VALUES (1, NULL, NULL), (2, 'x', NULL), (3, NULL, 0x01)"
    )
    cursor.execute("SELECT id FROM #legacy WHERE t IS NULL ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [1, 3]
    cursor.execute("SELECT id FROM #legacy WHERE im IS NULL")
    assert [r[0] for r in cursor.fetchall()] == [1, 2]


def test_text_update_to_null_and_back(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, 'first')")
    cursor.execute("UPDATE #legacy SET t = ? WHERE id = 1", [None])
    cursor.execute("SELECT t FROM #legacy WHERE id = 1")
    assert tuple(cursor.fetchone()) == (None,)
    cursor.execute("UPDATE #legacy SET t = ? WHERE id = 1", ["second"])
    cursor.execute("SELECT t FROM #legacy WHERE id = 1")
    assert tuple(cursor.fetchone()) == ("second",)


# --- Large values and TEXTSIZE ----------------------------------------------------------


def test_textsize_default_is_unlimited_on_connection(cursor):
    # ODBC / native clients set TEXTSIZE -1 on connect so SELECT returns full LOBs.
    # A native client that leaves the server default (4 KB) silently truncates.
    cursor.execute("SELECT @@TEXTSIZE")
    assert tuple(cursor.fetchone()) == (-1,)


def test_large_text_2mb_roundtrip_is_not_truncated(cursor, legacy_table):
    value = "q" * BIG_TEXT_CHARS
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, ?)", [value])
    cursor.execute("SELECT t, DATALENGTH(t) FROM #legacy WHERE id = 1")
    fetched, nbytes = cursor.fetchone()
    assert type(fetched) is str
    assert len(fetched) == BIG_TEXT_CHARS
    assert nbytes == BIG_TEXT_CHARS
    assert fetched == value


def test_large_ntext_roundtrip_is_not_truncated(cursor, legacy_table):
    value = ("ü€" * (NTEXT_CHARS // 2))
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, ?)", [value])
    cursor.execute("SELECT nt, DATALENGTH(nt) FROM #legacy WHERE id = 1")
    fetched, nbytes = cursor.fetchone()
    assert fetched == value
    assert len(fetched) == NTEXT_CHARS
    assert nbytes == 2 * NTEXT_CHARS


def test_large_image_roundtrip_exact_bytes(cursor, legacy_table):
    payload = _image_payload(IMAGE_BYTES)
    cursor.execute("INSERT INTO #legacy (id, im) VALUES (1, ?)", [payload])
    cursor.execute("SELECT im, DATALENGTH(im) FROM #legacy WHERE id = 1")
    fetched, nbytes = cursor.fetchone()
    assert nbytes == IMAGE_BYTES
    assert fetched == payload


def test_set_textsize_limits_select_then_reset_restores_full_value(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, ?)", ["abcdefghij" * 100])
    cursor.execute("SET TEXTSIZE 5")
    cursor.execute("SELECT t FROM #legacy WHERE id = 1")
    assert tuple(cursor.fetchone()) == ("abcde",)
    cursor.execute("SET TEXTSIZE -1")
    cursor.execute("SELECT t FROM #legacy WHERE id = 1")
    assert tuple(cursor.fetchone()) == ("abcdefghij" * 100,)


def test_cast_ntext_to_nvarchar_max_returns_full_value(cursor, legacy_table):
    # The documented migration path; must carry the whole value, not a CAST-induced cap.
    value = "n" * NTEXT_CHARS
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, ?)", [value])
    cursor.execute("SELECT CAST(nt AS nvarchar(max)) FROM #legacy WHERE id = 1")
    (fetched,) = cursor.fetchone()
    assert fetched == value


def test_executemany_mixed_nulls_and_large_values(cursor, perm_legacy_table):
    rows = [
        (1, "a", b"\x01", "u" * 9000),
        (2, None, None, None),
        (3, "", b"", ""),
    ]
    cursor.executemany(f"INSERT INTO {perm_legacy_table} (id, t, im, nt) VALUES (?, ?, ?, ?)", rows)
    cursor.execute(f"SELECT id, t, im, nt FROM {perm_legacy_table} ORDER BY id")
    fetched = [tuple(r) for r in cursor.fetchall()]
    assert fetched == [
        (1, "a", b"\x01", "u" * 9000),
        (2, None, None, None),
        (3, "", b"", ""),
    ]


# --- Operations the server rejects on text/ntext/image ----------------------------------


def test_text_equality_comparison_raises_programming_error(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, 'hello')")
    with pytest.raises(ProgrammingError, match="text"):
        cursor.execute("SELECT id FROM #legacy WHERE t = 'hello'")


def test_order_by_text_raises_programming_error(cursor, legacy_table):
    # pyodbc #1058: "The text, ntext, and image data types cannot be compared or sorted"
    cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, 'a')")
    with pytest.raises(ProgrammingError, match="cannot be compared or sorted"):
        cursor.execute("SELECT id FROM #legacy ORDER BY t")


def test_ntext_like_predicate_is_allowed(cursor, legacy_table):
    # LIKE is the one comparison the server permits on these types.
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, N'wörld'), (2, N'hello')")
    cursor.execute("SELECT id FROM #legacy WHERE nt LIKE N'w%' ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]


def test_ntext_equals_nvarchar_param_raises_programming_error(cursor, legacy_table):
    cursor.execute("INSERT INTO #legacy (id, nt) VALUES (1, N'x')")
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT id FROM #legacy WHERE nt = ?", ["x"])


def test_int_param_bound_to_text_column_raises_data_error(cursor, legacy_table):
    # Implicit numeric->text conversion is refused by the server; a reimplementation that
    # silently stringifies would insert '42' and not raise.
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, ?)", [42])


def test_bytes_param_bound_to_text_column_raises_data_error(cursor, legacy_table):
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #legacy (id, t) VALUES (1, ?)", [b"abc"])


def test_non_ascii_str_param_bound_to_image_column_raises_data_error(cursor, legacy_table):
    # ASCII str goes over the wire as varchar and the server converts it to image; non-ASCII
    # str is sent as nvarchar, which has no implicit conversion to image.
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #legacy (id, im) VALUES (1, ?)", ["ö"])
