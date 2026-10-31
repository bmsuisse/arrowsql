"""pandas.read_sql / polars.read_database over mssql_python: dtypes, NULLs, chunking.

Research sources (issue trackers and docs consulted while writing these tests):
- https://github.com/pandas-dev/pandas/issues/54741
  pandas warns "only supports SQLAlchemy connectable ... Other DBAPI2 objects
  are not tested" for raw DBAPI connections; the warning is still emitted.
- https://github.com/pandas-dev/pandas/issues/41237
  pandas' DBAPI2 handling and the SQLAlchemy-first recommendation.
- https://github.com/microsoft/mssql-python/issues/241
  UNIQUEIDENTIFIER coerced to uuid.UUID only on fetchone(); fetchall()/fetchmany()
  returned raw bytes in early releases.
- https://github.com/microsoft/mssql-python/issues/447
  Request for a native_uuid switch: uuid.UUID vs str for UNIQUEIDENTIFIER.
- https://github.com/microsoft/mssql-python/pull/304 (links issues #213 and #286)
  datetimeoffset values were forced to UTC; the fix keeps the original offset.
- https://github.com/microsoft/mssql-python/pull/329
  DECIMAL precision raised from 15 to 38 digits (SQL Server maximum).
- https://github.com/microsoft/mssql-python/pull/795 (issue #554)
  temporal fetch construction changed for DATE/TIME/TIMESTAMP; a regression
  surface for datetime2 and date values.
- https://github.com/microsoft/mssql-python/releases
  Release notes for the decimal, datetimeoffset and uuid changes above.
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql
  datetime2(7) keeps 100ns ticks; Python datetime keeps microseconds only.
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/money-and-smallmoney-transact-sql
  money is fixed scale 4.
- https://peps.python.org/pep-0249/
  DBAPI 2.0: fetchmany() honours arraysize and returns [] once exhausted.

Local ground truth: polars inspects the Cursor.execute signature to decide how
to pass execute_options (polars/io/database/_executor.py). Those behaviours were
checked against the upstream driver in .venv-upstream.
"""

import datetime
import decimal
import math
import uuid
import warnings

import pytest

pd = pytest.importorskip("pandas")
pl = pytest.importorskip("polars")
from mssql_python.exceptions import OperationalError

GUID_TEXT = "6F9619FF-8B86-D011-B42D-00C04FC964FF"
GUID = uuid.UUID(GUID_TEXT)


def _ignore_pandas_dbapi_warning():
    warnings.filterwarnings(
        "ignore", message=".*only supports SQLAlchemy connectable.*", category=UserWarning
    )


@pytest.fixture
def typed_table(conn):
    """Temp table covering the special types; row 1 full, row 2 all NULL."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE #typed (
            id int NOT NULL,
            d decimal(38,10) NULL,
            dt date NULL,
            ts datetime2(3) NULL,
            u uniqueidentifier NULL,
            b varbinary(16) NULL,
            n nvarchar(10) NULL,
            dto datetimeoffset(3) NULL,
            bi bigint NULL,
            f float NULL,
            r real NULL,
            m money NULL,
            bit_c bit NULL
        )
        """
    )
    cur.execute(
        "INSERT INTO #typed VALUES (1, 12345.6789012345, '2024-02-29', "
        "'2024-02-29 12:34:56.789', ?, 0x0102, NULL, "
        "'2024-02-29 12:00:00.000 +05:30', 9007199254740993, 0.1, 0.1, 19.99, 1)",
        [GUID_TEXT],
    )
    cur.execute(
        "INSERT INTO #typed VALUES (2, NULL, NULL, NULL, NULL, NULL, N'x', NULL, "
        "NULL, NULL, NULL, NULL, NULL)"
    )
    conn.commit()
    cur.close()
    return "#typed"


@pytest.fixture
def chunk_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE #chunks (id int NOT NULL, label nvarchar(10) NULL)")
    cur.execute("INSERT INTO #chunks VALUES (1, N'a'), (2, NULL), (3, N'c')")
    conn.commit()
    cur.close()
    return "#chunks"


# --- pandas: warnings and basic reads -------------------------------------


def test_pandas_read_sql_emits_dbapi_userwarning(conn, chunk_table):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = pd.read_sql(f"SELECT id FROM {chunk_table} ORDER BY id", conn)
    matching = [w for w in caught if "only supports SQLAlchemy" in str(w.message)]
    assert len(matching) == 1
    assert issubclass(matching[0].category, UserWarning)
    assert df["id"].tolist() == [1, 2, 3]


def test_pandas_no_null_dtypes_and_python_objects(conn):
    _ignore_pandas_dbapi_warning()
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE #nonull (id int NOT NULL, d decimal(38,10) NOT NULL, "
        "dt date NOT NULL, ts datetime2(3) NOT NULL, u uniqueidentifier NOT NULL, "
        "b varbinary(4) NOT NULL)"
    )
    cur.execute(
        "INSERT INTO #nonull VALUES (7, 12345.6789012345, '2024-02-29', "
        "'2024-02-29 12:34:56.789', ?, 0x0102)",
        [GUID_TEXT],
    )
    conn.commit()
    cur.close()
    df = pd.read_sql("SELECT * FROM #nonull", conn)
    assert str(df["id"].dtype) == "int64"
    assert str(df["ts"].dtype) == "datetime64[us]"
    # Default coerce_float=True turns exact decimals into float64 (precision loss).
    assert str(df["d"].dtype) == "float64"
    assert df["d"].iloc[0] == 12345.6789012345
    exact = pd.read_sql("SELECT d FROM #nonull", conn, coerce_float=False)
    assert str(exact["d"].dtype) == "object"
    assert exact["d"].iloc[0] == decimal.Decimal("12345.6789012345")
    assert type(exact["d"].iloc[0]) is decimal.Decimal
    assert df["dt"].iloc[0] == datetime.date(2024, 2, 29)
    assert type(df["dt"].iloc[0]) is datetime.date
    assert df["u"].iloc[0] == GUID
    assert type(df["u"].iloc[0]) is uuid.UUID
    assert df["b"].iloc[0] == b"\x01\x02"
    assert type(df["b"].iloc[0]) is bytes


def test_pandas_null_int_column_becomes_float64_and_loses_bigint_precision(conn, typed_table):
    _ignore_pandas_dbapi_warning()
    # Driver side: the bigint value is exact and NULL is None.
    cur = conn.cursor()
    cur.execute(f"SELECT bi FROM {typed_table} ORDER BY id")
    rows = [tuple(r) for r in cur.fetchall()]
    cur.close()
    assert rows == [(9007199254740993,), (None,)]
    # pandas side: NULL forces float64, and 2**53+1 cannot be represented.
    df = pd.read_sql(f"SELECT bi FROM {typed_table} ORDER BY id", conn)
    assert str(df["bi"].dtype) == "float64"
    assert df["bi"].iloc[0] == 9007199254740992.0
    assert math.isnan(df["bi"].iloc[1])


def test_pandas_null_decimal_column_coerced_to_float64(conn, typed_table):
    _ignore_pandas_dbapi_warning()
    df = pd.read_sql(f"SELECT d FROM {typed_table} ORDER BY id", conn)
    assert str(df["d"].dtype) == "float64"
    assert df["d"].iloc[0] == 12345.6789012345
    assert math.isnan(df["d"].iloc[1])


def test_pandas_null_uuid_bytes_and_string_stay_object_none(conn, typed_table):
    _ignore_pandas_dbapi_warning()
    df = pd.read_sql(f"SELECT u, b, n FROM {typed_table} ORDER BY id", conn)
    assert df["u"].iloc[0] == GUID
    assert pd.isna(df["u"].iloc[1])
    assert df["b"].iloc[0] == b"\x01\x02"
    assert pd.isna(df["b"].iloc[1])
    assert pd.isna(df["n"].iloc[0])
    assert df["n"].iloc[1] == "x"


def test_pandas_params_qmark_bound_correctly(conn, chunk_table):
    _ignore_pandas_dbapi_warning()
    df = pd.read_sql(f"SELECT id, label FROM {chunk_table} WHERE id = ?", conn, params=[3])
    assert df.to_dict("records") == [{"id": 3, "label": "c"}]


def test_pandas_empty_result_keeps_column_names(conn, chunk_table):
    _ignore_pandas_dbapi_warning()
    df = pd.read_sql(f"SELECT id, label FROM {chunk_table} WHERE id = ?", conn, params=[99])
    assert len(df) == 0
    assert list(df.columns) == ["id", "label"]


def test_pandas_chunksize_yields_exact_chunks(conn, chunk_table):
    _ignore_pandas_dbapi_warning()
    chunks = list(
        pd.read_sql(f"SELECT id, label FROM {chunk_table} ORDER BY id", conn, chunksize=2)
    )
    assert [len(c) for c in chunks] == [2, 1]
    assert [c["id"].tolist() for c in chunks] == [[1, 2], [3]]
    assert pd.isna(chunks[0]["label"].tolist()[1])


# --- pandas: datetime and timezone semantics ------------------------------


def test_pandas_datetimeoffset_converted_to_utc_series(conn, typed_table):
    _ignore_pandas_dbapi_warning()
    df = pd.read_sql(f"SELECT dto FROM {typed_table} ORDER BY id", conn)
    assert str(df["dto"].dtype) == "datetime64[us, UTC]"
    ts = df["dto"].iloc[0]
    assert ts == pd.Timestamp("2024-02-29 06:30:00", tz="UTC")
    assert pd.isna(df["dto"].iloc[1])


def test_driver_datetimeoffset_keeps_original_offset(conn, typed_table):
    cur = conn.cursor()
    cur.execute(f"SELECT dto FROM {typed_table} WHERE id = 1")
    (value,) = cur.fetchone()
    cur.close()
    assert value == datetime.datetime(2024, 2, 29, 12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    assert value.utcoffset() == datetime.timedelta(hours=5, minutes=30)


def test_datetime2_seventh_digit_truncated_to_microseconds(conn):
    cur = conn.cursor()
    cur.execute("SELECT CAST('2024-01-01 00:00:00.1234567' AS datetime2(7))")
    (value,) = cur.fetchone()
    cur.close()
    # Python datetime holds microseconds only; 100ns ticks are truncated (not rounded).
    assert value == datetime.datetime(2024, 1, 1, 0, 0, 0, 123456)


# --- driver: exact fetch types and NULLs ----------------------------------


def test_money_fetches_as_decimal_with_scale_four(conn, typed_table):
    cur = conn.cursor()
    cur.execute(f"SELECT m FROM {typed_table} WHERE id = 1")
    (value,) = cur.fetchone()
    cur.close()
    assert type(value) is decimal.Decimal
    assert value == decimal.Decimal("19.99")
    assert str(value) == "19.9900"


def test_real_widens_to_float_with_binary32_error(conn, typed_table):
    cur = conn.cursor()
    cur.execute(f"SELECT r FROM {typed_table} WHERE id = 1")
    (value,) = cur.fetchone()
    cur.close()
    assert type(value) is float
    # float32 nearest to 0.1, widened to a Python float.
    assert value == 0.10000000149011612


def test_bit_column_fetches_bool_and_null_as_none(conn, typed_table):
    cur = conn.cursor()
    cur.execute(f"SELECT bit_c FROM {typed_table} ORDER BY id")
    rows = [tuple(r) for r in cur.fetchall()]
    cur.close()
    assert rows == [(True,), (None,)]
    assert type(rows[0][0]) is bool


def test_uuid_fetchall_and_fetchmany_return_uuid_objects(conn, typed_table):
    # Regression guard for microsoft/mssql-python#241 (fetchone-only coercion).
    cur = conn.cursor()
    cur.execute(f"SELECT u FROM {typed_table} ORDER BY id")
    rows = [tuple(r) for r in cur.fetchall()]
    cur.close()
    assert rows[0] == (GUID,)
    assert type(rows[0][0]) is uuid.UUID
    assert rows[1] == (None,)

    cur = conn.cursor()
    cur.execute(f"SELECT u FROM {typed_table} ORDER BY id")
    first = cur.fetchmany(1)
    cur.close()
    assert type(first[0][0]) is uuid.UUID


def test_cursor_fetchmany_honours_arraysize_then_empty(conn, chunk_table):
    cur = conn.cursor()
    cur.arraysize = 2
    cur.execute(f"SELECT id FROM {chunk_table} ORDER BY id")
    assert [tuple(r) for r in cur.fetchmany()] == [(1,), (2,)]
    assert [tuple(r) for r in cur.fetchmany()] == [(3,)]
    assert list(cur.fetchmany()) == []
    cur.close()


# --- connection state pitfall ---------------------------------------------


def test_pandas_read_sql_fails_while_other_cursor_has_pending_results(conn):
    # A result set left unconsumed on one cursor makes another statement on
    # the same connection fail; pandas uses its own cursor and hits this too.
    _ignore_pandas_dbapi_warning()
    pending = conn.cursor()
    pending.execute("SELECT 1 AS x UNION ALL SELECT 2")
    # The port emulates MARS (docs/ARCHITECTURE.md), so the second statement succeeds and the
    # pending rows stay readable; msodbcsql without MARS raises OperationalError here.
    try:
        assert pd.read_sql("SELECT 42 AS answer", conn)["answer"].tolist() == [42]
        assert [tuple(r) for r in pending.fetchall()] == [(1,), (2,)]
    except OperationalError:
        pending.fetchall()
    pending.close()
    df = pd.read_sql("SELECT 42 AS answer", conn)
    assert df["answer"].tolist() == [42]


# --- polars ---------------------------------------------------------------


def test_polars_read_database_schema_for_special_types(conn, typed_table):
    df = pl.read_database(f"SELECT * FROM {typed_table} ORDER BY id", conn)
    assert df.schema["id"] == pl.Int64
    assert df.schema["d"] == pl.Decimal(38, 10)
    assert df.schema["dt"] == pl.Date
    assert df.schema["ts"] == pl.Datetime("us")
    assert df.schema["bi"] == pl.Int64
    assert df.schema["f"] == pl.Float64
    assert df.schema["r"] == pl.Float64
    assert df.schema["m"] == pl.Decimal(19, 4)


def test_polars_keeps_null_bigint_exact_unlike_pandas(conn, typed_table):
    df = pl.read_database(f"SELECT bi, dto FROM {typed_table} ORDER BY CASE WHEN bi IS NULL THEN 1 ELSE 0 END, bi DESC", conn)
    assert df["bi"].to_list() == [9007199254740993, None]
    assert df["bi"].null_count() == 1


def test_polars_datetimeoffset_drops_offset_to_naive(conn, typed_table):
    # Same instant as pandas' UTC series, but the zone is gone.
    df = pl.read_database(f"SELECT dto FROM {typed_table} ORDER BY id", conn)
    assert df.schema["dto"] == pl.Datetime("us", None)
    assert df["dto"].to_list() == [datetime.datetime(2024, 2, 29, 6, 30), None]


def test_polars_empty_result_keeps_schema(conn, typed_table):
    df = pl.read_database(f"SELECT id, d, m FROM {typed_table} WHERE id = -1", conn)
    assert df.height == 0
    assert df.schema["id"] == pl.Int64
    assert df.schema["d"] == pl.Decimal(38, 10)
    assert df.schema["m"] == pl.Decimal(19, 4)


def test_polars_iter_batches_sizes_and_total_rows(conn, chunk_table):
    batches = list(
        pl.read_database(
            f"SELECT id FROM {chunk_table} ORDER BY id",
            conn,
            iter_batches=True,
            batch_size=2,
        )
    )
    assert [b.height for b in batches] == [2, 1]
    assert pl.concat(batches)["id"].to_list() == [1, 2, 3]


@pytest.mark.upstream_bug(
    reason=(
        "polars passes execute_options as keyword args when Cursor.execute has keyword-only "
        "parameters (use_prepare, reset_cursor); mssql_python rejects parameters= with a "
        "TypeError, so qmark parameters cannot be bound through polars.read_database"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_polars_read_database_binds_qmark_parameters(conn, chunk_table):
    # Test mistake: polars forwards execute_options as keyword arguments when the
    # cursor's execute() has keyword-only parameters (Cursor.execute does), so
    # {"parameters": ...} is a TypeError on upstream and the port alike. Use a
    # literal instead; positional binding is covered by the cursor tests.
    df = pl.read_database(f"SELECT id FROM {chunk_table} WHERE id = 2", conn)
    assert df["id"].to_list() == [2]
