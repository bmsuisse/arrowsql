"""sql_variant (every base type), hierarchyid, geometry, geography, json, vector,
table-valued and cursor types, and setinputsizes -- across fetch / bind /
executemany / Arrow / bulk copy.
"""

import datetime as dt
import uuid
from decimal import Decimal as D

import pytest

import mssql_python
from mssql_python import DatabaseError, ProgrammingError, connect
from mssql_python import ddbc_bindings

from rs_dtypes import same

pa = pytest.importorskip("pyarrow")
RUST = getattr(ddbc_bindings, "__rust_port__", False)
UTC = dt.timezone.utc
G = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


@pytest.fixture
def ac(conn_str):
    c = connect(conn_str, autocommit=True)
    yield c
    c.close()


@pytest.fixture
def cur(ac):
    c = ac.cursor()
    yield c
    c.close()


def table_of(cur, ddl):
    name = f"##rs_ex_{uuid.uuid4().hex[:10]}"
    cur.execute(f"CREATE TABLE {name} {ddl}")
    return name


# --------------------------------------------------------------- sql_variant
# (T-SQL expression, BaseType, expected Python value)
VARIANTS = [
    ("CAST(42 AS int)", "int", 42),
    ("CAST(-9223372036854775808 AS bigint)", "bigint", -(2**63)),
    ("CAST(32767 AS smallint)", "smallint", 32767),
    ("CAST(255 AS tinyint)", "tinyint", 255),
    ("CAST(1 AS bit)", "bit", True),
    ("CAST(0 AS bit)", "bit", False),
    ("CAST(-12345.678 AS decimal(10,3))", "decimal", D("-12345.678")),
    ("CAST(12345678901234567890123456789012345678 AS decimal(38,0))", "decimal", D("12345678901234567890123456789012345678")),
    ("CAST(1.25 AS numeric(5,2))", "numeric", D("1.25")),
    ("CAST(922337203685477.5807 AS money)", "money", D("922337203685477.5807")),
    ("CAST(-214748.3648 AS smallmoney)", "smallmoney", D("-214748.3648")),
    ("CAST(1.5 AS float)", "float", 1.5),
    ("CAST(1.5 AS real)", "real", 1.5),
    ("CAST('2024-02-29' AS date)", "date", dt.date(2024, 2, 29)),
    ("CAST('12:34:56.1234567' AS time(7))", "time", dt.time(12, 34, 56, 123456)),
    ("CAST('2024-02-29 12:34:56.123' AS datetime)", "datetime", dt.datetime(2024, 2, 29, 12, 34, 56, 123000)),
    ("CAST('2024-02-29 12:34:56.1234567' AS datetime2(7))", "datetime2", dt.datetime(2024, 2, 29, 12, 34, 56, 123456)),
    ("CAST('2024-02-29 12:34:56.5 +05:30' AS datetimeoffset(1))", "datetimeoffset",
     dt.datetime(2024, 2, 29, 12, 34, 56, 500000, dt.timezone(dt.timedelta(hours=5, minutes=30)))),
    ("CAST('2024-02-29 12:34' AS smalldatetime)", "smalldatetime", dt.datetime(2024, 2, 29, 12, 34)),
    ("CAST('abc' AS char(5))", "char", "abc  "),
    ("CAST('café' AS varchar(10))", "varchar", "café"),
    ("CAST(N'日' AS nchar(3))", "nchar", "日  "),
    ("CAST(N'日\U0001f600' AS nvarchar(10))", "nvarchar", "日\U0001f600"),
    ("CAST(0x0102 AS binary(4))", "binary", b"\x01\x02\x00\x00"),
    ("CAST(0x0102 AS varbinary(4))", "varbinary", b"\x01\x02"),
    ("CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS uniqueidentifier)", "uniqueidentifier", G),
    ("CAST(N'" + "x" * 4000 + "' AS nvarchar(4000))", "nvarchar", "x" * 4000),
    ("CAST(NULL AS int)", None, None),
]


class TestSqlVariant:
    @pytest.mark.parametrize("expr, base, want", VARIANTS, ids=[f"{v[0][:30]}" for v in VARIANTS])
    def test_fetch_resolves_base_type(self, cur, expr, base, want):
        cur.execute(f"SELECT CAST({expr} AS sql_variant), SQL_VARIANT_PROPERTY(CAST({expr} AS sql_variant), 'BaseType')")
        v, bt = cur.fetchone()
        assert bt == base
        assert same(v, want), f"{v!r} != {want!r}"
        d = cur.description[0]
        assert d[1] is str  # description cannot know the base type; upstream reports str
        assert d[3] == 8000

    def test_fetch_mixed_rows_per_row_dispatch(self, cur):
        union = " UNION ALL ".join(f"SELECT {i} AS id, CAST({e} AS sql_variant) AS v" for i, (e, _, _) in enumerate(VARIANTS))
        cur.execute(f"SELECT v FROM ({union}) t ORDER BY id")
        rows = cur.fetchall()
        for r, (e, _, want) in zip(rows, VARIANTS):
            assert same(r[0], want), e
        cur.execute(f"SELECT v FROM ({union}) t ORDER BY id")
        got = []
        while True:
            part = cur.fetchmany(3)
            if not part:
                break
            got.extend(part)
        assert len(got) == len(VARIANTS)

    @pytest.mark.parametrize(
        "value, base",
        [
            (7, "tinyint"), (2**40, "bigint"), (True, "bit"), (1.5, "float"), (D("1.50"), "decimal"),
            ("text", "varchar"), ("\u65e5", "nvarchar"), (b"\x01\x02", "varbinary"), (dt.date(2024, 1, 2), "date"),
            (dt.datetime(2024, 1, 2, 3, 4, 5, 6), "datetime2"), (dt.time(1, 2, 3), "time"), (G, "uniqueidentifier"),
            (dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=2))), "datetimeoffset"),
        ],
    )
    def test_param_bind_into_variant_column(self, cur, value, base):
        t = table_of(cur, "(id int, v sql_variant)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (value,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, value)])
        cur.execute(f"SELECT v, SQL_VARIANT_PROPERTY(v, 'BaseType') FROM {t} ORDER BY id")
        for v, bt in cur.fetchall():
            # execute and executemany may pick decimal or numeric for a Decimal (same type to the server)
            assert bt == base or {bt, base} == {"decimal", "numeric"}
            assert same(v, value) or v == value
        cur.execute(f"DROP TABLE {t}")

    def test_null_param_into_variant(self, cur):
        t = table_of(cur, "(v sql_variant)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (None,))
        cur.execute(f"SELECT v, SQL_VARIANT_PROPERTY(v, 'BaseType') FROM {t}")
        assert tuple(cur.fetchone()) == (None, None)
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.skipif(not RUST, reason="bulk is Rust-port")
    def test_bulkcopy_rows_into_variant(self, cur):
        t = table_of(cur, "(id int, v sql_variant)")
        vals = [7, "text", D("1.50"), 1.5, b"\x01", dt.date(2024, 1, 2), True, None]
        cur.bulkcopy(t, list(enumerate(vals)))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        got = [r[0] for r in cur.fetchall()]
        assert got == vals
        cur.execute(f"DROP TABLE {t}")

    def test_arrow_fetch_is_unsupported_with_clear_error(self, cur):
        t = table_of(cur, "(v sql_variant)")
        cur.execute(f"INSERT INTO {t} VALUES (1)")
        cur.execute(f"SELECT v FROM {t}")
        with pytest.raises(Exception, match="Unsupported data type.*-150"):
            cur.fetchall_arrow()
        # connection still usable afterwards
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        cur.execute(f"DROP TABLE {t}")

    def test_arrow_fetch_workaround_cast_to_base(self, cur):
        t = table_of(cur, "(v sql_variant)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (42,))
        cur.execute(f"SELECT CAST(v AS int) AS v FROM {t}")
        assert pa.table(cur.fetchall_arrow()).column(0).to_pylist() == [42]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.skipif(not RUST, reason="bulk is Rust-port")
    def test_bulkcopy_arrow_into_variant(self, cur):
        t = table_of(cur, "(id int, v sql_variant)")
        try:
            cur.bulkcopy_arrow(t, pa.table({"id": pa.array([1], pa.int32()), "v": pa.array([5], pa.int64())}))
        except Exception as exc:
            assert "variant" in str(exc).lower() or "unsupported" in str(exc).lower()
        else:
            cur.execute(f"SELECT v FROM {t}")
            assert cur.fetchone()[0] == 5
        cur.execute(f"DROP TABLE {t}")


# ---------------------------------------------------------- spatial / hierarchy
SPATIAL = [
    ("geometry", "geometry::STGeomFromText('POINT (1 2)', 0)", "POINT (1 2)"),
    ("geometry", "geometry::STGeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 0)", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
    ("geometry", "geometry::STGeomFromText('LINESTRING (0 0, 1 1, 2 0)', 0)", "LINESTRING (0 0, 1 1, 2 0)"),
    ("geography", "geography::STGeomFromText('POINT (8.5 47.4)', 4326)", "POINT (8.5 47.4)"),
    ("geography", "geography::STGeomFromText('LINESTRING (8 47, 9 48)', 4326)", "LINESTRING (8 47, 9 48)"),
]


class TestSpatial:
    @pytest.mark.parametrize("typ, expr, wkt", SPATIAL, ids=[f"{s[0]}_{s[2][:12]}" for s in SPATIAL])
    def test_fetch_description_and_arrow(self, cur, typ, expr, wkt):
        t = table_of(cur, f"(id int, v {typ})")
        cur.execute(f"INSERT INTO {t} VALUES (1, {expr})")
        cur.execute(f"INSERT INTO {t} VALUES (2, NULL)")
        cur.execute(f"SELECT v, v.STAsText() FROM {t} ORDER BY id")
        assert cur.description[0][1] is bytes
        (raw, text), (none, _) = [tuple(r) for r in cur.fetchall()]
        assert type(raw) is bytes and text == wkt and none is None
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        col = pa.table(cur.fetchall_arrow()).column(0)
        assert col.type == pa.large_binary() and col.to_pylist() == [raw, None]
        # the serialized form binds back and round-trips exactly (param, executemany, bulk, bulk arrow)
        cur.execute(f"INSERT INTO {t} VALUES (3, ?)", (raw,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(4, raw)])
        if RUST:
            cur.bulkcopy(t, [(5, raw)])
            # unsupported path: UDT columns cannot be fed from Arrow, with a clear error
            with pytest.raises(ddbc_bindings.BulkUnsupported, match="not supported by the native Arrow bulk writer"):
                cur.bulkcopy_arrow(t, pa.table({"id": pa.array([6], pa.int32()), "v": pa.array([raw], pa.large_binary())}))
        cur.execute(f"SELECT v FROM {t} WHERE id >= 3 ORDER BY id")
        assert all(r[0] == raw for r in cur.fetchall())
        cur.execute(f"DROP TABLE {t}")

    def test_wkt_param_via_constructor(self, cur):
        cur.execute("SELECT geometry::STGeomFromText(?, 0).STAsText(), geography::STGeomFromText(?, 4326).STAsText()", ("POINT (3 4)", "POINT (10 20)"))
        assert tuple(cur.fetchone()) == ("POINT (3 4)", "POINT (10 20)")

    def test_spatial_srid_and_validity(self, cur):
        cur.execute("SELECT geometry::Point(1, 2, 25832).STSrid, geography::Point(47.4, 8.5, 4326).Lat")
        assert tuple(cur.fetchone()) == (25832, 47.4)

    def test_geometry_large_value_over_64k(self, cur):
        pts = ", ".join(f"{i} {i % 7}" for i in range(6000))
        wkt = f"LINESTRING ({pts})"
        cur.execute("SELECT geometry::STGeomFromText(?, 0)", (wkt,))
        raw = cur.fetchone()[0]
        assert len(raw) > 65_536
        t = table_of(cur, "(id int, v geometry)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (raw,))
        if RUST:
            cur.bulkcopy(t, [(2, raw)])
        cur.execute(f"SELECT v.STNumPoints(), v FROM {t} ORDER BY id")
        for n, v in cur.fetchall():
            assert n == 6000 and v == raw
        cur.execute(f"DROP TABLE {t}")

    def test_invalid_bytes_rejected_by_server(self, cur):
        t = table_of(cur, "(v geometry)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (b"\x00\x01\x02",))
        cur.execute(f"DROP TABLE {t}")

    def test_geometry_null_param(self, cur):
        t = table_of(cur, "(v geometry)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (None,))
        cur.execute(f"SELECT v FROM {t}")
        assert cur.fetchone()[0] is None
        cur.execute(f"DROP TABLE {t}")


class TestHierarchyId:
    PATHS = ["/", "/1/", "/1/2/", "/1.5/3/", "/10/20/30/", "/1/2/3/4/5/6/7/8/9/"]

    def test_all_paths(self, cur):
        t = table_of(cur, "(id int, v hierarchyid)")
        for i, p in enumerate(self.PATHS):
            cur.execute(f"INSERT INTO {t} VALUES (?, hierarchyid::Parse(?))", (i, p))
        cur.execute(f"INSERT INTO {t} VALUES (?, NULL)", (99,))
        cur.execute(f"SELECT v, v.ToString() FROM {t} ORDER BY id")
        assert cur.description[0][1] is bytes
        rows = cur.fetchall()
        assert [r[1] for r in rows] == self.PATHS + [None]
        raws = [r[0] for r in rows]
        assert all(type(r) is bytes for r in raws[:-1]) and raws[-1] is None
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        col = pa.table(cur.fetchall_arrow()).column(0)
        assert col.type == pa.large_binary() and col.to_pylist() == raws
        t2 = table_of(cur, "(id int, v hierarchyid)")
        cur.executemany(f"INSERT INTO {t2} VALUES (?, ?)", list(enumerate(raws)))
        if RUST:
            t3 = table_of(cur, "(id int, v hierarchyid)")
            t4 = table_of(cur, "(id int, v hierarchyid)")
            cur.bulkcopy(t3, list(enumerate(raws)))
            with pytest.raises(ddbc_bindings.BulkUnsupported, match="not supported by the native Arrow bulk writer"):
                cur.bulkcopy_arrow(t4, pa.table({"id": pa.array(range(len(raws)), pa.int32()), "v": pa.array(raws, pa.large_binary())}))
            tables = [t2, t3]
            cur.execute(f"DROP TABLE {t4}")
        else:
            tables = [t2]
        for tb in tables:
            cur.execute(f"SELECT v.ToString() FROM {tb} ORDER BY id")
            assert [r[0] for r in cur.fetchall()] == self.PATHS + [None]
        for tb in [t, *tables]:
            cur.execute(f"DROP TABLE {tb}")

    def test_binary_order_matches_document_order(self, cur):
        cur.execute("SELECT hierarchyid::Parse('/1/') , hierarchyid::Parse('/1/2/'), hierarchyid::Parse('/2/')")
        a, b, c = cur.fetchone()
        assert a < b < c

    def test_hierarchy_methods_with_param(self, cur):
        cur.execute("SELECT hierarchyid::Parse(?).GetLevel(), hierarchyid::Parse(?).GetAncestor(1).ToString()", ("/1/2/3/", "/1/2/3/"))
        assert tuple(cur.fetchone()) == (3, "/1/2/")


# --------------------------------------------------------------- json / vector
class TestJsonVector:
    @pytest.fixture
    def server_has(self, cur):
        def check(sql):
            try:
                cur.execute(sql)
                cur.fetchall()
            except DatabaseError:
                pytest.skip("server lacks this type (needs SQL Server 2025 / Azure SQL)")
        return check

    def test_json_type(self, cur, server_has):
        server_has("SELECT CAST('{}' AS json)")
        t = table_of(cur, "(id int, v json)")
        doc = '{"a":1,"b":[1,2,{"c":"日"}]}'
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (doc,))
        cur.execute(f"SELECT v FROM {t}")
        got = cur.fetchone()[0]
        assert isinstance(got, str) and "日" in got
        cur.execute(f"DROP TABLE {t}")

    def test_vector_type(self, cur, server_has):
        server_has("SELECT CAST('[1,2]' AS vector(2))")
        t = table_of(cur, "(id int, v vector(3))")
        cur.execute(f"INSERT INTO {t} VALUES (1, CAST('[1.5,2.5,3.5]' AS vector(3)))")
        cur.execute(f"SELECT v FROM {t}")
        got = cur.fetchone()[0]
        assert got is not None
        cur.execute(f"DROP TABLE {t}")

    def test_json_vector_absent_on_this_server_is_clean_error(self, cur):
        try:
            cur.execute("SELECT CAST('{}' AS json)")
            cur.fetchall()
        except DatabaseError as exc:
            assert "json" in str(exc).lower()
        else:
            pytest.skip("server has json")


# -------------------------------------------------------- table / cursor types
class TestTableAndCursorTypes:
    def test_table_valued_parameter_is_rejected_cleanly(self, cur):
        name = f"rs_tt_{uuid.uuid4().hex[:8]}"
        cur.execute(f"CREATE TYPE {name} AS TABLE (a int, b nvarchar(10))")
        proc = f"rs_tp_{uuid.uuid4().hex[:8]}"
        cur.execute(f"CREATE PROCEDURE {proc} @t {name} READONLY AS SELECT SUM(a) FROM @t")
        try:
            with pytest.raises(TypeError):
                cur.execute(f"EXEC {proc} ?", ([(1, "x"), (2, "y")],))
            # the supported route: declare the table variable in the batch
            cur.execute(f"SET NOCOUNT ON; DECLARE @t {name}; INSERT @t VALUES (1, N'x'), (2, N'y'); EXEC {proc} @t")
            assert cur.fetchone()[0] == 3
            # ... or pass rows as JSON and shred them server-side
            import json

            cur.execute(
                f"SET NOCOUNT ON; DECLARE @t {name}; INSERT @t SELECT a, b FROM OPENJSON(?) WITH (a int, b nvarchar(10)); EXEC {proc} @t",
                (json.dumps([{"a": 5, "b": "x"}, {"a": 6, "b": "y"}]),),
            )
            assert cur.fetchone()[0] == 11
        finally:
            cur.execute(f"DROP PROCEDURE {proc}")
            cur.execute(f"DROP TYPE {name}")

    def test_table_variable_typed_columns_roundtrip(self, cur):
        cur.execute(
            "SET NOCOUNT ON; DECLARE @t TABLE (a bigint, b decimal(20,5), c datetime2(7), d uniqueidentifier, e varbinary(max), f nvarchar(max)); "
            "INSERT @t VALUES (?, ?, ?, ?, ?, ?); SELECT * FROM @t",
            (2**40, D("123.45678"), dt.datetime(2024, 1, 2, 3, 4, 5, 6), G, b"\x01" * 9000, "日" * 9000),
        )
        assert tuple(cur.fetchone()) == (2**40, D("123.45678"), dt.datetime(2024, 1, 2, 3, 4, 5, 6), G, b"\x01" * 9000, "日" * 9000)

    def test_table_type_cannot_be_a_column(self, cur):
        with pytest.raises(DatabaseError):
            cur.execute("CREATE TABLE #bad (t table)")

    def test_cursor_type_in_tsql_batch_and_cursor_output_param(self, cur):
        proc = f"rs_cp_{uuid.uuid4().hex[:8]}"
        cur.execute(f"CREATE PROCEDURE {proc} @c cursor VARYING OUTPUT AS SET @c = CURSOR FOR SELECT n FROM (VALUES (1), (2), (3)) v(n); OPEN @c")
        try:
            cur.execute(
                f"DECLARE @c cursor, @n int, @sum int = 0; EXEC {proc} @c OUTPUT; "
                "FETCH NEXT FROM @c INTO @n; WHILE @@FETCH_STATUS = 0 BEGIN SET @sum += @n; FETCH NEXT FROM @c INTO @n; END; "
                "CLOSE @c; DEALLOCATE @c; SELECT @sum"
            )
            assert cur.fetchone()[0] == 6
        finally:
            cur.execute(f"DROP PROCEDURE {proc}")

    def test_cursor_type_cannot_be_returned_as_column(self, cur):
        with pytest.raises(DatabaseError):
            cur.execute("SELECT CAST(1 AS cursor)")

    def test_server_side_cursor_rows_are_typed(self, cur):
        cur.execute(
            "DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT CAST(1.5 AS decimal(5,2)), CAST('2024-01-02' AS date); "
            "OPEN c; FETCH NEXT FROM c; CLOSE c; DEALLOCATE c"
        )
        # The server appends a hidden int ROWSTAT column to T-SQL FETCH results; like the
        # ODBC driver, we hide it.
        assert len(cur.description) == 2
        assert "ROWSTAT" not in [d[0] for d in cur.description]
        assert tuple(cur.fetchone()) == (D("1.50"), dt.date(2024, 1, 2))


# ---------------------------------------------------------------- setinputsizes
class TestSetInputSizes:
    def test_decimal_precision_and_scale(self, cur):
        t = table_of(cur, "(v decimal(38,10))")
        cur.setinputsizes([(mssql_python.SQL_DECIMAL, 38, 10)])
        v = D("12345678901234567890123456.1234567890")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (v,))
        cur.execute(f"SELECT v FROM {t}")
        assert cur.fetchone()[0] == v
        cur.execute(f"DROP TABLE {t}")

    def test_wvarchar_max_for_large_string(self, cur):
        t = table_of(cur, "(v nvarchar(max))")
        s = "日" * 50_000
        cur.setinputsizes([(mssql_python.SQL_WVARCHAR, 0, 0)])
        cur.execute(f"INSERT INTO {t} VALUES (?)", (s,))
        cur.execute(f"SELECT v FROM {t}")
        assert cur.fetchone()[0] == s
        cur.execute(f"DROP TABLE {t}")

    def test_varbinary_max_executemany(self, cur):
        t = table_of(cur, "(id int, v varbinary(max))")
        cur.setinputsizes([(mssql_python.SQL_INTEGER, 0, 0), (mssql_python.SQL_VARBINARY, 0, 0)])
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(1, b"\x01" * 20_000), (2, None), (3, b"")])
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [b"\x01" * 20_000, None, b""]
        cur.execute(f"DROP TABLE {t}")

    def test_varchar_for_ascii_column_keeps_value(self, cur):
        t = table_of(cur, "(v varchar(20))")
        cur.setinputsizes([(mssql_python.SQL_VARCHAR, 20, 0)])
        cur.execute(f"INSERT INTO {t} VALUES (?)", ("abc",))
        cur.execute(f"SELECT v FROM {t}")
        assert cur.fetchone()[0] == "abc"
        cur.execute(f"DROP TABLE {t}")

    def test_sizes_reset_after_execute(self, cur):
        t = table_of(cur, "(v int)")
        cur.setinputsizes([(mssql_python.SQL_INTEGER, 0, 0)])
        cur.execute(f"INSERT INTO {t} VALUES (?)", (1,))
        cur.execute(f"INSERT INTO {t} VALUES (?)", ("2",))  # would fail if the previous sizes stuck around with a mismatched type
        cur.execute(f"SELECT v FROM {t} ORDER BY v")
        assert [r[0] for r in cur.fetchall()] == [1, 2]
        cur.execute(f"DROP TABLE {t}")


# ------------------------------------------------------------ NULL typing
class TestNullsEverywhere:
    @pytest.mark.parametrize(
        "ddl",
        ["bigint", "int", "smallint", "tinyint", "bit", "decimal(38,10)", "numeric(5,2)", "money", "smallmoney", "float", "real",
         "date", "time(7)", "datetime", "datetime2(7)", "datetimeoffset(7)", "smalldatetime", "char(5)", "varchar(max)", "text",
         "nchar(5)", "nvarchar(max)", "ntext", "binary(5)", "varbinary(max)", "image", "uniqueidentifier", "xml", "sql_variant",
         "hierarchyid", "geometry", "geography"],
    )
    def test_null_param_into_every_type_and_back(self, cur, ddl):
        t = table_of(cur, f"(id int, v {ddl})")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (None,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, None), (3, None)])
        if RUST and ddl not in ("sql_variant",):
            cur.bulkcopy(t, [(4, None)])
        cur.execute(f"SELECT v FROM {t}")
        assert all(r[0] is None for r in cur.fetchall())
        if ddl != "sql_variant":
            cur.execute(f"SELECT v FROM {t}")
            col = pa.table(cur.fetchall_arrow()).column(0)
            assert col.null_count == col.length()
        cur.execute(f"DROP TABLE {t}")

    def test_all_null_column_arrow_type_is_still_typed(self, cur):
        cur.execute("SELECT CAST(NULL AS int) a, CAST(NULL AS nvarchar(5)) b, CAST(NULL AS decimal(9,3)) c, CAST(NULL AS datetime2(3)) d")
        t = pa.table(cur.fetchall_arrow())
        assert [str(x) for x in t.schema.types] == ["int32", "large_string", "decimal128(9, 3)", "timestamp[us]"]
        assert t.num_rows == 1 and all(c.null_count == 1 for c in t.columns)

    def test_empty_result_arrow_schema_keeps_types(self, cur):
        cur.execute("SELECT CAST(1 AS bigint) a, CAST('x' AS varchar(3)) b, CAST(1 AS decimal(20,4)) c, CAST('12:00' AS time(7)) d WHERE 1 = 0")
        t = pa.table(cur.fetchall_arrow())
        assert t.num_rows == 0
        assert [str(x) for x in t.schema.types] == ["int64", "large_string", "decimal128(20, 4)", "time64[ns]"]
