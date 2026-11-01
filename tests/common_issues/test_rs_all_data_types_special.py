"""Data types that need special handling across paths, and the edge values
(NaN/inf, -0.0, rounding, 100 ns precision, GUID byte order, offsets, ...).

The generic type x path matrix is in test_rs_all_data_types_paths.py.
"""

import datetime as dt
import math
import struct
import uuid
from decimal import Decimal as D

import pytest

from mssql_python import DataError, DatabaseError, ProgrammingError, connect
from mssql_python import ddbc_bindings

from rs_dtypes import f32, same

pa = pytest.importorskip("pyarrow")
RUST = getattr(ddbc_bindings, "__rust_port__", False)


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


def one(cur, sql, *params):
    cur.execute(sql, *params)
    return cur.fetchone()[0]


def table_of(cur, ddl):
    name = f"##rs_sp_{uuid.uuid4().hex[:10]}"
    cur.execute(f"CREATE TABLE {name} {ddl}")
    return name


# ------------------------------------------------------------------ floats
class TestFloatSpecials:
    @pytest.mark.parametrize("col", ["float", "real"])
    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"])
    def test_non_finite_bind_rejected_cleanly(self, cur, col, bad):
        t = table_of(cur, f"(v {col})")
        with pytest.raises(DataError, match="non-finite"):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (bad,))
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        assert cur.fetchone()[0] == 0
        cur.execute(f"DROP TABLE {t}")

    def test_non_finite_executemany_rejected(self, cur):
        t = table_of(cur, "(v float)")
        with pytest.raises(DataError):
            cur.executemany(f"INSERT INTO {t} VALUES (?)", [(1.0,), (float("nan"),)])
        cur.execute(f"DROP TABLE {t}")

    def test_server_cannot_produce_nan_inf(self, cur):
        with pytest.raises(DatabaseError):
            cur.execute("SELECT CAST('nan' AS float)")
        with pytest.raises(DatabaseError):
            cur.execute("SELECT CAST('inf' AS float)")

    def test_negative_zero_roundtrip_via_param(self, cur):
        t = table_of(cur, "(v float)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (-0.0,))
        v = one(cur, f"SELECT v FROM {t}")
        assert struct.pack("<d", v) == struct.pack("<d", -0.0)
        cur.execute(f"DROP TABLE {t}")

    def test_negative_zero_arrow(self, cur):
        t = table_of(cur, "(v float)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (-0.0,))
        cur.execute(f"SELECT v FROM {t}")
        v = pa.table(cur.fetchall_arrow()).column(0).to_pylist()[0]
        assert math.copysign(1.0, v) == -1.0
        cur.execute(f"DROP TABLE {t}")

    def test_negative_zero_literal_is_positive_zero(self, cur):
        # -0.0 in T-SQL is numeric negation of 0: positive zero.
        assert math.copysign(1.0, one(cur, "SELECT CAST(-0.0 AS float)")) == 1.0

    @pytest.mark.parametrize("v", [5e-324, 2.2250738585072009e-308, 1e-310, -5e-324])
    def test_float_subnormals_via_param(self, cur, v):
        t = table_of(cur, "(v float)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (v,))
        assert same(one(cur, f"SELECT v FROM {t}"), v)
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("v", [5e-324, 1e-310])
    def test_float_subnormals_bulk(self, cur, v):
        if not RUST:
            pytest.skip("bulk is Rust-port")
        t = table_of(cur, "(id int, v float)")
        cur.bulkcopy(t, [(1, v)])
        cur.bulkcopy_arrow(t, pa.table({"id": pa.array([2], pa.int32()), "v": pa.array([v])}))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [v, v]
        cur.execute(f"DROP TABLE {t}")

    def test_real_subnormal(self, cur):
        sub = 1.401298464324817e-45  # smallest float32 subnormal
        t = table_of(cur, "(v real)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (sub,))
        got = one(cur, f"SELECT v FROM {t}")
        assert got == f32(sub)
        cur.execute(f"DROP TABLE {t}")

    def test_real_overflow_on_bind(self, cur):
        t = table_of(cur, "(v real)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (1e300,))
        cur.execute(f"DROP TABLE {t}")

    def test_float_max_roundtrip_all_paths(self, cur):
        mx = 1.7976931348623157e308
        t = table_of(cur, "(id int, v float)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (mx,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, -mx)])
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [mx, -mx]
        cur.execute(f"DROP TABLE {t}")

    def test_float_repr_roundtrip_is_exact(self, cur):
        vals = [0.1, 1 / 3, 2 / 3, math.pi, math.e, 1e22, 123456789012345.67, 5e-5]
        t = table_of(cur, "(id int, v float)")
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", list(enumerate(vals)))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == vals
        cur.execute(f"DROP TABLE {t}")

    def test_float_n_maps_to_real_or_double(self, cur):
        t = table_of(cur, "(a float(1), b float(24), c float(25), d float(53))")
        cur.execute(f"SELECT a, b, c, d FROM {t}")
        assert [d[4] for d in cur.description] == [24, 24, 53, 53]
        assert pa.table(cur.fetchall_arrow()).schema.types == [pa.float32(), pa.float32(), pa.float64(), pa.float64()]
        cur.execute(f"DROP TABLE {t}")

    def test_real_bind_rounds_to_float32(self, cur):
        t = table_of(cur, "(v real)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (0.1,))
        assert one(cur, f"SELECT v FROM {t}") == f32(0.1)
        cur.execute(f"DROP TABLE {t}")

    def test_float_from_int_and_decimal_params(self, cur):
        t = table_of(cur, "(v float)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (3,))
        cur.execute(f"INSERT INTO {t} VALUES (?)", (D("2.5"),))
        cur.execute(f"SELECT v FROM {t} ORDER BY v")
        assert [r[0] for r in cur.fetchall()] == [2.5, 3.0]
        cur.execute(f"DROP TABLE {t}")


# ----------------------------------------------------------------- money
class TestMoneyRounding:
    @pytest.mark.parametrize(
        "text, expected",
        [("1.23455", "1.2346"), ("1.23454", "1.2345"), ("-1.23455", "-1.2346"), ("0.00005", "0.0001"), ("0.00004", "0.0000")],
    )
    def test_server_rounds_half_away_from_zero(self, cur, text, expected):
        assert one(cur, f"SELECT CAST({text} AS money)") == D(expected)
        t = table_of(cur, "(v money)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (D(text),))
        got = one(cur, f"SELECT v FROM {t}")
        assert got == D(expected) and got.as_tuple().exponent == -4
        cur.execute(f"DROP TABLE {t}")

    def test_money_overflow_bind(self, cur):
        t = table_of(cur, "(v smallmoney)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (D("214748.3648"),))
        cur.execute(f"DROP TABLE {t}")

    def test_money_arrow_exact_integer_scale(self, cur):
        cur.execute("SELECT CAST(922337203685477.5807 AS money)")
        col = pa.table(cur.fetchall_arrow()).column(0)
        assert col.type == pa.decimal128(19, 4)
        assert col.to_pylist() == [D("922337203685477.5807")]


# ---------------------------------------------------------------- decimals
class TestDecimalSpecials:
    def test_decimal_38_digits_all_paths(self, cur):
        v = D("12345678901234567890123456789012345678")
        t = table_of(cur, "(id int, v decimal(38,0))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (v,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, -v)])
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [v, -v]
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert pa.table(cur.fetchall_arrow()).column(0).to_pylist() == [v, -v]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("p", [1, 9, 10, 18, 19, 28, 29, 37, 38])
    def test_precision_overflow_is_server_error(self, cur, p):
        t = table_of(cur, f"(v decimal({p},0))")
        # p=38: 10**38 has 39 digits, refused client-side before it reaches the server
        with pytest.raises((DatabaseError, ValueError)):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (D(10) ** p,))
        cur.execute(f"DROP TABLE {t}")

    def test_scale_rounding_on_insert(self, cur):
        t = table_of(cur, "(v decimal(10,2))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (D("1.005"),))
        cur.execute(f"INSERT INTO {t} VALUES (?)", (D("1.004"),))
        cur.execute(f"SELECT v FROM {t}")
        assert sorted(r[0] for r in cur.fetchall()) == [D("1.00"), D("1.01")]
        cur.execute(f"DROP TABLE {t}")

    def test_decimal_nan_inf_rejected(self, cur):
        t = table_of(cur, "(v decimal(10,2))")
        for bad in (D("NaN"), D("Infinity"), D("-Infinity")):
            with pytest.raises((DataError, DatabaseError, ProgrammingError, ValueError)):
                cur.execute(f"INSERT INTO {t} VALUES (?)", (bad,))
        cur.execute(f"DROP TABLE {t}")

    def test_numeric_is_decimal_alias(self, cur):
        t = table_of(cur, "(v numeric(12,3))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (D("123456789.123"),))
        cur.execute(f"SELECT v FROM {t}")
        assert cur.description[0][1] is D and cur.description[0][4:6] == (12, 3)
        assert cur.fetchone()[0] == D("123456789.123")
        cur.execute(f"DROP TABLE {t}")

    def test_default_precision_decimal(self, cur):
        t = table_of(cur, "(v decimal, w decimal(5))")
        cur.execute(f"SELECT v, w FROM {t}")
        assert [d[4:6] for d in cur.description] == [(18, 0), (5, 0)]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("v", [D("1E+3"), D("1E-10"), D("0E-10"), D("-0.00"), D("123456789012345678.1234567890")])
    def test_decimal_exponent_forms_roundtrip_value(self, cur, v):
        t = table_of(cur, "(v decimal(38,10))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (v,))
        assert one(cur, f"SELECT v FROM {t}") == v
        cur.execute(f"DROP TABLE {t}")


# ------------------------------------------------------------ date and time
class TestTemporalPrecision:
    def test_time7_row_truncates_arrow_keeps_100ns(self, cur):
        cur.execute("SELECT CAST('12:34:56.1234567' AS time(7))")
        assert cur.fetchone()[0] == dt.time(12, 34, 56, 123456)
        cur.execute("SELECT CAST('12:34:56.1234567' AS time(7))")
        col = pa.table(cur.fetchall_arrow()).column(0)
        assert col.type == pa.time64("ns")
        assert col.cast(pa.int64()).to_pylist() == [((12 * 60 + 34) * 60 + 56) * 10**9 + 123_456_700]

    def test_datetime2_7_row_truncates_arrow_is_microseconds(self, cur):
        s = "2024-05-06 07:08:09.1234567"
        cur.execute(f"SELECT CAST('{s}' AS datetime2(7))")
        assert cur.fetchone()[0] == dt.datetime(2024, 5, 6, 7, 8, 9, 123456)
        cur.execute(f"SELECT CAST('{s}' AS datetime2(7))")
        col = pa.table(cur.fetchall_arrow()).column(0)
        # documented mapping: timestamp[us]; the 100 ns digit is dropped (no overflow of the date range)
        assert col.type == pa.timestamp("us")
        assert col.to_pylist() == [dt.datetime(2024, 5, 6, 7, 8, 9, 123456)]

    def test_datetimeoffset7_truncates(self, cur):
        cur.execute("SELECT CAST('2024-05-06 07:08:09.9999999 +01:00' AS datetimeoffset(7))")
        v = cur.fetchone()[0]
        assert v.microsecond == 999999 and v.utcoffset() == dt.timedelta(hours=1)

    def test_full_precision_available_as_text(self, cur):
        s = "2024-05-06 07:08:09.1234567"
        assert one(cur, f"SELECT CONVERT(varchar(30), CAST('{s}' AS datetime2(7)), 121)") == s

    @pytest.mark.parametrize("n", range(8))
    def test_bound_python_values_keep_microseconds(self, cur, n):
        t = table_of(cur, f"(id int, t time({n}), d datetime2({n}), o datetimeoffset({n}))")
        us = 123456
        tz = dt.timezone(dt.timedelta(hours=-3))
        cur.execute(
            f"INSERT INTO {t} VALUES (1, ?, ?, ?)",
            (dt.time(1, 2, 3, us), dt.datetime(2024, 1, 2, 3, 4, 5, us), dt.datetime(2024, 1, 2, 3, 4, 5, us, tz)),
        )
        cur.execute(f"SELECT CONVERT(varchar(40), t, 121), CONVERT(varchar(40), d, 121), CONVERT(varchar(40), o, 121) FROM {t}")
        tt, dd, oo = cur.fetchone()
        frac = ("." + "123456"[:n].ljust(n, "0")) if n else ""
        # a column with fewer digits than the bound value rounds (server conversion)
        if n >= 6:
            assert tt == "01:02:03" + frac and dd == "2024-01-02 03:04:05" + frac
        else:
            assert tt.startswith("01:02:03") and dd.startswith("2024-01-02 03:04:05")
        cur.execute(f"DROP TABLE {t}")

    def test_datetime_rounds_to_third_millisecond(self, cur):
        t = table_of(cur, "(id int, v datetime)")
        vals = [dt.datetime(2024, 1, 1, 0, 0, 0, 1000), dt.datetime(2024, 1, 1, 0, 0, 0, 2000), dt.datetime(2024, 1, 1, 0, 0, 0, 4000),
                dt.datetime(2024, 1, 1, 0, 0, 0, 5000), dt.datetime(2024, 1, 1, 0, 0, 0, 998000), dt.datetime(2024, 1, 1, 23, 59, 59, 999000)]
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", list(enumerate(vals)))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        got = [r[0] for r in cur.fetchall()]
        # DATETIME has 1/300 s ticks: .001 -> .000, .002 -> .003, .004 -> .003, .005 -> .007, .998 -> .997, .999 -> 00:00:00 next day
        want_ms = [0, 3, 3, 7, 997]
        assert [g.microsecond // 1000 for g in got[:5]] == want_ms
        assert got[5] == dt.datetime(2024, 1, 2, 0, 0, 0)
        cur.execute(f"DROP TABLE {t}")

    def test_datetime_out_of_range_rejected(self, cur):
        t = table_of(cur, "(v datetime)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (dt.datetime(1752, 12, 31),))
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize(
        "value, expected",
        [
            (dt.datetime(2024, 1, 1, 12, 29, 29, 999000), dt.datetime(2024, 1, 1, 12, 29)),
            (dt.datetime(2024, 1, 1, 12, 29, 30), dt.datetime(2024, 1, 1, 12, 30)),
            (dt.datetime(2024, 1, 1, 23, 59, 30), dt.datetime(2024, 1, 2, 0, 0)),
            (dt.datetime(2079, 6, 6, 23, 59, 29), dt.datetime(2079, 6, 6, 23, 59)),
            (dt.datetime(1900, 1, 1, 0, 0, 29), dt.datetime(1900, 1, 1, 0, 0)),
        ],
    )
    def test_smalldatetime_rounds_to_minute(self, cur, value, expected):
        t = table_of(cur, "(v smalldatetime)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (value,))
        assert one(cur, f"SELECT v FROM {t}") == expected
        cur.execute(f"DROP TABLE {t}")

    def test_smalldatetime_out_of_range(self, cur):
        t = table_of(cur, "(v smalldatetime)")
        for bad in (dt.datetime(1899, 12, 31), dt.datetime(2079, 6, 7)):
            with pytest.raises(DatabaseError):
                cur.execute(f"INSERT INTO {t} VALUES (?)", (bad,))
        cur.execute(f"DROP TABLE {t}")

    def test_date_param_from_datetime_and_str(self, cur):
        t = table_of(cur, "(id int, v date)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (dt.date(2024, 2, 29),))
        cur.execute(f"INSERT INTO {t} VALUES (2, ?)", ("2020-12-31",))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [dt.date(2024, 2, 29), dt.date(2020, 12, 31)]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("hours, minutes", [(14, 0), (-14, 0), (5, 30), (-9, -30), (0, 0), (5, 45)])
    def test_datetimeoffset_offsets(self, cur, hours, minutes):
        off = dt.timedelta(hours=hours, minutes=minutes)
        tz = dt.timezone(off)
        v = dt.datetime(2024, 6, 15, 12, 0, 0, 500000, tz)
        t = table_of(cur, "(v datetimeoffset(3))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (v,))
        got = one(cur, f"SELECT v FROM {t}")
        assert got == v and got.utcoffset() == off
        cur.execute(f"SELECT DATEPART(tz, v) FROM {t}")
        assert cur.fetchone()[0] == off.total_seconds() // 60
        cur.execute(f"SELECT v FROM {t}")
        arrow_v = pa.table(cur.fetchall_arrow()).column(0)
        assert arrow_v.cast(pa.int64()).to_pylist() == [int((v - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)) / dt.timedelta(microseconds=1))]
        cur.execute(f"DROP TABLE {t}")

    def test_datetimeoffset_beyond_14h_rejected(self, cur):
        t = table_of(cur, "(v datetimeoffset)")
        with pytest.raises(RuntimeError, match="out of SQL Server range"):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (dt.datetime(2024, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=14, minutes=1))),))
        cur.execute(f"DROP TABLE {t}")

    def test_naive_datetime_into_datetimeoffset_is_offset_zero(self, cur):
        t = table_of(cur, "(v datetimeoffset)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (dt.datetime(2024, 1, 1, 10, 0),))
        got = one(cur, f"SELECT v FROM {t}")
        assert got == dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        cur.execute(f"DROP TABLE {t}")

    def test_time_params_maximum(self, cur):
        t = table_of(cur, "(v time(6))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (dt.time(23, 59, 59, 999999),))
        assert one(cur, f"SELECT v FROM {t}") == dt.time(23, 59, 59, 999999)
        cur.execute(f"DROP TABLE {t}")

    def test_time_with_tzinfo_param_rejected(self, cur):
        t = table_of(cur, "(v time(6))")
        with pytest.raises(ValueError, match="no UTC offset"):
            cur.execute(f"INSERT INTO {t} VALUES (?)", (dt.time(1, 2, 3, tzinfo=dt.timezone.utc),))
        cur.execute(f"DROP TABLE {t}")

    def test_timestamp_arrow_range_edges(self, cur):
        cur.execute("SELECT CAST('0001-01-01' AS date), CAST('9999-12-31' AS date)")
        t = pa.table(cur.fetchall_arrow())
        assert t.to_pylist() == [{"": dt.date(1, 1, 1)} if False else t.to_pylist()[0]]
        assert t.column(0).to_pylist() == [dt.date(1, 1, 1)] and t.column(1).to_pylist() == [dt.date(9999, 12, 31)]


# --------------------------------------------------------------------- GUID
class TestGuid:
    G = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

    def test_text_form_matches_server(self, cur):
        assert one(cur, "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS uniqueidentifier)") == self.G

    def test_mixed_endian_wire_layout(self, cur):
        # The server stores Data1-3 little-endian: CAST(guid AS binary(16)) shows it.
        cur.execute("SELECT CAST(? AS uniqueidentifier), CAST(CAST(? AS uniqueidentifier) AS binary(16))", (self.G, self.G))
        g, raw = cur.fetchone()
        assert g == self.G
        assert raw == self.G.bytes_le

    def test_uuid_param_roundtrip_and_text(self, cur):
        g = uuid.uuid4()
        cur.execute("SELECT ?, CAST(? AS varchar(36))", (g, g))
        back, txt = cur.fetchone()
        assert back == g and txt.lower() == str(g)

    def test_guid_sort_order_is_server_order(self, cur):
        a = uuid.UUID("00000000-0000-0000-0000-000000000001")
        b = uuid.UUID("00000000-0000-0000-0001-000000000000")
        cur.execute("SELECT v FROM (VALUES (CAST(? AS uniqueidentifier)), (CAST(? AS uniqueidentifier))) t(v) ORDER BY v", (b, a))
        # SQL Server compares the last 6 bytes first, so b (zero tail) sorts before a
        assert [r[0] for r in cur.fetchall()] == [b, a]

    def test_bytes_16_does_not_become_guid_in_varbinary(self, cur):
        t = table_of(cur, "(v varbinary(16))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (self.G.bytes,))
        assert one(cur, f"SELECT v FROM {t}") == self.G.bytes
        cur.execute(f"DROP TABLE {t}")

    def test_guid_as_string_param(self, cur):
        t = table_of(cur, "(v uniqueidentifier)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (str(self.G),))
        assert one(cur, f"SELECT v FROM {t}") == self.G
        cur.execute(f"DROP TABLE {t}")

    def test_invalid_guid_string_rejected(self, cur):
        t = table_of(cur, "(v uniqueidentifier)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", ("not-a-guid",))
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.skipif(not RUST, reason="bulk is Rust-port")
    def test_bulk_uuid_object_and_string(self, cur):
        t = table_of(cur, "(id int, v uniqueidentifier)")
        cur.bulkcopy(t, [(1, self.G), (2, str(self.G))])
        cur.execute(f"SELECT v FROM {t}")
        assert [r[0] for r in cur.fetchall()] == [self.G, self.G]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.skipif(not RUST, reason="bulk is Rust-port")
    def test_bulk_arrow_guid_from_fixed_size_binary_is_rejected_or_exact(self, cur):
        # 16 raw bytes in RFC order must land as the same GUID (not byte-swapped), or be rejected.
        t = table_of(cur, "(id int, v uniqueidentifier)")
        tbl = pa.table({"id": pa.array([1], pa.int32()), "v": pa.array([self.G.bytes], pa.binary(16))})
        try:
            cur.bulkcopy_arrow(t, tbl)
        except Exception as exc:  # clear error is acceptable
            assert "uniqueidentifier" in str(exc).lower() or "guid" in str(exc).lower() or "binary" in str(exc).lower()
        else:
            assert one(cur, f"SELECT v FROM {t}") == self.G
        cur.execute(f"DROP TABLE {t}")


# ------------------------------------------------------------ strings / LOBs
class TestStrings:
    def test_embedded_nul_all_paths(self, cur):
        v = "a\x00b\x00"
        t = table_of(cur, "(id int, n nvarchar(20), m nvarchar(max))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?, ?)", (v, v))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?, ?)", [(2, v, v)])
        if RUST:
            cur.bulkcopy(t, [(3, v, v)])
            cur.bulkcopy_arrow(t, pa.table({"id": pa.array([4], pa.int32()), "n": [v], "m": [v]}))
        cur.execute(f"SELECT n, m, LEN(n), DATALENGTH(n) FROM {t} ORDER BY id")
        for n, m, ln, dl in cur.fetchall():
            assert n == v and m == v and dl == 8
        cur.execute(f"SELECT n, m FROM {t} ORDER BY id")
        a = pa.table(cur.fetchall_arrow())
        assert set(a.column(0).to_pylist()) == {v} and set(a.column(1).to_pylist()) == {v}
        cur.execute(f"DROP TABLE {t}")

    def test_lone_surrogate_param_rejected(self, cur):
        t = table_of(cur, "(v nvarchar(20))")
        with pytest.raises(Exception) as exc:
            cur.execute(f"INSERT INTO {t} VALUES (?)", ("a\ud800b",))
        assert "surrogate" in str(exc.value).lower() or "encode" in str(exc.value).lower()
        cur.execute(f"DROP TABLE {t}")

    def test_lone_surrogate_in_bulk_rejected(self, cur):
        if not RUST:
            pytest.skip("bulk is Rust-port")
        t = table_of(cur, "(id int, v nvarchar(20))")
        with pytest.raises(Exception) as exc:
            cur.bulkcopy(t, [(1, "a\ud800b")])
        assert "surrogate" in str(exc.value).lower() or "encode" in str(exc.value).lower() or "utf" in str(exc.value).lower()
        cur.execute(f"DROP TABLE {t}")

    def test_server_side_lone_surrogate_fetch_is_decode_error(self, cur):
        # Documented limitation (same as upstream): invalid UTF-16 stored server-side
        # cannot become a Python str; the row fetch raises UnicodeDecodeError.
        cur.execute("SELECT NCHAR(0xD800) + N'x'")
        with pytest.raises(UnicodeDecodeError):
            cur.fetchone()
        # the connection stays usable
        assert one(cur, "SELECT 1") == 1

    def test_server_side_lone_surrogate_arrow_is_replaced(self, cur):
        # Arrow strings must be valid UTF-8: invalid UTF-16 becomes U+FFFD instead of an error.
        cur.execute("SELECT NCHAR(0xD800) + N'x', NCHAR(0xDC00) + NCHAR(0xD800)")
        t = pa.table(cur.fetchall_arrow())
        assert t.column(0).to_pylist() == ["\ufffdx"] and t.column(1).to_pylist() == ["\ufffd\ufffd"]

    def test_supplementary_planes_length(self, cur):
        s = "\U0001f600\U0001f680\U00020000"
        t = table_of(cur, "(v nvarchar(10))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (s,))
        cur.execute(f"SELECT v, LEN(v), DATALENGTH(v) FROM {t}")
        v, ln, dl = cur.fetchone()
        assert v == s and len(v) == 3 and ln == 6 and dl == 12
        cur.execute(f"DROP TABLE {t}")

    def test_nvarchar_param_over_column_size_is_truncation_error(self, cur):
        t = table_of(cur, "(v nvarchar(3), w varchar(3), x char(3), y varbinary(3))")
        for col, val in (("v", "abcd"), ("w", "abcd"), ("x", "abcd"), ("y", b"abcd")):
            with pytest.raises(DatabaseError, match="truncated"):
                cur.execute(f"INSERT INTO {t} ({col}) VALUES (?)", (val,))
        cur.execute(f"DROP TABLE {t}")

    def test_varchar_non_representable_becomes_question_mark(self, cur):
        t = table_of(cur, "(v varchar(10) COLLATE Latin1_General_CI_AS)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", ("a日b",))
        assert one(cur, f"SELECT v FROM {t}") == "a?b"
        cur.execute(f"DROP TABLE {t}")

    def test_utf8_collation_roundtrip_bytes_and_len(self, cur):
        s = "\U0001f600日éa"
        t = table_of(cur, "(v varchar(20) COLLATE Latin1_General_100_CI_AS_SC_UTF8)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (s,))
        cur.execute(f"SELECT v, DATALENGTH(v) FROM {t}")
        v, dl = cur.fetchone()
        assert v == s and dl == 4 + 3 + 2 + 1
        cur.execute(f"SELECT v FROM {t}")
        assert pa.table(cur.fetchall_arrow()).column(0).to_pylist() == [s]
        cur.execute(f"DROP TABLE {t}")

    def test_utf8_collation_column_over_byte_size(self, cur):
        t = table_of(cur, "(v varchar(5) COLLATE Latin1_General_100_CI_AS_SC_UTF8)")
        with pytest.raises(DatabaseError, match="truncated"):
            cur.execute(f"INSERT INTO {t} VALUES (?)", ("日本",))  # 6 bytes
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("n", [3999, 4000, 4001, 8000, 8001, 32767, 32768, 65535, 65536, 65537, 131072, 1_000_000])
    def test_nvarchar_max_size_boundaries(self, cur, n):
        s = ("日a" * n)[:n]
        t = table_of(cur, "(id int, v nvarchar(max))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (s,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, s)])
        cur.execute(f"SELECT v, DATALENGTH(v) FROM {t} ORDER BY id")
        for v, dl in cur.fetchall():
            assert v == s and dl == 2 * n
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert pa.table(cur.fetchall_arrow()).column(0).to_pylist() == [s, s]
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.parametrize("n", [7999, 8000, 8001, 65535, 65536, 65537, 1_000_000])
    def test_varbinary_max_size_boundaries(self, cur, n):
        b = bytes((i * 7) % 256 for i in range(n))
        t = table_of(cur, "(id int, v varbinary(max))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (b,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, b)])
        if RUST:
            cur.bulkcopy(t, [(3, b)])
            cur.bulkcopy_arrow(t, pa.table({"id": pa.array([4], pa.int32()), "v": pa.array([b], pa.large_binary())}))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert all(r[0] == b for r in cur.fetchall())
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert all(x == b for x in pa.table(cur.fetchall_arrow()).column(0).to_pylist())
        cur.execute(f"DROP TABLE {t}")

    def test_lob_spanning_many_packets_with_small_rows(self, cur):
        # one big LOB between small rows: row framing must survive packet boundaries
        t = table_of(cur, "(id int, s nvarchar(max), b varbinary(max), i int)")
        big_s, big_b = "z" * 150_000, bytes(range(256)) * 600
        for i in range(5):
            cur.execute(f"INSERT INTO {t} VALUES (?, ?, ?, ?)", (i, big_s if i % 2 else "x", big_b if i % 2 else b"\x01", i * 10))
        cur.execute(f"SELECT * FROM {t} ORDER BY id")
        rows = cur.fetchall()
        for i, (a, s, b, c) in enumerate(rows):
            assert a == i and c == i * 10
            assert s == (big_s if i % 2 else "x") and b == (big_b if i % 2 else b"\x01")
        cur.execute(f"DROP TABLE {t}")

    def test_text_ntext_image_deprecated_types_roundtrip_params(self, cur):
        t = table_of(cur, "(id int, t text, n ntext, i image)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?, ?, ?)", ("café" * 3000, "日" * 9000, b"\x00\x01" * 9000))
        cur.execute(f"SELECT t, n, i FROM {t}")
        a, b, c = cur.fetchone()
        assert a == "café" * 3000 and b == "日" * 9000 and c == b"\x00\x01" * 9000
        cur.execute(f"DROP TABLE {t}")

    def test_empty_string_vs_null_distinct(self, cur):
        t = table_of(cur, "(id int, v nvarchar(10), w varbinary(10))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?, ?)", ("", b""))
        cur.execute(f"INSERT INTO {t} VALUES (2, ?, ?)", (None, None))
        cur.execute(f"SELECT v, w FROM {t} ORDER BY id")
        assert [tuple(r) for r in cur.fetchall()] == [("", b""), (None, None)]
        cur.execute(f"SELECT v, w FROM {t} ORDER BY id")
        a = pa.table(cur.fetchall_arrow())
        assert a.column(0).to_pylist() == ["", None] and a.column(1).to_pylist() == [b"", None]
        cur.execute(f"DROP TABLE {t}")

    def test_char_padding_semantics(self, cur):
        t = table_of(cur, "(a char(5), b nchar(5), c binary(5))")
        cur.execute(f"INSERT INTO {t} VALUES (?, ?, ?)", ("ab", "日", b"\x01"))
        cur.execute(f"SELECT a, b, c FROM {t}")
        assert tuple(cur.fetchone()) == ("ab   ", "日    ", b"\x01\x00\x00\x00\x00")
        cur.execute(f"DROP TABLE {t}")


# --------------------------------------------------------------------- XML
class TestXml:
    @pytest.mark.parametrize(
        "text",
        [
            "<a/>",
            "<a>é日\U0001f600</a>",
            "<a b='1' c=\"2\"><b/><!-- c --><?pi x?></a>",
            "<ns:a xmlns:ns='urn:x'><ns:b/></ns:a>",
            "<a>" + "x" * 100_000 + "</a>",
        ],
    )
    def test_xml_roundtrip_all_paths(self, cur, text):
        t = table_of(cur, "(id int, v xml)")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (text,))
        cur.executemany(f"INSERT INTO {t} VALUES (?, ?)", [(2, text)])
        if RUST:
            cur.bulkcopy(t, [(3, text)])
            cur.bulkcopy_arrow(t, pa.table({"id": pa.array([4], pa.int32()), "v": [text]}))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        rows = [r[0] for r in cur.fetchall()]
        assert len(set(rows)) == 1 and isinstance(rows[0], str)
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert set(pa.table(cur.fetchall_arrow()).column(0).to_pylist()) == set(rows)
        # the server normalised it, so re-binding the normalised form is stable
        cur.execute(f"SELECT CAST(? AS xml)", (rows[0],))
        assert cur.fetchone()[0] == rows[0]
        cur.execute(f"DROP TABLE {t}")

    def test_xml_encoding_declaration_utf16_ok(self, cur):
        t = table_of(cur, "(v xml)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", ('<?xml version="1.0" encoding="utf-16"?><a>日</a>',))
        assert one(cur, f"SELECT v FROM {t}") == "<a>日</a>"
        cur.execute(f"DROP TABLE {t}")

    def test_xml_encoding_declaration_utf8_str_param_accepted(self, cur):
        t = table_of(cur, "(v xml)")
        cur.execute(f"INSERT INTO {t} VALUES (?)", ('<?xml version="1.0" encoding="utf-8"?><a/>',))
        assert one(cur, f"SELECT v FROM {t}") == "<a/>"
        cur.execute(f"DROP TABLE {t}")

    def test_xml_encoding_declaration_in_n_literal_is_server_error(self, cur):
        with pytest.raises(DatabaseError, match="encoding"):
            cur.execute("""SELECT CAST(N'<?xml version="1.0" encoding="utf-8"?><a/>' AS xml)""")

    def test_xml_encoding_declaration_utf8_via_bytes(self, cur):
        t = table_of(cur, "(v xml)")
        cur.execute(f"INSERT INTO {t} VALUES (CAST(? AS xml))", ('<?xml version="1.0" encoding="utf-8"?><a>é</a>'.encode("utf-8"),))
        assert one(cur, f"SELECT v FROM {t}") == "<a>é</a>"
        cur.execute(f"DROP TABLE {t}")

    def test_malformed_xml_rejected(self, cur):
        t = table_of(cur, "(v xml)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (?)", ("<a><b></a>",))
        cur.execute(f"DROP TABLE {t}")

    def test_xml_nulls_and_empty_document_fragment(self, cur):
        t = table_of(cur, "(id int, v xml)")
        cur.execute(f"INSERT INTO {t} VALUES (1, NULL)")
        cur.execute(f"INSERT INTO {t} VALUES (2, ?)", ("",))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [None, ""]
        cur.execute(f"DROP TABLE {t}")

    def test_typed_xml_with_schema_collection(self, cur):
        name = f"rs_xsc_{uuid.uuid4().hex[:8]}"
        cur.execute(
            f"CREATE XML SCHEMA COLLECTION {name} AS N'<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            '<xs:element name="n" type="xs:int"/></xs:schema>\''
        )
        try:
            t = f"rs_xt_{uuid.uuid4().hex[:8]}"  # permanent: a temp table cannot see the collection
            cur.execute(f"CREATE TABLE {t} (v xml({name}))")
            try:
                cur.execute(f"INSERT INTO {t} VALUES (?)", ("<n>5</n>",))
                assert one(cur, f"SELECT v FROM {t}") == "<n>5</n>"
                with pytest.raises(DatabaseError):
                    cur.execute(f"INSERT INTO {t} VALUES (?)", ("<n>x</n>",))
            finally:
                cur.execute(f"DROP TABLE {t}")
        finally:
            cur.execute(f"DROP XML SCHEMA COLLECTION {name}")


# ----------------------------------------------------- binary / rowversion
class TestBinaryAndRowversion:
    def test_bytearray_and_memoryview_params(self, cur):
        t = table_of(cur, "(id int, v varbinary(max))")
        cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (bytearray(b"\x01\x02"),))
        cur.execute(f"INSERT INTO {t} VALUES (2, ?)", (memoryview(b"\x03\x04"),))
        cur.execute(f"SELECT v FROM {t} ORDER BY id")
        got = [r[0] for r in cur.fetchall()]
        assert got == [b"\x01\x02", b"\x03\x04"] and all(type(g) is bytes for g in got)
        cur.execute(f"DROP TABLE {t}")

    def test_binary_fixed_length_pads_with_zero(self, cur):
        t = table_of(cur, "(v binary(6))")
        cur.execute(f"INSERT INTO {t} VALUES (?)", (b"\xaa",))
        assert one(cur, f"SELECT v FROM {t}") == b"\xaa\x00\x00\x00\x00\x00"
        cur.execute(f"DROP TABLE {t}")

    def test_rowversion_fetch_and_description(self, cur):
        t = table_of(cur, "(id int, r rowversion)")
        t2 = table_of(cur, "(id int, t timestamp)")  # timestamp is the deprecated rowversion synonym
        for tb in (t, t2):
            cur.execute(f"INSERT INTO {tb} (id) VALUES (1)")
            cur.execute(f"SELECT * FROM {tb}")
            d = cur.description[1]
            assert d[1] is bytes and d[3] == 8 and d[6] is False
            r = cur.fetchone()[1]
            assert type(r) is bytes and len(r) == 8
            cur.execute(f"DROP TABLE {tb}")

    def test_rowversion_monotonic_and_big_endian_order(self, cur):
        t = table_of(cur, "(id int, r rowversion)")
        for i in range(3):
            cur.execute(f"INSERT INTO {t} (id) VALUES (?)", (i,))
        cur.execute(f"SELECT r FROM {t} ORDER BY id")
        vals = [int.from_bytes(r[0], "big") for r in cur.fetchall()]
        assert vals == sorted(vals) and len(set(vals)) == 3
        cur.execute(f"DROP TABLE {t}")

    def test_rowversion_arrow_and_update_changes_it(self, cur):
        t = table_of(cur, "(id int, x int, r rowversion)")
        cur.execute(f"INSERT INTO {t} (id, x) VALUES (1, 1)")
        cur.execute(f"SELECT r FROM {t}")
        a = pa.table(cur.fetchall_arrow())
        assert a.schema.types[0] == pa.large_binary()
        before = a.column(0).to_pylist()[0]
        cur.execute(f"UPDATE {t} SET x = 2")
        assert one(cur, f"SELECT r FROM {t}") != before
        cur.execute(f"DROP TABLE {t}")

    def test_rowversion_is_not_insertable(self, cur):
        t = table_of(cur, "(id int, r rowversion)")
        with pytest.raises(DatabaseError):
            cur.execute(f"INSERT INTO {t} VALUES (1, ?)", (b"\x00" * 8,))
        cur.execute(f"DROP TABLE {t}")

    @pytest.mark.skipif(not RUST, reason="bulk is Rust-port")
    def test_rowversion_bulk_skips_generated_column(self, cur):
        t = table_of(cur, "(id int, r rowversion, x int)")
        cur.bulkcopy(t, [(1, 10)], column_mappings=[(0, "id"), (1, "x")])
        cur.execute(f"SELECT id, x, r FROM {t}")
        i, x, r = cur.fetchone()
        assert (i, x) == (1, 10) and len(r) == 8
        cur.execute(f"DROP TABLE {t}")

    def test_binary_arrow_types_for_all_binary_flavours(self, cur):
        cur.execute("SELECT CAST(0x01 AS binary(2)), CAST(0x01 AS varbinary(2)), CAST(0x01 AS varbinary(max)), CAST(0x01 AS image), @@DBTS")
        assert all(ty == pa.large_binary() for ty in pa.table(cur.fetchall_arrow()).schema.types)
