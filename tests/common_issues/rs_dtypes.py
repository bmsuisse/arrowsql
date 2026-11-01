"""Catalog of SQL Server data types for the systematic data-type suite.

Source: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
(and the per-type sub-pages for ranges, precision and storage).

Each ``Case`` is one column type with a list of ``Row`` values:

* ``Row.s``     T-SQL expression that produces the value server-side.
* ``Row.py``    exact Python value a row fetch must return (``None`` = NULL).
* ``Row.bind``  value bound as a parameter / bulk-copied (default: ``py``).
* ``Row.arrow`` value the Arrow column holds (default: ``py``); for temporal
  Arrow types with ``Case.arrow_int`` it is the raw integer (ns / us / days).

The files ``test_rs_all_data_types_*.py`` run every case through every path.
"""

from __future__ import annotations

import datetime as dt
import math
import struct
import uuid
from dataclasses import dataclass, field
from decimal import Decimal as D
from typing import Any, Callable, List, Optional

SAME = object()


@dataclass
class Row:
    s: str
    py: Any
    bind: Any = SAME
    arrow: Any = SAME
    arrow_in: Any = SAME

    @property
    def bind_value(self):
        return self.py if self.bind is SAME else self.bind

    @property
    def arrow_in_value(self):
        return self.bind_value if self.arrow_in is SAME else self.arrow_in

    @property
    def arrow_value(self):
        return self.py if self.arrow is SAME else self.arrow


@dataclass
class Case:
    id: str
    ddl: str
    rows: List[Row]
    py_type: type
    arrow_type: str  # str(pyarrow type) of the fetched column
    desc: tuple  # (type_code, internal_size, precision, scale), as upstream reports
    arrow_int: bool = False  # compare temporal Arrow columns as raw integers
    pa_in: Optional[Callable] = None  # pyarrow type used to build bulkcopy_arrow input
    bulk_rows: bool = True  # cursor.bulkcopy supports the type
    bulk_arrow: bool = True  # cursor.bulkcopy_arrow supports the type
    arrow_fetch: bool = True  # Arrow fetch supports the type
    heavy: bool = True  # False: only run on the cheap paths (fetch/desc/bind/arrow)
    bind_params: bool = True  # python parameter binding into this column works
    notes: str = ""
    nullable_rows: bool = True

    def all_rows(self) -> List[Row]:
        rows = list(self.rows)
        if self.nullable_rows:
            rows.append(Row("NULL", None))
        return rows


def C(ddl: str, text: str) -> str:
    """CAST a string literal to the column type."""
    return f"CAST({text} AS {ddl})"


def Q(ddl: str, text: str) -> str:
    return C(ddl, "'" + text.replace("'", "''") + "'")


def f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


EPOCH = dt.datetime(1970, 1, 1)


def us_since_epoch(v: dt.datetime) -> int:
    if v.tzinfo is not None:
        v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return (v - EPOCH) // dt.timedelta(microseconds=1)


def time_ns(h, m, s, ns=0) -> int:
    return ((h * 60 + m) * 60 + s) * 1_000_000_000 + ns


# --------------------------------------------------------------------------
# comparison helpers
# --------------------------------------------------------------------------
def same(a, b) -> bool:
    """Strict equality: type, value, float bit pattern, Decimal exponent, tz offset."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float):
        return struct.pack("<d", a) == struct.pack("<d", b) or (math.isnan(a) and math.isnan(b))
    if isinstance(a, D):
        return a == b and a.as_tuple().exponent == b.as_tuple().exponent
    if isinstance(a, dt.datetime):
        return a == b and a.utcoffset() == b.utcoffset()
    return a == b


# --------------------------------------------------------------------------
# exact numerics
# --------------------------------------------------------------------------
def _ints(name, lo, hi, size, prec, extra=()):
    rows = [Row(f"CAST('{v}' AS {name})", v) for v in (lo, hi, 0, 1, -1 if lo < 0 else 2, *extra)]
    return Case(
        name, name, rows, int,
        {"bigint": "int64", "int": "int32", "smallint": "int16", "tinyint": "uint8"}[name],
        (int, size, prec, 0),
        pa_in=lambda pa, _n=name: getattr(pa, {"bigint": "int64", "int": "int32", "smallint": "int16", "tinyint": "uint8"}[_n])(),
    )


NUMERIC_CORE: List[Case] = [
    _ints("bigint", -(2**63), 2**63 - 1, 19, 19, (2**53 + 1, -(2**53) - 1)),
    _ints("int", -(2**31), 2**31 - 1, 10, 10),
    _ints("smallint", -(2**15), 2**15 - 1, 5, 5),
    _ints("tinyint", 0, 255, 3, 3),
    Case(
        "bit", "bit",
        [Row("CAST(1 AS bit)", True), Row("CAST(0 AS bit)", False)],
        bool, "bool", (bool, 1, 1, 0), pa_in=lambda pa: pa.bool_(),
    ),
]


def decimal_case(p: int, s: int) -> Case:
    ddl = f"decimal({p},{s})"
    ip = "9" * (p - s) or "0"
    fr = ("." + "9" * s) if s else ""
    mx = ip + fr
    rows = [
        Row(Q(ddl, mx), D(mx)),
        Row(Q(ddl, "-" + mx), D("-" + mx)),
        Row(Q(ddl, "0"), D("0").scaleb(0) if s == 0 else D("0." + "0" * s)),
    ]
    if s:
        rows.append(Row(Q(ddl, "0." + "0" * (s - 1) + "1"), D("0." + "0" * (s - 1) + "1")))
    if p - s >= 1:
        one = "1" + (("." + "0" * s) if s else "")
        rows.append(Row(Q(ddl, one), D(one)))
    return Case(
        f"decimal_{p}_{s}", ddl, rows, D, f"decimal128({p}, {s})", (D, p, p, s),
        pa_in=lambda pa, _p=p, _s=s: pa.decimal128(_p, _s),
    )


DECIMAL_HEAVY = [(1, 0), (1, 1), (9, 2), (10, 0), (18, 6), (19, 4), (28, 14), (29, 0), (38, 0), (38, 18), (38, 38)]
DECIMAL_ALL = sorted({(p, s) for p in range(1, 39) for s in (0, p // 2, p)})


def money_cases() -> List[Case]:
    return [
        Case(
            "money", "money",
            [
                Row(Q("money", "922337203685477.5807"), D("922337203685477.5807")),
                Row(Q("money", "-922337203685477.5808"), D("-922337203685477.5808")),
                Row(Q("money", "0"), D("0.0000")),
                Row(Q("money", "0.0001"), D("0.0001")),
                Row(Q("money", "-0.0001"), D("-0.0001")),
            ],
            D, "decimal128(19, 4)", (D, 19, 19, 4), pa_in=lambda pa: pa.decimal128(19, 4),
        ),
        Case(
            "smallmoney", "smallmoney",
            [
                Row(Q("smallmoney", "214748.3647"), D("214748.3647")),
                Row(Q("smallmoney", "-214748.3648"), D("-214748.3648")),
                Row(Q("smallmoney", "0"), D("0.0000")),
                Row(Q("smallmoney", "0.0001"), D("0.0001")),
            ],
            D, "decimal128(10, 4)", (D, 10, 10, 4), pa_in=lambda pa: pa.decimal128(10, 4),
        ),
    ]


# --------------------------------------------------------------------------
# approximate numerics
# --------------------------------------------------------------------------
FLOAT_CORE = [
    Case(
        "float53", "float(53)",
        [
            Row(Q("float", v), float(v))
            for v in ("0", "1.5", "-1.5", "0.1", "1.7976931348623157e308", "-1.7976931348623157e308",
                      "2.2250738585072014e-308", "1e-300", "123456789.123456789")
        ],
        float, "double", (float, 53, 53, 0), pa_in=lambda pa: pa.float64(),
    ),
    Case(
        "float24", "float(24)",
        [
            Row(Q("float(24)", v), f32(float(v)))
            for v in ("0", "1.5", "-1.5", "0.1", "3.4028234663852886e38", "-3.4028234663852886e38",
                      "1.1754943508222875e-38", "16777217")
        ],
        float, "float", (float, 24, 24, 0), pa_in=lambda pa: pa.float32(),
    ),
    Case(
        "real", "real",
        [Row(Q("real", v), f32(float(v))) for v in ("0", "2.5", "-2.5", "0.3", "3.4028234663852886e38")],
        float, "float", (float, 24, 24, 0), pa_in=lambda pa: pa.float32(),
    ),
]
# float(n) with 1<=n<=24 is real (24), 25<=n<=53 is float (53)
FLOAT_N = {1: "real", 24: "real", 25: "float", 53: "float"}


# --------------------------------------------------------------------------
# date and time
# --------------------------------------------------------------------------
def _frac(n: int, digits: str) -> str:
    return digits[:n]


def time_case(n: int) -> Case:
    ddl = f"time({n})"
    full = "1234567"
    f = _frac(n, full)
    base = "12:34:56" + (("." + f) if n else "")
    top = "23:59:59" + (("." + "9" * n) if n else "")
    # Python time keeps microseconds: truncate (never round) the 7th digit.
    def py_of(h, m, s, frac):
        us = int((frac + "000000")[:6]) if frac else 0
        return dt.time(h, m, s, us)

    def ns_of(h, m, s, frac):
        return time_ns(h, m, s, int((frac + "000000000")[:9]) if frac else 0)

    rows = [
        Row(Q(ddl, "00:00:00"), dt.time(0, 0, 0), arrow=0),
        Row(Q(ddl, base), py_of(12, 34, 56, f), arrow=ns_of(12, 34, 56, f)),
        Row(Q(ddl, top), py_of(23, 59, 59, "9" * n), arrow=ns_of(23, 59, 59, "9" * n)),
    ]
    return Case(
        f"time_{n}", ddl, rows, dt.time, "time64[ns]", (dt.time, 8 if n == 0 else 9 + n, 8 if n == 0 else 9 + n, n),
        arrow_int=True, pa_in=lambda pa: pa.time64("ns"),
        notes="time(7) row fetch truncates to microseconds; Arrow keeps 100 ns",
    )


def datetime2_case(n: int) -> Case:
    ddl = f"datetime2({n})"
    full = "1234567"
    f = _frac(n, full)
    def lit(y, mo, d, h, mi, s, frac):
        return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}" + (("." + frac) if n else "")

    def py(y, mo, d, h, mi, s, frac):
        us = int((frac + "000000")[:6]) if frac else 0
        return dt.datetime(y, mo, d, h, mi, s, us)

    nines = "9" * n
    rows = [
        Row(Q(ddl, lit(1, 1, 1, 0, 0, 0, "0" * n)), dt.datetime(1, 1, 1)),
        Row(Q(ddl, lit(2024, 2, 29, 12, 34, 56, f)), py(2024, 2, 29, 12, 34, 56, f)),
        Row(Q(ddl, lit(9999, 12, 31, 23, 59, 59, nines)), py(9999, 12, 31, 23, 59, 59, nines)),
        Row(Q(ddl, lit(1970, 1, 1, 0, 0, 0, "0" * n)), dt.datetime(1970, 1, 1)),
    ]
    for r in rows:
        r.arrow = us_since_epoch(r.py)
    return Case(
        f"datetime2_{n}", ddl, rows, dt.datetime, "timestamp[us]",
        (dt.datetime, 19 if n == 0 else 20 + n, 19 if n == 0 else 20 + n, n),
        arrow_int=True, pa_in=lambda pa: pa.timestamp("us"),
        notes="Arrow timestamp[us]: the 100 ns digit of datetime2(7) is truncated (documented mapping)",
    )


def datetimeoffset_case(n: int) -> Case:
    ddl = f"datetimeoffset({n})"
    f = _frac(n, "1234567")
    frac = ("." + f) if n else ""
    tz14 = dt.timezone(dt.timedelta(hours=14))
    tzm14 = dt.timezone(dt.timedelta(hours=-14))
    tzm = dt.timezone(dt.timedelta(hours=5, minutes=30))
    us = int((f + "000000")[:6]) if f else 0
    rows = [
        Row(Q(ddl, f"2024-03-04 05:06:07{frac} +14:00"), dt.datetime(2024, 3, 4, 5, 6, 7, us, tz14)),
        Row(Q(ddl, f"2024-03-04 05:06:07{frac} -14:00"), dt.datetime(2024, 3, 4, 5, 6, 7, us, tzm14)),
        Row(Q(ddl, f"2024-03-04 05:06:07{frac} +05:30"), dt.datetime(2024, 3, 4, 5, 6, 7, us, tzm)),
        Row(Q(ddl, f"2024-03-04 05:06:07{frac} +00:00"), dt.datetime(2024, 3, 4, 5, 6, 7, us, dt.timezone.utc)),
        Row(Q(ddl, "0001-01-02 00:00:00 +14:00"), dt.datetime(1, 1, 2, tzinfo=tz14)),
        Row(Q(ddl, "9999-12-30 23:59:59 -14:00"), dt.datetime(9999, 12, 30, 23, 59, 59, tzinfo=tzm14)),
    ]
    for r in rows:
        r.arrow = us_since_epoch(r.py)
    return Case(
        f"datetimeoffset_{n}", ddl, rows, dt.datetime, "timestamp[us, tz=+00:00]",
        (dt.datetime, 26 if n == 0 else 27 + n, 26 if n == 0 else 27 + n, n),
        arrow_int=True, pa_in=lambda pa: pa.timestamp("us", tz="UTC"),
        notes="Arrow converts to UTC (offset is not preserved)",
    )


DATE_CORE = [
    Case(
        "date", "date",
        [
            Row(Q("date", "0001-01-01"), dt.date(1, 1, 1)),
            Row(Q("date", "9999-12-31"), dt.date(9999, 12, 31)),
            Row(Q("date", "2024-02-29"), dt.date(2024, 2, 29)),
            Row(Q("date", "1970-01-01"), dt.date(1970, 1, 1)),
            Row(Q("date", "1900-01-01"), dt.date(1900, 1, 1)),
        ],
        dt.date, "date32[day]", (dt.date, 10, 10, 0), pa_in=lambda pa: pa.date32(),
    ),
    Case(
        "datetime", "datetime",
        [
            Row(Q("datetime", "1753-01-01 00:00:00.000"), dt.datetime(1753, 1, 1)),
            Row(Q("datetime", "9999-12-31 23:59:59.997"), dt.datetime(9999, 12, 31, 23, 59, 59, 997000)),
            Row(Q("datetime", "2024-02-29 12:34:56.003"), dt.datetime(2024, 2, 29, 12, 34, 56, 3000)),
            Row(Q("datetime", "2024-02-29 12:34:56.007"), dt.datetime(2024, 2, 29, 12, 34, 56, 7000)),
            Row(Q("datetime", "1900-01-01 00:00:00.000"), dt.datetime(1900, 1, 1)),
        ],
        dt.datetime, "timestamp[us]", (dt.datetime, 23, 23, 3), arrow_int=True, pa_in=lambda pa: pa.timestamp("us"),
    ),
    Case(
        "smalldatetime", "smalldatetime",
        [
            Row(Q("smalldatetime", "1900-01-01 00:00"), dt.datetime(1900, 1, 1)),
            Row(Q("smalldatetime", "2079-06-06 23:59"), dt.datetime(2079, 6, 6, 23, 59)),
            Row(Q("smalldatetime", "2024-02-29 12:34"), dt.datetime(2024, 2, 29, 12, 34)),
        ],
        dt.datetime, "timestamp[us]", (dt.datetime, 16, 16, 0), arrow_int=True, pa_in=lambda pa: pa.timestamp("us"),
    ),
]
for _c in DATE_CORE:
    if _c.arrow_int:
        for _r in _c.rows:
            _r.arrow = us_since_epoch(_r.py)
for _r in DATE_CORE[0].rows:
    _r.arrow = (_r.py - dt.date(1970, 1, 1)).days
DATE_CORE[0].arrow_int = True

TIME_CASES = [time_case(n) for n in range(8)]
DATETIME2_CASES = [datetime2_case(n) for n in range(8)]
DTO_CASES = [datetimeoffset_case(n) for n in range(8)]

# --------------------------------------------------------------------------
# character / unicode / binary
# --------------------------------------------------------------------------
def str_case(ddl: str, values, *, unicode: bool, pad: Optional[int] = None, py_type=str, **kw) -> Case:
    rows = []
    for v in values:
        lit = ("N" if unicode else "") + "'" + v.replace("'", "''") + "'"
        # embedded NULs cannot sit in a literal; build with CHAR(0)/NCHAR(0)
        if "\x00" in v:
            parts = v.split("\x00")
            fn = "NCHAR(0)" if unicode else "CHAR(0)"
            lit = " + ".join(
                x for p in parts for x in (f"{'N' if unicode else ''}'{p.replace(chr(39), chr(39) * 2)}'", fn)
            )
            lit = lit[: -len(" + " + fn)]
            lit = f"CAST({lit} AS {ddl})"
        py = v if pad is None else v.ljust(pad)
        rows.append(Row(lit, py, bind=v))
    name = ddl.replace("(", "_").replace(")", "").replace(" ", "_").replace(",", "_")
    return Case(name, ddl, rows, py_type, "large_string", kw.pop("desc"), pa_in=lambda pa: pa.large_string(), **kw)


ASCII = ["", "a", "hello world", "x" * 50, " lead and trail ", "it's \"quoted\"", "tab\there\r\nnewline"]
LATIN1 = ["café üöä ß", "£€"]  # cp1252 has the euro sign
UNI = ["日本語テスト", "\U0001f600\U0001f680 emoji", "مرحبا שלום", "a\x00b", "é combining",
       "\U0001f468‍\U0001f469‍\U0001f467 zwj", "é" * 50]


def _bytes_pattern(n: int, seed: int = 7) -> bytes:
    return bytes((i * 31 + seed) % 256 for i in range(n))


def _bin_rows(ddl: str, values, pad: Optional[int] = None):
    rows = []
    for v in values:
        lit = "0x" + v.hex() if v else "0x"
        rows.append(Row(f"CAST({lit} AS {ddl})", v if pad is None else v.ljust(pad, b"\x00"), bind=v))
    return rows


def _case_for(name, ddl, rows, py_type, arrow_type, desc, **kw):
    return Case(name, ddl, rows, py_type, arrow_type, desc, **kw)


STRING_CASES: List[Case] = [
    str_case("char(10)", ["", "a", "abcdefghij", "ab cd"], unicode=False, pad=10, desc=(str, 10, 10, 0)),
    str_case("char(1)", ["a", " ", "z"], unicode=False, pad=1, desc=(str, 1, 1, 0)),
    str_case("char(8000)", ["x", "y" * 8000, "café"], unicode=False, pad=8000, desc=(str, 8000, 8000, 0)),
    str_case("varchar(50)", ASCII + LATIN1, unicode=False, desc=(str, 50, 50, 0)),
    str_case("varchar(1)", ["", "a"], unicode=False, desc=(str, 1, 1, 0)),
    str_case("varchar(8000)", ["x" * 8000, "y" * 7999 + "é", "short"], unicode=False, desc=(str, 8000, 8000, 0)),
    str_case(
        "varchar(max)",
        ASCII + LATIN1 + ["m" * 8001, "n" * 70_000, "o" * 300_000, ("0123456789" * 6554)[:65_536], "p" * 65_535],
        unicode=False, desc=(str, 0, 0, 0),
    ),
    str_case(
        "text", ASCII + LATIN1 + ["t" * 8001, "u" * 70_000, "v" * 300_000], unicode=False,
        desc=(str, 2147483647, 2147483647, 0),
    ),
    str_case("nchar(10)", ["", "a", "abcdefghij", "日本"], unicode=True, pad=10, desc=(str, 10, 10, 0)),
    str_case("nchar(4000)", ["x", "日" * 4000], unicode=True, pad=4000, desc=(str, 4000, 4000, 0)),
    str_case("nvarchar(50)", ASCII + LATIN1 + UNI, unicode=True, desc=(str, 50, 50, 0)),
    str_case("nvarchar(1)", ["", "a", "日"], unicode=True, desc=(str, 1, 1, 0)),
    str_case("nvarchar(4000)", ["日" * 4000, "\U0001f600" * 1999, "short"], unicode=True, desc=(str, 4000, 4000, 0)),
    str_case(
        "nvarchar(max)",
        ASCII + LATIN1 + UNI
        + ["m" * 4001, "日" * 40_000, "\U0001f600" * 20_000, "p" * 200_000, "q" * 32_768, "r" * 65_536, ("é\U0001f600" * 40_000)],
        unicode=True, desc=(str, 0, 0, 0),
    ),
    str_case(
        "ntext", ASCII + LATIN1 + UNI + ["n" * 4001, "日" * 40_000, "z" * 200_000], unicode=True,
        desc=(str, 1073741823, 1073741823, 0),
    ),
    # UTF-8 collation: varchar holds full Unicode
    str_case(
        "varchar(100) COLLATE Latin1_General_100_CI_AS_SC_UTF8",
        ASCII + LATIN1 + UNI[:3] + UNI[4:], unicode=True, desc=(str, 100, 100, 0),
    ),
    str_case(
        "varchar(max) COLLATE Latin1_General_100_CI_AS_SC_UTF8",
        ["\U0001f600" * 20_000, "日本" * 40_000, "ascii" * 20_000], unicode=True, desc=(str, 0, 0, 0),
    ),
]

BIN_CORE = [0, 1, 2, 255, 256, 8000]
BINARY_CASES: List[Case] = [
    Case("binary_4", "binary(4)", _bin_rows("binary(4)", [b"\x01\x02\x03\x04", b"\x00\x00\x00\x00", b"\xff\xff\xff\xff", b"\x01"], pad=4),
         bytes, "large_binary", (bytes, 4, 4, 0), pa_in=lambda pa: pa.large_binary()),
    Case("binary_8000", "binary(8000)", _bin_rows("binary(8000)", [_bytes_pattern(8000), b"\x01"], pad=8000),
         bytes, "large_binary", (bytes, 8000, 8000, 0), pa_in=lambda pa: pa.large_binary()),
    Case("varbinary_20", "varbinary(20)", _bin_rows("varbinary(20)", [b"", b"\x00", b"\x00\x01\x00", _bytes_pattern(20), b"\xff"]),
         bytes, "large_binary", (bytes, 20, 20, 0), pa_in=lambda pa: pa.large_binary()),
    Case("varbinary_8000", "varbinary(8000)", _bin_rows("varbinary(8000)", [_bytes_pattern(8000), _bytes_pattern(7999)]),
         bytes, "large_binary", (bytes, 8000, 8000, 0), pa_in=lambda pa: pa.large_binary()),
    Case("varbinary_max", "varbinary(max)",
         _bin_rows("varbinary(max)", [b"", b"\x00", _bytes_pattern(8001), _bytes_pattern(65_535), _bytes_pattern(65_536), _bytes_pattern(70_000), _bytes_pattern(300_000)]),
         bytes, "large_binary", (bytes, 0, 0, 0), pa_in=lambda pa: pa.large_binary()),
    Case("image", "image",
         _bin_rows("image", [b"", b"\x00\x01", _bytes_pattern(8001), _bytes_pattern(70_000), _bytes_pattern(300_000)]),
         bytes, "large_binary", (bytes, 2147483647, 2147483647, 0), pa_in=lambda pa: pa.large_binary()),
]

# --------------------------------------------------------------------------
# other types
# --------------------------------------------------------------------------
G1 = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")
G2 = uuid.UUID("00000000-0000-0000-0000-000000000000")
G3 = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
G4 = uuid.UUID("01020304-0506-0708-090a-0b0c0d0e0f10")

OTHER_CASES: List[Case] = [
    Case(
        "uniqueidentifier", "uniqueidentifier",
        [Row(Q("uniqueidentifier", str(g)), g, arrow=str(g).upper()) for g in (G1, G2, G3, G4)],
        uuid.UUID, "large_string", (uuid.UUID, 36, 36, 0), pa_in=lambda pa: pa.large_string(),
        notes="Arrow carries the canonical text form",
    ),
    Case(
        "xml", "xml",
        [
            Row(Q("xml", "<a/>"), "<a/>"),
            Row(C("xml", "N'<a x=''1''>téxt 日本</a>'"), '<a x="1">téxt 日本</a>'),
            Row(Q("xml", "<r><c>1</c><c>2</c></r>"), "<r><c>1</c><c>2</c></r>"),
            Row(Q("xml", "<![CDATA[<>&]]>"), "&lt;&gt;&amp;"),
        ],
        str, "large_string", (str, 0, 0, 0), pa_in=lambda pa: pa.large_string(),
        bulk_arrow=True,
    ),
]


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------
for _c in OTHER_CASES:
    if _c.id == "uniqueidentifier":
        for _r in _c.rows:
            _r.arrow_in = str(_r.py).upper()

CORE_CASES: List[Case] = (
    NUMERIC_CORE
    + [decimal_case(p, s) for p, s in DECIMAL_HEAVY]
    + money_cases()
    + FLOAT_CORE
    + DATE_CORE
    + TIME_CASES
    + DATETIME2_CASES
    + DTO_CASES
    + STRING_CASES
    + BINARY_CASES
    + OTHER_CASES
)
_extra = []
for _p, _s in DECIMAL_ALL:
    if (_p, _s) not in DECIMAL_HEAVY:
        _c = decimal_case(_p, _s)
        _c.heavy = False
        _extra.append(_c)
FULL_CASES: List[Case] = CORE_CASES + _extra
