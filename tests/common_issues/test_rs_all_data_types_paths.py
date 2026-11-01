"""Systematic data-type matrix: every catalogued SQL Server type x every API path.

Catalog: ``rs_dtypes.py``. Paths: row fetch, cursor.description, parameter
binding, executemany, Arrow fetch (fetchall_arrow / arrow / execute_arrow /
read_arrow with utf8_strings on and off), bulkcopy (rows), bulkcopy_arrow.
Types with special handling (sql_variant, spatial, rowversion, ...) live in
test_rs_all_data_types_special.py; async in test_rs_all_data_types_aio.py.
"""

import datetime as dt
import uuid

import pytest

from mssql_python import connect
from mssql_python import ddbc_bindings

from rs_dtypes import CORE_CASES, FULL_CASES, Case, same

pa = pytest.importorskip("pyarrow")

RUST = getattr(ddbc_bindings, "__rust_port__", False)

cheap = [pytest.param(c, id=c.id) for c in FULL_CASES]
core = [pytest.param(c, id=c.id) for c in CORE_CASES]


@pytest.fixture
def ac(conn_str):
    c = connect(conn_str, autocommit=True)
    yield c
    c.close()


@pytest.fixture
def mk(ac):
    """Create ``##rs_dt_*(id int NOT NULL, v <ddl> NULL)``; dropped on teardown."""
    made = []
    cur = ac.cursor()

    def make(case: Case) -> str:
        name = f"##rs_dt_{uuid.uuid4().hex[:12]}"
        cur.execute(f"CREATE TABLE {name} (id int NOT NULL, v {case.ddl} NULL)")
        made.append(name)
        return name

    yield make
    for n in made:
        cur.execute(f"DROP TABLE IF EXISTS {n}")
    cur.close()


def load_literals(cur, tbl, case):
    rows = case.all_rows()
    for i, r in enumerate(rows):
        cur.execute(f"INSERT INTO {tbl} VALUES ({i}, {r.s})")
    return rows


def check(got, row, case, what=""):
    assert same(got, row.py), f"{case.id} {what}: got {got!r} ({type(got).__name__}), want {row.py!r}"


def read_back(cur, tbl):
    cur.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
    return [tuple(r) for r in cur.fetchall()]


def check_table(rows, case, expected_rows=None, what=""):
    exp = expected_rows or case.all_rows()
    assert [r[0] for r in rows] == list(range(len(exp)))
    for (_, got), row in zip(rows, exp):
        check(got, row, case, what)


# ---------------------------------------------------------------- 1. fetch
@pytest.mark.parametrize("case", cheap)
def test_fetch_all_one_many(ac, mk, case):
    cur = ac.cursor()
    tbl = mk(case)
    rows = load_literals(cur, tbl, case)
    cur.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
    got = cur.fetchall()
    assert len(got) == len(rows)
    for (i, v), row in zip(got, rows):
        check(v, row, case, "fetchall")
    # fetchone
    cur.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
    for row in rows:
        r = cur.fetchone()
        check(r[1], row, case, "fetchone")
        check(r.v, row, case, "row attribute")
    assert cur.fetchone() is None
    # fetchmany in chunks of 2
    cur.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
    out = []
    while True:
        part = cur.fetchmany(2)
        if not part:
            break
        assert len(part) <= 2
        out.extend(part)
    assert len(out) == len(rows)
    for r, row in zip(out, rows):
        check(r[1], row, case, "fetchmany")
    cur.close()


# ---------------------------------------------------------- 2. description
@pytest.mark.parametrize("case", cheap)
def test_description(ac, mk, case):
    cur = ac.cursor()
    tbl = mk(case)
    cur.execute(f"SELECT v AS col, CAST(1 AS int) AS c2 FROM {tbl}")
    name, type_code, display, internal, prec, scale, null_ok = cur.description[0]
    assert name == "col"
    assert (type_code, internal, prec, scale) == case.desc, case.id
    assert display is None
    assert null_ok is True
    assert cur.description[1][1] is int
    cur.close()


@pytest.mark.parametrize("case", [pytest.param(c, id=c.id) for c in CORE_CASES if c.id in ("int", "decimal_10_0", "nvarchar_50", "varbinary_20", "datetime2_3")])
def test_description_not_null(ac, case):
    cur = ac.cursor()
    cur.execute(f"CREATE TABLE #nn (v {case.ddl} NOT NULL, w {case.ddl} NULL)")
    cur.execute("SELECT v, w FROM #nn")
    assert [d[6] for d in cur.description] == [False, True]
    cur.execute("DROP TABLE #nn")
    cur.close()


# ------------------------------------------------------ 3. parameter binding
@pytest.mark.parametrize("case", cheap)
def test_param_insert(ac, mk, case):
    if not case.bind_params:
        pytest.skip("not bindable")
    cur = ac.cursor()
    tbl = mk(case)
    rows = case.all_rows()
    for i, r in enumerate(rows):
        cur.execute(f"INSERT INTO {tbl} VALUES (?, ?)", (i, r.bind_value))
    check_table(read_back(cur, tbl), case, what="param")
    cur.close()


# ------------------------------------------------------------ 4. executemany
@pytest.mark.parametrize("case", core)
@pytest.mark.parametrize("null_first", [False, True], ids=["null_last", "null_first"])
def test_executemany(ac, mk, case, null_first):
    if not case.bind_params:
        pytest.skip("not bindable")
    cur = ac.cursor()
    tbl = mk(case)
    rows = case.all_rows()
    if null_first:
        rows = rows[-1:] + rows[:-1]
    cur.executemany(f"INSERT INTO {tbl} VALUES (?, ?)", [(i, r.bind_value) for i, r in enumerate(rows)])
    check_table(read_back(cur, tbl), case, rows, "executemany")
    cur.close()


# ------------------------------------------------------------------ 5. Arrow
def arrow_values(col, case):
    if case.arrow_int:
        t = col.type
        target = pa.int32() if pa.types.is_date32(t) else pa.int64()
        col = col.cast(target)
    return col.to_pylist()


def arrow_table(ac, conn_str, mode, tbl, cur):
    sql = f"SELECT id, v FROM {tbl}"
    if mode == "fetchall_arrow":
        cur.execute(sql)
        return pa.table(cur.fetchall_arrow())
    if mode == "arrow":
        cur.execute(sql)
        return cur.arrow()
    if mode == "fetchmany_arrow":
        cur.execute(sql)
        parts = []
        while True:
            t = cur.fetchmany_arrow(2)
            if t.num_rows == 0:
                break
            parts.append(pa.table(t))
        return pa.concat_tables(parts)
    if mode == "execute_arrow_utf8":
        return pa.table(cur.execute_arrow(sql, utf8_strings=True))
    if mode == "execute_arrow_plain":
        return pa.table(cur.execute_arrow(sql, utf8_strings=False))
    from mssql_python import read_arrow

    return pa.table(read_arrow(conn_str, sql, utf8_strings=(mode == "read_arrow_utf8")))


ARROW_MODES = ["fetchall_arrow", "arrow", "fetchmany_arrow", "execute_arrow_utf8", "execute_arrow_plain", "read_arrow_utf8", "read_arrow_plain"]


@pytest.mark.parametrize("mode", ARROW_MODES)
@pytest.mark.parametrize("case", cheap)
def test_arrow_fetch(ac, conn_str, mk, case, mode):
    if mode in ("fetchmany_arrow", "execute_arrow_plain") and not case.heavy:
        pytest.skip("covered by heavy decimals")
    cur = ac.cursor()
    tbl = mk(case)
    rows = load_literals(cur, tbl, case)
    t = arrow_table(ac, conn_str, mode, tbl, cur)
    assert str(t.schema.types[1]) == case.arrow_type, f"{case.id} {mode}"
    got = sorted(zip(t.column(0).to_pylist(), arrow_values(t.column(1), case)))
    assert [g[0] for g in got] == list(range(len(rows)))
    for (_, v), row in zip(got, rows):
        want = row.arrow_value
        assert same(v, want) or v == want, f"{case.id} {mode}: got {v!r} want {want!r}"
    cur.close()


# ------------------------------------------------------------- 6. bulk copy
def to_arrow(case, rows):
    ids = pa.array(range(len(rows)), pa.int32())
    vals = [r.arrow_in_value for r in rows]
    return pa.table({"id": ids, "v": pa.array(vals, type=case.pa_in(pa))})


@pytest.mark.skipif(not RUST, reason="native bulk copy is a Rust-port feature")
@pytest.mark.parametrize("case", core)
def test_bulkcopy_rows(ac, mk, case):
    if not case.bulk_rows:
        pytest.skip("unsupported")
    cur = ac.cursor()
    tbl = mk(case)
    rows = case.all_rows()
    res = cur.bulkcopy(tbl, [(i, r.bind_value) for i, r in enumerate(rows)])
    assert res["rows_copied"] == len(rows)
    check_table(read_back(cur, tbl), case, what="bulkcopy")
    cur.close()


@pytest.mark.skipif(not RUST, reason="native bulk copy is a Rust-port feature")
@pytest.mark.parametrize("case", core)
def test_bulkcopy_arrow(ac, mk, case):
    if not case.bulk_arrow:
        pytest.skip("unsupported")
    cur = ac.cursor()
    tbl = mk(case)
    rows = case.all_rows()
    res = cur.bulkcopy_arrow(tbl, to_arrow(case, rows))
    assert res["rows_copied"] == len(rows)
    got = read_back(cur, tbl)
    if case.id.startswith("datetimeoffset"):
        # Arrow timestamps carry no offset: the instant is kept, stored as +00:00.
        for (_, v), row in zip(got, rows):
            if row.py is None:
                assert v is None
            else:
                assert v == row.py and v.utcoffset() == dt.timedelta(0), case.id
        return
    check_table(got, case, what="bulkcopy_arrow")
    cur.close()
