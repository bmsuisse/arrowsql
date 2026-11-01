"""Async (arrowsql.aio) fetch / bind / Arrow for the whole type catalog."""

import uuid

import pytest

pytest_asyncio = pytest.importorskip("pytest_asyncio")
pa = pytest.importorskip("pyarrow")
aio = pytest.importorskip("mssql_python.aio")

from mssql_python import ddbc_bindings as _ddbc  # noqa: E402

from rs_dtypes import CORE_CASES, same  # noqa: E402

pytestmark = pytest.mark.skipif(not hasattr(_ddbc, "AioHub"), reason="aio is Unix-only")


@pytest_asyncio.fixture
async def aconn(conn_str):
    c = await aio.connect(conn_str, autocommit=True)
    yield c
    await c.close()


cases = [pytest.param(c, id=c.id) for c in CORE_CASES]


async def make(conn, case):
    name = f"##rs_aio_{uuid.uuid4().hex[:12]}"
    await conn.execute(f"CREATE TABLE {name} (id int NOT NULL, v {case.ddl} NULL)")
    return name


@pytest.mark.asyncio
@pytest.mark.parametrize("case", cases)
async def test_aio_fetch_description_arrow(aconn, case):
    tbl = await make(aconn, case)
    try:
        rows = case.all_rows()
        for i, r in enumerate(rows):
            await aconn.execute(f"INSERT INTO {tbl} VALUES ({i}, {r.s})")
        cur = await aconn.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
        d = cur.description[1]
        assert (d[1], d[3], d[4], d[5]) == case.desc
        got = await cur.fetchall()
        assert len(got) == len(rows)
        for (_, v), row in zip(got, rows):
            assert same(v, row.py), f"{case.id}: {v!r} != {row.py!r}"
        cur = await aconn.execute(f"SELECT id, v FROM {tbl} ORDER BY id")
        first = await cur.fetchone()
        assert same(first[1], rows[0].py)
        rest = await cur.fetchmany(2)
        assert len(rest) == min(2, len(rows) - 1)
        cur = await aconn.execute(f"SELECT id, v FROM {tbl}")
        t = pa.table(await cur.fetchall_arrow())
        assert str(t.schema.types[1]) == case.arrow_type
        col = t.column(1)
        if case.arrow_int:
            col = col.cast(pa.int32() if pa.types.is_date32(col.type) else pa.int64())
        by_id = dict(zip(t.column(0).to_pylist(), col.to_pylist()))
        for i, row in enumerate(rows):
            want = row.arrow_value
            assert same(by_id[i], want) or by_id[i] == want, f"{case.id} arrow row {i}"
    finally:
        await aconn.execute(f"DROP TABLE {tbl}")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", cases)
async def test_aio_param_insert_and_executemany(aconn, case):
    tbl = await make(aconn, case)
    try:
        rows = case.all_rows()
        for i, r in enumerate(rows):
            await aconn.execute(f"INSERT INTO {tbl} VALUES (?, ?)", i, r.bind_value)
        got = [tuple(r) for r in await (await aconn.execute(f"SELECT id, v FROM {tbl} ORDER BY id")).fetchall()]
        for (_, v), row in zip(got, rows):
            assert same(v, row.py), f"{case.id} execute: {v!r} != {row.py!r}"
        await aconn.execute(f"DELETE FROM {tbl}")
        cur = aconn.cursor()
        await cur.executemany(f"INSERT INTO {tbl} VALUES (?, ?)", [(i, r.bind_value) for i, r in enumerate(rows)])
        got = [tuple(r) for r in await (await aconn.execute(f"SELECT id, v FROM {tbl} ORDER BY id")).fetchall()]
        for (_, v), row in zip(got, rows):
            assert same(v, row.py), f"{case.id} executemany: {v!r} != {row.py!r}"
    finally:
        await aconn.execute(f"DROP TABLE {tbl}")


@pytest.mark.asyncio
async def test_aio_sql_variant_fetch(aconn):
    cur = await aconn.execute("SELECT CAST(42 AS sql_variant), CAST(N'x' AS sql_variant), CAST(NULL AS sql_variant)")
    assert tuple(await cur.fetchone()) == (42, "x", None)


@pytest.mark.asyncio
async def test_aio_sql_variant_arrow_is_unsupported(aconn):
    cur = await aconn.execute("SELECT CAST(42 AS sql_variant)")
    with pytest.raises(Exception, match="Unsupported data type"):
        await cur.fetchall_arrow()


@pytest.mark.asyncio
async def test_aio_spatial_hierarchy_rowversion(aconn):
    cur = await aconn.execute(
        "SELECT geometry::Point(1, 2, 0), geography::Point(1, 2, 4326), hierarchyid::Parse('/1/'), @@DBTS"
    )
    row = await cur.fetchone()
    assert all(type(x) is bytes for x in row)
