"""Client-CPU fast paths: MAX values read straight into Arrow buffers, UTF-8 collation
text borrowed on bulk load, sliced Arrow batches. All must round-trip exactly."""

import hashlib

import pytest

pa = pytest.importorskip("pyarrow")

from mssql_python import ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="Rust-port feature",
)

UTF8 = "Latin1_General_100_CI_AS_SC_UTF8"


def _blob(size, seed):
    out = bytearray()
    block = hashlib.sha256(str(seed).encode()).digest()
    while len(out) < size:
        block = hashlib.sha256(block).digest()
        out += block
    return bytes(out[:size])


# Sizes straddle the 4096-byte packet payload and PLP chunk boundaries.
SIZES = [0, 1, 4095, 4096, 4097, 8191, 16383, 16384, 65535, 65536, 65537, 1 << 20]


def test_varbinary_max_round_trip(cursor, table_name):
    values = [_blob(size, size) for size in SIZES]
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int, v varbinary(max) NULL)")
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} VALUES (?, ?)",
        [(i, v) for i, v in enumerate(values)] + [(len(values), None)],
    )
    got = cursor.execute(f"SELECT id, v FROM dbo.{table_name} ORDER BY id").arrow()
    assert got.column("v").to_pylist() == values + [None]


def test_nvarchar_max_and_utf8_varchar_max_round_trip(cursor, table_name):
    texts = ["", "a", "Zürich 日本語 😀"] + [("日本語ab😀" * (n // 6 + 1))[:n] for n in (4095, 4097, 8191, 40000, 300000)]
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int, n nvarchar(max) NULL, "
        f"u varchar(max) COLLATE {UTF8} NULL, a varchar(max) COLLATE Latin1_General_CI_AS NULL)"
    )
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, n, u, a) VALUES (?, ?, ?, ?)",
        [(i, t, t, t if t.isascii() else None) for i, t in enumerate(texts)] + [(len(texts), None, None, None)],
    )
    got = cursor.execute(f"SELECT id, n, u, a FROM dbo.{table_name} ORDER BY id").arrow()
    assert got.column("n").to_pylist() == texts + [None]
    assert got.column("u").to_pylist() == texts + [None]
    assert got.column("a").to_pylist() == [t if t.isascii() else None for t in texts] + [None]


def test_mixed_columns_keep_row_alignment(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int, b varbinary(max), n nvarchar(max), s varchar(20), x varbinary(10))")
    rows = [(i, _blob(5000 + i, i), "é" * (3000 + i), f"s{i}", _blob(5, i)) for i in range(50)]
    cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?, ?, ?, ?, ?)", rows)
    got = cursor.execute(f"SELECT * FROM dbo.{table_name} ORDER BY id").arrow(batch_size=7)
    assert [tuple(r.values()) for r in got.to_pylist()] == rows


def test_sliced_arrow_bulk_load(cursor, table_name):
    n = 100_000
    parent = pa.RecordBatch.from_arrays(
        [pa.array(range(n), pa.int64()), pa.array([float(i) for i in range(n)])] + [pa.array(range(n), pa.int32())] * 8,
        names=["id", "f"] + [f"c{i}" for i in range(8)],
    )
    batches = [parent.slice(lo, 3000) for lo in range(0, n, 3000)]
    cols = ", ".join(f"c{i} int" for i in range(8))
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id bigint, f float, {cols})")
    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", pa.Table.from_batches(batches))
    assert result["rows_copied"] == n
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(id), SUM(f), SUM(CAST(c7 AS bigint)) FROM dbo.{table_name}")
    total = sum(range(n))
    assert tuple(cursor.fetchone()) == (n, total, float(total), total)


def test_multilingual_utf8_collation_bulk_load(cursor, table_name):
    words = ["Zürich Öl", "日本語のテキスト", "Привет мир", "plain ascii", "Ελληνικά", "emoji 😀 mix", ""]
    values = [words[i % len(words)] * (1 + i % 3) for i in range(20_000)] + [None]
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int, s varchar(400) COLLATE {UTF8} NULL)")
    table = pa.table({"id": pa.array(range(len(values)), pa.int32()), "s": pa.array(values, pa.string())})
    cursor.bulkcopy_arrow(f"dbo.{table_name}", table)
    got = cursor.execute(f"SELECT s FROM dbo.{table_name} ORDER BY id").arrow()
    assert got.column("s").to_pylist() == values
