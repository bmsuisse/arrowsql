"""Workload matrix. Each workload is (name, category, required capabilities, run function).

Naming: <category>.<dataset-or-op>.<variant>; categories are read / write / latency.
`run(driver, ctx)` is the timed region and returns the number of units processed
(rows, queries, ops). `setup` runs untimed before every repetition, `prepare` once
before the warm-up, `verify` once after the last repetition, `teardown` last.

--quick shrinks the big datasets (3M -> 300k rows, 1M -> 100k, ...) and loop counts;
workload names stay the same, the record carries quick=true. Everything here is
stdlib-only at import time so run.py can enumerate it from any interpreter.
"""

from __future__ import annotations

import fnmatch
import random
from dataclasses import dataclass, field
from typing import Callable, Optional

from datasets import DATASETS
from drivers.base import Unsupported
from payload import SMALL_DDL, SMALL_INSERT, TaxiPayload, small_rows, taxi_target_ddl

BULK_DEFAULT_BATCH = 100_000
POINT_IDS_MAX = 2_000_000


@dataclass
class Ctx:
    quick: bool
    datasets_info: dict
    cache: dict = field(default_factory=dict)

    def table(self, d) -> str:
        return f"dbo.bench_w_{d.name}"

    def payload(self, n: int) -> TaxiPayload:
        return self.cache.setdefault(("payload", n), TaxiPayload(n))


@dataclass
class Workload:
    name: str
    category: str
    requires: frozenset
    run: Callable
    dataset: Optional[str] = None  # key into datasets.json for the MB/s estimate (bytes per unit)
    unit: str = "rows"
    setup: Optional[Callable] = None
    prepare: Optional[Callable] = None
    verify: Optional[Callable] = None
    teardown: Optional[Callable] = None
    repeats: int = 5
    quick_repeats: int = 3
    warmup: int = 1
    rows_hint: int = 0  # rows the driver must hold at once (memory guard for drivers without streaming)
    pooled: bool = False
    note: str = ""


WORKLOADS: list[Workload] = []


def _add(w: Workload) -> Workload:
    WORKLOADS.append(w)
    return w


def get(name: str) -> Workload:
    return next(w for w in WORKLOADS if w.name == name)


def select(patterns: list[str] | None) -> list[Workload]:
    if not patterns:
        return list(WORKLOADS)
    return [w for w in WORKLOADS if any(fnmatch.fnmatch(w.name, p) for p in patterns)]


def _check_rows(d, n):
    cap = getattr(d, "max_fetch_rows", None)
    if cap is not None and n > cap:
        raise Unsupported(f"{d.name} materializes whole results (limit {cap} rows)")


# --------------------------------------------------------------------------- READ

def _read_modes():
    heavy = dict(repeats=3, quick_repeats=2)
    # (dataset, modes) ; modes: fetchall / fetchmany / fetchone / arrow / duckdb
    plan = [
        ("taxi_100k", ["fetchall", "fetchmany", "fetchone", "arrow"], {}),
        ("taxi_narrow", ["fetchmany", "arrow"], heavy),
        ("taxi_all", ["fetchmany", "arrow", "duckdb"], heavy),
        ("strings", ["fetchmany", "arrow"], heavy),
        ("decimals", ["fetchmany", "arrow"], heavy),
        ("datetimes", ["fetchmany", "arrow"], heavy),
        ("lob", ["fetchmany", "arrow"], heavy),
        ("wide", ["fetchmany", "arrow"], heavy),
    ]
    labels = {"taxi_narrow": "narrow_3m", "taxi_all": "all_3m"}
    for ds_name, modes, kw in plan:
        ds = DATASETS[ds_name]
        label = labels.get(ds_name, ds_name)
        for mode in modes:
            def run(d, ctx, ds=ds, mode=mode):
                sql = ds.sql(ctx.quick)
                if mode in ("fetchall", "fetchmany", "fetchone"):
                    _check_rows(d, ds.rows(ctx.quick))
                    return d.fetch_rows(sql, mode=mode)
                if mode == "arrow":
                    return d.fetch_arrow(sql)
                return d.fetch_arrow_duckdb(sql)

            req = {"fetchall": "rows", "fetchmany": "rows", "fetchone": "rows",
                   "arrow": "arrow", "duckdb": "duckdb"}[mode]
            _add(Workload(f"read.{label}.{mode}", "read", frozenset({req}), run, dataset=ds_name, **kw))
        # bcp native export: server-local speed-of-light for the same query
        def export(d, ctx, ds=ds):
            return d.export_native(ds.sql(ctx.quick))

        _add(Workload(f"read.{label}.export", "read", frozenset({"export"}), export, dataset=ds_name, **heavy))

    ds = DATASETS["taxi_all"]
    for parts in (2, 4, 8):
        def run(d, ctx, parts=parts):
            return d.fetch_arrow_partitioned(ds.sql(ctx.quick), "trip_id", parts)

        _add(Workload(f"read.all_3m.partitioned_{parts}", "read", frozenset({"partitioned"}), run,
                      dataset="taxi_all", repeats=3, quick_repeats=2))


def _read_small():
    def point(d, ctx):
        n = 200 if ctx.quick else 1000
        return d.point_lookups(range(1, n + 1))

    _add(Workload("read.point_lookup_x1000", "read", frozenset({"rows"}), point, dataset="taxi_all",
                  unit="queries", repeats=5, quick_repeats=3))

    def small(d, ctx):
        reps = 50 if ctx.quick else 200
        n = 0
        for _ in range(reps):
            n += d.fetch_rows("SELECT TOP 100 * FROM dbo.yellow_trips", mode="fetchall")
        return n

    _add(Workload("read.small_100_rows_x200", "read", frozenset({"rows"}), small, dataset="taxi_all"))


# ------------------------------------------------------------------------ LATENCY

def _latency():
    def connect_close(d, ctx):
        return d.connect_close(10 if ctx.quick else 50)

    _add(Workload("latency.connect_close_x50", "latency", frozenset({"connect"}), connect_close, unit="connects"))

    def pooled(d, ctx):
        return d.pooled_connect_close(10 if ctx.quick else 50)

    _add(Workload("latency.pooled_connect_x50", "latency", frozenset({"pool"}), pooled, unit="connects",
                  pooled=True))

    def select1(d, ctx):
        return d.select1(1000 if ctx.quick else 5000)

    _add(Workload("latency.select1_x5000", "latency", frozenset({"select1"}), select1, unit="queries"))

    for threads in (8, 16):
        def setup(d, ctx, threads=threads):
            d.close_workers()
            d.open_workers(threads)

        def run(d, ctx, threads=threads):
            per = 100 if ctx.quick else 500
            rng = random.Random(7)
            ids = [[rng.randrange(1, 2_900_000) for _ in range(per)] for _ in range(threads)]
            return d.run_workers(ids)

        def teardown(d, ctx):
            d.close_workers()

        _add(Workload(f"latency.threads{threads}_point_qps", "latency", frozenset({"threads"}), run,
                      dataset="taxi_all", unit="queries", setup=setup, teardown=teardown))

    def mem_rows(d, ctx):
        sql = DATASETS["taxi_narrow"].sql(ctx.quick)
        _check_rows(d, DATASETS["taxi_narrow"].rows(ctx.quick))
        return d.fetch_rows_retained(sql)

    _add(Workload("latency.mem_fetchall_narrow_3m", "latency", frozenset({"rows"}), mem_rows,
                  dataset="taxi_narrow", repeats=1, quick_repeats=1, warmup=0,
                  note="single fetchall() retaining all rows; see peak RSS"))

    def mem_arrow(d, ctx):
        return d.fetch_arrow(DATASETS["taxi_all"].sql(ctx.quick))

    _add(Workload("latency.mem_arrow_all_3m", "latency", frozenset({"arrow"}), mem_arrow,
                  dataset="taxi_all", repeats=1, quick_repeats=1, warmup=0,
                  note="single full Arrow materialization; see peak RSS"))


# -------------------------------------------------------------------------- WRITE

def _small_table(name_suffix=""):
    def setup(d, ctx):
        t = ctx.table(d)
        d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")
        d.execute(SMALL_DDL.format(t=t))

    def verify(d, ctx, n):
        got = int(d.scalar(f"SELECT COUNT_BIG(*) FROM {ctx.table(d)}"))
        if got != n:
            raise RuntimeError(f"target has {got} rows, expected {n}")

    def teardown(d, ctx):
        t = ctx.table(d)
        d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")

    return dict(setup=setup, verify=verify, teardown=teardown)


def _writes_small():
    hooks = _small_table()

    def single(d, ctx):
        rows = small_rows(1000)
        return d.insert_single(SMALL_INSERT.format(t=ctx.table(d)), rows)

    _add(Workload("write.single_insert_x1000", "write", frozenset({"insert"}), single, unit="rows",
                  note="autocommit off, one commit at the end", **hooks))

    for label, n, qn in (("10k", 10_000, 10_000), ("100k", 100_000, 20_000)):
        def run(d, ctx, n=n, qn=qn):
            rows = ctx.cache.setdefault(("small", qn if ctx.quick else n), small_rows(qn if ctx.quick else n))
            return d.executemany(SMALL_INSERT.format(t=ctx.table(d)), rows)

        _add(Workload(f"write.executemany_{label}", "write", frozenset({"executemany"}), run,
                      repeats=3, quick_repeats=2, **hooks))

    def multirow(d, ctx):
        n = 2_000 if ctx.quick else 10_000
        rows = ctx.cache.setdefault(("small", n), small_rows(n))
        return d.insert_multirow(ctx.table(d), rows, 200)

    _add(Workload("write.multirow_values_10k", "write", frozenset({"multirow"}), multirow,
                  note="200 rows / statement, 5 params each", **hooks))


def _writes_bulk():
    def make(kind, size_label, n_full, n_quick, layout, tablock, batch, batch_label, stream=False):
        base = f"write.bulk_{kind}_{size_label}.{layout}.{'tablock' if tablock else 'notablock'}"
        if batch_label:
            base += f".{batch_label}"
        if stream:
            base += ".stream"

        def n_of(ctx):
            return n_quick if ctx.quick else n_full

        def setup(d, ctx):
            t = ctx.table(d)
            d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")
            d.execute(taxi_target_ddl(t, layout))

        def prepare(d, ctx):
            p = ctx.payload(n_of(ctx))
            d.prepare_payload(p)
            if d.name != "bcp" and kind == "arrow":
                p.arrow()  # parquet read + casts happen outside the timed region
            if d.name != "bcp" and kind == "rows" and not stream:
                p.materialize_rows()  # Arrow -> tuple boxing stays out of the timed region (bcp stages a file)

        def run(d, ctx):
            p = ctx.payload(n_of(ctx))
            fn = d.bulk_insert_rows if kind == "rows" else d.bulk_insert_arrow
            return fn(ctx.table(d), p, batch, tablock)

        def verify(d, ctx, n):
            got = int(d.scalar(f"SELECT COUNT_BIG(*) FROM {ctx.table(d)}"))
            if got != n:
                raise RuntimeError(f"target has {got} rows, expected {n}")

        def teardown(d, ctx):
            t = ctx.table(d)
            d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")

        req = {"bulk_rows" if kind == "rows" else "bulk_arrow"} | ({"tablock"} if tablock else set())
        _add(Workload(base, "write", frozenset(req), run, dataset="taxi_all", setup=setup, prepare=prepare,
                      verify=verify, teardown=teardown, repeats=3, quick_repeats=2,
                      note="rows boxed lazily inside the timed call (old behaviour)" if stream else ""))

    D = BULK_DEFAULT_BATCH
    for kind in ("rows", "arrow"):
        if kind == "rows":
            main_label, n_full, n_quick = "100k", 100_000, 100_000
            variants = [("heap", True, D, ""), ("heap", False, D, ""), ("pk", True, D, ""), ("cci", True, D, ""),
                        ("heap", True, 1_000, "b1k"), ("heap", True, 10_000, "b10k"), ("heap", True, 0, "b0")]
        else:
            main_label, n_full, n_quick = "1m", 1_000_000, 250_000
            variants = [("heap", True, D, ""), ("heap", False, D, ""), ("pk", True, D, ""), ("cci", True, D, ""),
                        ("heap", True, 10_000, "b10k"), ("heap", True, 0, "b0")]
        for layout, tablock, batch, bl in variants:
            make(kind, main_label, n_full, n_quick, layout, tablock, batch, bl)
        if kind == "rows":
            make("rows", "1m", 1_000_000, 250_000, "heap", True, D, "")
            make("rows", "100k", 100_000, 100_000, "heap", True, D, "", stream=True)
            make("rows", "1m", 1_000_000, 250_000, "heap", True, D, "", stream=True)
        else:
            make("arrow", "3m", 2_964_624, 750_000, "heap", True, D, "")


def _bcp_fixed_cost():
    def run(d, ctx):
        return d.noop()

    _add(Workload("write.bcp_fixed_cost.noop", "write", frozenset({"export"}), run, unit="calls",
                  repeats=10, quick_repeats=5,
                  note="one `bcp SELECT 1 queryout`: podman exec + process start + login; subtract from bcp loads"))


_read_modes()
_read_small()
_latency()
_writes_small()
_writes_bulk()
_bcp_fixed_cost()


# ------------------------------------------------- PARALLEL WRITE (rust_port only)

_PAR_TABLE = "dbo.bench_wpar_taxi_3m"


def _writes_parallel():
    """mssql_python.write_arrow: N connections, each one TABLOCK bulk copy into a heap.

    Compare with write.bulk_arrow_3m.heap.tablock (one stream, upstream bulkcopy_arrow)
    and with the bcp CLI on the same table and data.
    """
    n_full, n_quick = 2_964_624, 750_000
    # Own table: the bench_w_* tables are shared with the single-stream write workloads,
    # and TABLOCK loads from concurrent runs on the same table block each other.

    def conn_str(d):
        i = d.info
        return (f"Server={i.host},{i.port};Database={i.database};Uid={i.user};Pwd={i.password};"
                "Encrypt=yes;TrustServerCertificate=yes")

    for workers in (1, 2, 4, 8):
        def setup(d, ctx):
            t = _PAR_TABLE
            d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")
            d.execute(taxi_target_ddl(t, "heap"))

        def prepare(d, ctx):
            if d.name != "rust_port":
                raise Unsupported("write_arrow is the Rust-port parallel loader")
            ctx.payload(n_quick if ctx.quick else n_full).arrow()  # parquet read + casts stay untimed

        def run(d, ctx, workers=workers):
            if d.name != "rust_port":
                raise Unsupported("write_arrow is the Rust-port parallel loader")
            from mssql_python import write_arrow

            p = ctx.payload(n_quick if ctx.quick else n_full)
            result = write_arrow(conn_str(d), _PAR_TABLE, p.arrow(), workers=workers, table_lock=True)
            return result["rows_copied"]

        def verify(d, ctx, n):
            got = int(d.scalar(f"SELECT COUNT_BIG(*) FROM {_PAR_TABLE}"))
            if got != n:
                raise RuntimeError(f"target has {got} rows, expected {n}")

        def teardown(d, ctx):
            t = _PAR_TABLE
            d.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}")

        _add(Workload(f"write.parallel_arrow_3m.w{workers}", "write",
                      frozenset({"bulk_arrow", "tablock"}), run, dataset="taxi_all", setup=setup,
                      prepare=prepare, verify=verify, teardown=teardown, repeats=5, quick_repeats=3,
                      note=f"write_arrow, workers={workers}, batch 0 (one batch per stream), heap TABLOCK"))


_writes_parallel()
