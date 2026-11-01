"""G2 microbenchmarks: python benchmarks/experiments/g2_micro.py <case> [repeats]
Needs DB_CONNECTION_STRING (see scripts/run_tests.py for how the env is loaded)."""
import datetime as dt
import decimal
import os
import statistics
import sys
import time
import uuid

import mssql_python

D = decimal.Decimal
cs = os.environ["DB_CONNECTION_STRING"]


def timed(fn, repeats):
    fn()
    out = []
    for _ in range(repeats):
        t = time.perf_counter(); c0 = time.process_time()
        fn()
        out.append((time.perf_counter() - t, time.process_time() - c0))
    return statistics.median(o[0] for o in out), statistics.median(o[1] for o in out)


def main():
    case = sys.argv[1]
    rep = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    c = mssql_python.connect(cs, autocommit=True)
    cur = c.cursor()
    tz = dt.timezone(dt.timedelta(hours=2))
    try:
        if case == "mixed4":
            p = (dt.time(1, 2, 3, 4), dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=tz), uuid.uuid4(), dt.date(2020, 1, 2))

            def f():
                for _ in range(20000):
                    cur.execute("SELECT ?,?,?,?", p)
        elif case in ("time", "aware", "uuid", "date"):
            v = {"time": dt.time(1, 2, 3, 4), "aware": dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=tz),
                 "uuid": uuid.uuid4(), "date": dt.date(2020, 1, 2)}[case]

            def f():
                for _ in range(20000):
                    cur.execute("SELECT ?", (v,))
        elif case == "hinted":
            def f():
                for _ in range(20000):
                    cur.setinputsizes([(mssql_python.SQL_DECIMAL, 10, 2)])
                    cur.execute("SELECT ?", (D("1.23456"),))
        elif case == "em":
            cur.execute("IF OBJECT_ID('g2_em') IS NOT NULL DROP TABLE g2_em")
            cur.execute("CREATE TABLE g2_em (t TIME(7), a DATETIMEOFFSET(7), n DECIMAL(38,6))")
            n = 20000
            tz1 = dt.timezone(dt.timedelta(hours=1))
            rows = [(dt.time(i % 24, i % 60, i % 60, i), dt.datetime(2020, 1, 1 + i % 28, i % 24, 0, 0, tzinfo=tz1),
                     (i if i % 2 else D(i) / 100)) for i in range(n)]

            def f():
                cur.execute("TRUNCATE TABLE g2_em")
                cur.executemany("INSERT INTO g2_em VALUES (?,?,?)", rows)
        elif case == "cache":
            cur.execute("IF OBJECT_ID('g2_t') IS NULL CREATE TABLE g2_t (a INT)")
            sqls = [f"SELECT a FROM g2_t WHERE a = ? AND {i} = {i}" for i in range(600)]

            def f():
                for s in sqls:
                    cur.execute(s, (None,))
                for s in sqls:
                    cur.execute(s, (None,))
        else:
            raise SystemExit("unknown case")
        med, mn = timed(f, rep)
        print(f"{case}\tmedian={med:.4f}\tcpu={mn:.4f}")
    finally:
        try:
            cur.execute("IF OBJECT_ID('g2_em') IS NOT NULL DROP TABLE g2_em")
            cur.execute("IF OBJECT_ID('g2_t') IS NOT NULL DROP TABLE g2_t")
        except Exception:
            pass
        c.close()


main()
