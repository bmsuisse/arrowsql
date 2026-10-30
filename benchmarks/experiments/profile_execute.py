"""Where does upstream spend ~350 us on a one-round-trip point lookup?

    benchmarks/experiments/run_py.sh profile_execute.py [--n 3000]

Splits time into the native binding (ddbc_bindings.*) vs Python-layer code
via cProfile, and reports server time from sys.dm_exec_query_stats.
"""

import argparse
import cProfile
import os
import pstats
import time

from mssql_python import connect


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=3000)
    args = ap.parse_args()
    cs = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")
    conn = connect(cs, autocommit=True)
    cur = conn.cursor()
    sql = "SELECT * FROM dbo.yellow_trips WHERE trip_id = ?"

    def loop():
        for i in range(args.n):
            cur.execute(sql, (12345 + i % 7,))
            cur.fetchall()

    loop()
    t0 = time.perf_counter()
    loop()
    wall_us = (time.perf_counter() - t0) * 1e6 / args.n

    prof = cProfile.Profile()
    prof.enable()
    loop()
    prof.disable()
    stats = pstats.Stats(prof)
    native = python = 0.0
    for (filename, _, name), (_, _, tottime, _, _) in stats.stats.items():
        if filename == "~" and ("ddbc" in name or "DDBC" in name or "built-in method" in name):
            native += tottime
        else:
            python += tottime
    total = native + python
    print(f"wall per execute+fetchall: {wall_us:.0f} us (unprofiled)")
    print(f"profiled split: native/builtins {native / total:.0%}, python-layer {python / total:.0%}")
    stats.sort_stats("tottime").print_stats(18)

    cur.execute(
        "SELECT total_worker_time / execution_count AS cpu_us, total_elapsed_time / execution_count AS elapsed_us "
        "FROM sys.dm_exec_query_stats s CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) t "
        "WHERE t.text LIKE '%yellow_trips WHERE trip_id = @P1%'"
    )
    print("server per execution (cpu_us, elapsed_us):", cur.fetchall())


if __name__ == "__main__":
    main()
