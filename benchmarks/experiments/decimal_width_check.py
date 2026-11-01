"""Bulk-load decimal values at the shortest legal wire width and verify exact storage."""
import os
from decimal import Decimal, getcontext

getcontext().prec = 60

import pyarrow as pa

from mssql_python import connect

U = [0, 1, 99, 100, 10**9 - 1, 10**9, 10**18 - 1, 10**18, 2**32 - 1, 2**32, 2**64 - 1, 2**64,
     2**96 - 1, 2**96, 10**38 - 1]
UNSCALED = {v for u in U for v in (u, -u)} | {10**p + d for p in (9, 18, 28) for d in (-1, 0, 1)}


def main(cs):
    conn = connect(cs, autocommit=True)
    cur = conn.cursor()
    for prec, scale in ((38, 2), (38, 0), (20, 2), (12, 4), (9, 2), (28, 10)):
        lim = 10**prec - 1
        vals = sorted({v for v in UNSCALED if abs(v) <= lim} | {lim, -lim})
        cur.execute("DROP TABLE IF EXISTS dbo.dw")
        cur.execute(f"CREATE TABLE dbo.dw (i int, d decimal({prec},{scale}) NULL)")
        ids = list(range(len(vals) + 2))
        exp = [format(Decimal(v).scaleb(-scale).quantize(Decimal(1).scaleb(-scale)), "f") for v in vals] + [None, None]
        col = pa.array([Decimal(v).scaleb(-scale) for v in vals] + [None, None], pa.decimal128(prec, scale))
        t = pa.table({"i": pa.array(ids, pa.int32()), "d": col})
        cur.bulkcopy_arrow("dbo.dw", t, timeout=0)
        cur.execute("SELECT i, CAST(d AS varchar(60)) FROM dbo.dw ORDER BY i")
        got = [r[1] for r in cur.fetchall()]
        bad = [(e, g) for e, g in zip(exp, got) if e != g]
        cur.execute("TRUNCATE TABLE dbo.dw")
        rows = [(i, None if e is None else Decimal(e)) for i, e in zip(ids, exp)]
        cur.bulkcopy("dbo.dw", rows, timeout=0)
        cur.execute("SELECT i, CAST(d AS varchar(60)) FROM dbo.dw ORDER BY i")
        got2 = [r[1] for r in cur.fetchall()]
        bad2 = [(e, g) for e, g in zip(exp, got2) if e != g]
        print(f"decimal({prec},{scale}): {len(vals)} values + 2 NULLs, arrow mismatches={len(bad)} rows mismatches={len(bad2)}", bad[:3], bad2[:3], flush=True)
        cur.execute("DROP TABLE dbo.dw")
    conn.close()


main(os.environ["DB_CONNECTION_STRING"])
