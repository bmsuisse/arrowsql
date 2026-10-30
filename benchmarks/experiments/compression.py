"""Does compression help moving SQL Server data? (upstream wheel, Arrow fetch)

    benchmarks/experiments/run_py.sh compression.py setup
    benchmarks/experiments/run_py.sh compression.py read [--port 14330] [--label 100mbit]

Table perf_research.dbo.docs: 100k JSON documents (one per taxi trip, ~600
chars) stored as nvarchar(max), as UTF-8-collated varchar(max), and
pre-compressed with COMPRESS() (GZIP of the UTF-16 bytes).

Arms (each fetches all 100k docs as Python str / decoded text):
  nvarchar        plain UTF-16 on the wire
  utf8_varchar    UTF-8 collation: ~half the bytes for ASCII-heavy text
  compress_live   SELECT COMPRESS(doc): server gzips per row at query time
  compress_stored SELECT doc_gz: pre-compressed column
Client-side GZIP decompression runs on a thread pool (zlib releases the GIL).
Run through wan_proxy.py with --mbit/--rtt-ms to simulate a WAN link.
"""

import argparse
import json
import os
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

from mssql_python import connect


def cs(port: int) -> str:
    s = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=perf_research")
    return s.replace("localhost,1433", f"127.0.0.1,{port}") if port != 1433 else s


def gunzip_utf16(blobs):
    # COMPRESS() output is a gzip stream; wbits=31 selects the gzip container.
    return [zlib.decompress(b, 31).decode("utf-16-le") for b in blobs]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["setup", "read"])
    ap.add_argument("--port", type=int, default=1433)
    ap.add_argument("--label", default="loopback")
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--skip", nargs="*", default=[])
    args = ap.parse_args()
    conn = connect(cs(args.port), autocommit=True)
    cur = conn.cursor()

    if args.mode == "setup":
        cur.execute("DROP TABLE IF EXISTS dbo.docs")
        cur.execute(
            f"SELECT TOP ({args.rows}) t.trip_id, "
            "CAST((SELECT t.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS nvarchar(max)) AS doc "
            "INTO dbo.docs FROM nyctaxi.dbo.yellow_trips t ORDER BY t.trip_id"
        )
        cur.execute("ALTER TABLE dbo.docs ADD doc_utf8 varchar(max) COLLATE Latin1_General_100_CI_AS_SC_UTF8, doc_gz varbinary(max)")
        cur.execute("UPDATE dbo.docs SET doc_utf8 = doc, doc_gz = COMPRESS(doc)")
        cur.execute("SELECT SUM(DATALENGTH(doc)), SUM(DATALENGTH(doc_utf8)), SUM(DATALENGTH(doc_gz)) FROM dbo.docs")
        a, b, c = cur.fetchone()
        print(json.dumps({"setup_rows": args.rows, "nvarchar_mb": round(a / 1e6, 1),
                          "utf8_mb": round(b / 1e6, 1), "gzip_mb": round(c / 1e6, 1)}))
        return

    pool = ThreadPoolExecutor(8)

    def fetch(sql):
        cur.execute(sql)
        return cur.arrow(batch_size=65536).column(0).to_pylist()

    def decompress_parallel(blobs):
        chunk = max(1, len(blobs) // 32)
        parts = [blobs[i:i + chunk] for i in range(0, len(blobs), chunk)]
        return [s for part in pool.map(gunzip_utf16, parts) for s in part]

    arms = {
        "nvarchar": lambda: fetch("SELECT doc FROM dbo.docs"),
        "utf8_varchar": lambda: fetch("SELECT doc_utf8 FROM dbo.docs"),
        "compress_live": lambda: decompress_parallel(fetch("SELECT COMPRESS(doc) FROM dbo.docs")),
        "compress_stored": lambda: decompress_parallel(fetch("SELECT doc_gz FROM dbo.docs")),
    }
    for name, fn in arms.items():
        if name in args.skip:
            continue
        fn()
        samples = []
        for _ in range(3):
            t0 = time.perf_counter()
            docs = fn()
            samples.append(time.perf_counter() - t0)
        assert len(docs) == args.rows and docs[0].startswith("{")
        print(json.dumps({"exp": "compression", "link": args.label, "arm": name,
                          "best_s": round(min(samples), 3)}), flush=True)


if __name__ == "__main__":
    main()
