"""Userspace TCP proxy that simulates a WAN link and counts bytes.

    python wan_proxy.py --listen 14330 --target localhost:1433 [--mbit 100] [--rtt-ms 20]

Each direction is shaped independently with a token bucket (--mbit, 0 = no
cap) and delayed by rtt/2 per chunk. Byte and chunk counters per connection
are printed when a connection closes and written to --stats (JSON lines).
TLS passes through untouched, so the counts are true on-the-wire bytes.
"""

import argparse
import asyncio
import json
import time


class Shaper:
    def __init__(self, mbit: float):
        self.rate = mbit * 1_000_000 / 8 if mbit else 0.0
        self.free_at = time.monotonic()

    async def take(self, nbytes: int) -> None:
        if not self.rate:
            return
        now = time.monotonic()
        self.free_at = max(self.free_at, now) + nbytes / self.rate
        delay = self.free_at - now
        if delay > 0.0005:
            await asyncio.sleep(delay)


async def pump(reader, writer, shaper, one_way_s, counter, key):
    # Latency is pipelined: each chunk is stamped on arrival and released
    # one_way_s later by a separate task, so delay does not cut throughput.
    queue: asyncio.Queue = asyncio.Queue()

    async def deliver():
        while (item := await queue.get()) is not None:
            due, data = item
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            writer.write(data)
            await writer.drain()

    sender = asyncio.create_task(deliver())
    try:
        while data := await reader.read(256 * 1024):
            counter[key] += len(data)
            counter[key + "_chunks"] += 1
            await shaper.take(len(data))
            queue.put_nowait((time.monotonic() + one_way_s, data))
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        queue.put_nowait(None)
        try:
            await sender
        except (ConnectionResetError, BrokenPipeError):
            pass
        writer.close()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=14330)
    ap.add_argument("--target", default="localhost:1433")
    ap.add_argument("--mbit", type=float, default=0)
    ap.add_argument("--rtt-ms", type=float, default=0)
    ap.add_argument("--stats", default="")
    args = ap.parse_args()
    host, port = args.target.rsplit(":", 1)
    one_way = args.rtt_ms / 2000

    async def handle(client_reader, client_writer):
        server_reader, server_writer = await asyncio.open_connection(host, int(port))
        counter = {"up": 0, "up_chunks": 0, "down": 0, "down_chunks": 0}
        start = time.monotonic()
        await asyncio.gather(
            pump(client_reader, server_writer, Shaper(args.mbit), one_way, counter, "up"),
            pump(server_reader, client_writer, Shaper(args.mbit), one_way, counter, "down"),
        )
        counter["seconds"] = round(time.monotonic() - start, 3)
        line = json.dumps(counter)
        print(line, flush=True)
        if args.stats:
            with open(args.stats, "a") as fh:
                fh.write(line + "\n")

    server = await asyncio.start_server(handle, "127.0.0.1", args.listen)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
