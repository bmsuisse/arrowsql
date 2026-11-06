"""Process-wide DNS cache (vendor/mssql-tds dns_ttl_cache.rs, VENDOR.md patch 15).

Connects by the host name `localhost` and watches the cache's counters
(`ddbc_bindings.dns_cache_stats()`); a slow or failing resolver is simulated
with `dns_cache_test_fault`, a stale or wrong entry is planted with
`dns_cache_test_seed`. The cache logic itself is unit tested in
src/dns_ttl_cache_tests.rs.
"""

import os
import re
import socket
import subprocess
import sys
import threading
import time

import pytest

import mssql_python
from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"), reason="Database connection string not provided"
)

_SERVER = re.compile(r"(?i)(^|;)(\s*(?:server|address|addr|data source)\s*=\s*)(?:tcp:)?([^;]+)")


def _server_port(conn_str: str) -> int:
    value = _SERVER.search(conn_str).group(3)
    return int(value.split(",", 1)[1]) if "," in value else 1433


def _with_host(conn_str: str, host: str) -> str:
    """conn_str with its host replaced by ``host`` (port kept)."""
    port = _server_port(conn_str)
    new, count = _SERVER.subn(lambda m: f"{m.group(1)}{m.group(2)}{host},{port}", conn_str, count=1)
    assert count == 1
    return new


CONN_STR = os.getenv("DB_CONNECTION_STRING", "")
LOCALHOST = _with_host(CONN_STR, "localhost") if CONN_STR else ""
PORT = _server_port(CONN_STR) if CONN_STR else 1433


def _stats() -> dict:
    return ddbc_bindings.dns_cache_stats()


def _delta(before: dict) -> dict:
    after = _stats()
    return {k: after[k] - before[k] for k in after if k not in ("entries", "in_flight")}


def _settle(timeout: float = 15.0) -> None:
    """Waits for background refreshes to finish."""
    deadline = time.monotonic() + timeout
    while _stats()["in_flight"]:
        assert time.monotonic() < deadline, "background DNS refresh did not finish"
        time.sleep(0.02)


def _query(conn_str: str) -> None:
    conn = mssql_python.connect(conn_str)
    try:
        assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


def _localhost_v4() -> str:
    return f"127.0.0.1:{PORT}"


@pytest.fixture(autouse=True)
def _unpooled_empty_cache():
    mssql_python.pooling(enabled=False)
    ddbc_bindings.dns_cache_test_fault(0, 0)
    ddbc_bindings.dns_cache_clear()
    yield
    ddbc_bindings.dns_cache_test_fault(0, 0)
    _settle()
    ddbc_bindings.dns_cache_clear()


def test_second_connect_does_not_resolve_again():
    before = _stats()
    for _ in range(3):
        _query(LOCALHOST)
    d = _delta(before)
    assert d["os_resolver_calls"] == 1
    assert (d["misses"], d["hits"]) == (1, 2)
    assert _stats()["entries"] == 1


def test_dns_cache_ttl_zero_resolves_every_time_without_retry():
    conn_str = LOCALHOST + ";DnsCacheTTL=0"
    before = _stats()
    _query(conn_str)
    _query(conn_str)
    d = _delta(before)
    assert d["os_resolver_calls"] == 2
    assert (d["hits"], d["misses"], d["resolver_calls"]) == (0, 0, 0)
    assert _stats()["entries"] == 0


def test_ttl_from_the_environment():
    code = (
        "import sys, mssql_python\n"
        "mssql_python.pooling(enabled=False)\n"
        "from mssql_python import ddbc_bindings\n"
        "for _ in range(2):\n"
        "    mssql_python.connect(sys.argv[1]).close()\n"
        "print(ddbc_bindings.dns_cache_stats()['os_resolver_calls'])\n"
    )
    for ttl, expected in (("0", "2"), ("300", "1")):
        out = subprocess.run(
            [sys.executable, "-I", "-c", code, LOCALHOST],
            env={**os.environ, "ARROWSQL_DNS_CACHE_TTL": ttl},
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        assert out == expected, f"ARROWSQL_DNS_CACHE_TTL={ttl}"


def test_ip_literal_bypasses_the_cache():
    before = _stats()
    _query(_with_host(CONN_STR, "127.0.0.1"))
    d = _delta(before)
    assert (d["hits"], d["misses"]) == (0, 0)
    assert _stats()["entries"] == 0


@pytest.mark.parametrize("bad", ["x", "-1", "1.5"])
def test_invalid_dns_cache_ttl_is_rejected(bad):
    with pytest.raises(Exception, match="DnsCacheTTL"):
        mssql_python.connect(CONN_STR + f";DnsCacheTTL={bad}")


def test_concurrent_cold_connects_share_one_lookup():
    """Many connections opened at once from a cold cache resolve once."""
    ddbc_bindings.dns_cache_test_fault(0, 300)
    barrier = threading.Barrier(6)
    errors = []

    def worker():
        try:
            barrier.wait()
            _query(LOCALHOST)
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    before = _stats()
    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    d = _delta(before)
    assert d["os_resolver_calls"] == 1
    assert d["misses"] + d["coalesced"] + d["hits"] == 6
    assert d["coalesced"] >= 1


def test_parallel_read_slices_hit_the_cache():
    """All slices of one partitioned read are served by one lookup."""
    query = "SELECT object_id AS id FROM sys.all_objects"
    bounds = [-1_000_000_000, 0, 1_000_000_000]
    conn = mssql_python.connect(_with_host(CONN_STR, "127.0.0.1"))
    expected = conn.cursor().execute("SELECT COUNT(*) FROM sys.all_objects").fetchone()[0]
    conn.close()
    ddbc_bindings.dns_cache_clear()
    before = _stats()
    table = mssql_python.read_arrow(LOCALHOST, query, partition_on="id", partitions=4, partition_bounds=bounds)
    assert table.num_rows == expected
    d = _delta(before)
    assert d["os_resolver_calls"] == 1
    assert d["hits"] + d["coalesced"] >= 3, d


def test_stale_entry_is_served_while_the_resolver_is_down():
    ddbc_bindings.dns_cache_test_seed("localhost", PORT, [_localhost_v4()], age_secs=400)
    ddbc_bindings.dns_cache_test_fault(100, 0)
    before = _stats()
    _query(LOCALHOST)
    _settle()
    d = _delta(before)
    assert (d["stale_served"], d["refreshes"]) == (1, 1)
    assert d["resolver_failures"] >= 1
    assert _stats()["entries"] == 1, "a failed refresh keeps the stale entry"


def test_slow_resolver_does_not_delay_a_stale_connect():
    ddbc_bindings.dns_cache_test_seed("localhost", PORT, [_localhost_v4()], age_secs=400)
    ddbc_bindings.dns_cache_test_fault(0, 3000)
    started = time.monotonic()
    _query(LOCALHOST)
    assert time.monotonic() - started < 2.0
    ddbc_bindings.dns_cache_test_fault(0, 0)
    _settle()
    # The refresh landed: the next connect is a fresh hit.
    before = _stats()
    _query(LOCALHOST)
    assert _delta(before)["hits"] == 1


def test_transient_resolver_failure_is_retried():
    ddbc_bindings.dns_cache_test_fault(1, 0)
    before = _stats()
    _query(LOCALHOST)
    d = _delta(before)
    assert (d["os_resolver_calls"], d["resolver_failures"]) == (2, 1)


def test_cached_address_that_refuses_is_dropped_and_resolved_again():
    # A closed port on loopback refuses at once, like a server that moved.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed = probe.getsockname()[1]
    ddbc_bindings.dns_cache_test_seed("localhost", PORT, [f"127.0.0.1:{closed}"])
    before = _stats()
    _query(LOCALHOST)
    d = _delta(before)
    assert (d["hits"], d["misses"], d["os_resolver_calls"]) == (1, 1, 1)
    before = _stats()
    _query(LOCALHOST)
    assert _delta(before)["hits"] == 1, "the re-resolved addresses were cached"


def test_multisubnet_failover_uses_the_cache():
    conn_str = LOCALHOST + ";MultiSubnetFailover=yes"
    before = _stats()
    _query(conn_str)
    _query(conn_str)
    d = _delta(before)
    assert (d["os_resolver_calls"], d["hits"]) == (1, 1)


def test_pooled_connections_share_the_cache():
    mssql_python.pooling(max_size=4, idle_timeout=30)
    try:
        before = _stats()
        conns = [mssql_python.connect(LOCALHOST) for _ in range(3)]
        for conn in conns:
            conn.close()
        assert _delta(before)["os_resolver_calls"] == 1
    finally:
        mssql_python.pooling(enabled=False)
