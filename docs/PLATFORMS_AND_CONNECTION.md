# Platforms, connection strings and environment

## Platforms and Python versions


Honest status: Linux x86_64 (glibc) is the reference platform, the only one run
against SQL Server by hand. CI (`../.github/workflows/ci.yml`) runs the full suites
against SQL Server 2022 on Linux x86_64 and against SQL Server Express on Windows
x64, and the server-free tests plus `cargo test --lib` on macOS (arm64, x86_64),
Windows (x64, arm64) and Linux arm64. The wheel workflow
(`../.github/workflows/wheels.yml`) builds every wheel below and import-tests it;
publishing to PyPI is gated by `../.github/workflows/workflow.yml`.

| platform | wheel | import tested in CI | DB tests in CI |
|---|---|---|---|
| Linux x86_64, glibc (manylinux_2_28) | yes | native | SQL Server 2022 |
| Linux aarch64, glibc (manylinux_2_28) | yes | native (arm runner) | no (no arm64 server image) |
| Linux x86_64 / aarch64, musl (musllinux_1_2) | yes | Alpine container | no |
| macOS arm64 / x86_64 | yes | native | no (server-free tests) |
| Windows x64 | yes | native | SQL Server Express |
| Windows arm64 | yes | native (arm runner) | no (server-free tests) |

`aio` is Unix-only (needs `loop.add_reader`). Not built: ppc64le, s390x (cross images unusable with --find-interpreter, see PACKAGING.md), 32-bit targets (i686,
armv7: the vendored `mssql-tds` uses 64-bit `usize` constants) and PyPy.

Python: CPython 3.10 to 3.15 plus free-threaded 3.14t and 3.15t where an
interpreter exists for the platform.


## Connection string


Parsed natively (`src/connstr.rs`) and by the vendored Python layer.

| keyword | status |
|---|---|
| `Server` / `Address` / `Data Source`, `Database`, `Uid`, `Pwd`, `APP`, `WSID`, `Language` | supported |
| `Encrypt` (`yes`/`no`/`strict`), `TrustServerCertificate`, `HostNameInCertificate`, `ServerCertificate` | supported (rustls; TLS 1.2 / 1.3). `Encrypt=strict` is implemented but not verifiable against the local SQL Server 2022 container |
| `ConnectRetryCount`, `ConnectRetryInterval`, `MultiSubnetFailover`, `ApplicationIntent`, `PacketSize` | supported |
| login timeout | set with `connect(timeout=<seconds>)` (as upstream `mssql_python.connect`); an expired login raises `OperationalError` HYT00 "Login timeout expired" |
| `DnsCacheTTL=<seconds>` | arrowsql extension: seconds a successfully resolved host name is reused without asking the resolver (default 300; `0` disables the cache and its retries entirely; env `ARROWSQL_DNS_CACHE_TTL` sets the default). See [DNS cache](#dns-cache) |
| `Trusted_Connection=yes` / `Integrated Security=sspi` | Kerberos via GSSAPI (library loaded at runtime) on Unix, SSPI on Windows; not tested here |
| `Authentication=ActiveDirectoryDefault`, `ActiveDirectoryInteractive`, `ActiveDirectoryDeviceCode`, `ActiveDirectoryMSI`, `ActiveDirectoryServicePrincipal` | acquired in Python with `azure-identity` (the `[azure]` extra) and passed as an access token; not tested against Entra ID here |
| `token_provider=` / `attrs_before={1256: ...}` (access token) | supported |
| `Authentication=ActiveDirectoryPassword`, `SqlPassword`, `ActiveDirectoryIntegrated`, NTLM (`DOMAIN\user`) | not supported |
| Always Encrypted (`ColumnEncryption`) | not supported |


## Environment variables


| variable | effect |
|---|---|
| `MSSQL_RS_BUSY_POLL` | `0`, `off` or `false` disables the short busy poll for the first response byte (default on, at most `min(cpus / 4, 2)` spinning threads; it trades a little CPU for lower latency on very short round trips) |
| `MSSQL_RS_MULTISUBNET_FAILOVER_DEFAULT` | `true` / `false`: deployment-wide default for `MultiSubnetFailover` when the connection string does not set it |
| `ARROWSQL_DNS_CACHE_TTL` | default `DnsCacheTTL` in seconds when the connection string does not set it (default 300, `0` disables the DNS cache) |
| `ARROWSQL_DNS_RESOLVE_TIMEOUT` | bound in seconds on one resolver call made through the DNS cache (default 10; a temporary failure or timeout is retried once) |
| `MSSQL_PYTHON_NATIVE_PROVIDER` | upstream's ODBC provider selector; has no effect on the Rust path |
| `DB_CONNECTION_STRING` | used by the test suites and benchmarks only, never by the library |


## DNS cache

The JVM JDBC driver caches successful lookups for the life of the process.
arrowsql used to resolve the server name on every connection, so on hosts with
a slow or flaky resolver (Databricks clusters resolving an on-premises name:
seconds per lookup, occasional "Temporary failure in name resolution") every
Spark task, parallel read slice and pool member paid for it. The process-wide
cache in front of the TCP and SQL Browser lookups works like this:

- Entries are keyed by host (case-insensitive) and port and hold the
  resolver's full address list in its order, so `MultiSubnetFailover` and
  `IpAddressPreference` see the same addresses as without the cache. At most
  256 entries are kept.
- An entry younger than `DnsCacheTTL` (default 300 s) is used without asking
  the resolver.
- An older entry, up to one hour past its TTL, is used at once while a single
  background thread refreshes it; a failed refresh keeps the old addresses.
  Beyond that the refresh happens in line, and the old addresses are used only
  if it fails.
- Concurrent lookups of the same host (the slices of a parallel read, a pool
  filling up) share one resolver call; its failure is shared with those
  waiters but never cached.
- Each resolver call is bounded by `ARROWSQL_DNS_RESOLVE_TIMEOUT` (default
  10 s), and a timeout or temporary failure is retried once. An unknown name
  is not retried.
- If no address of a cached entry accepts the connection, the entry is
  dropped and the host is resolved once more within the connect timeout.
- IP literals, `DnsCacheTTL=0` and connection diagnostics bypass the cache:
  they resolve exactly as before, without retries.

`mssql_python.ddbc_bindings.dns_cache_stats()` returns the counters (`hits`,
`stale_served`, `misses`, `coalesced`, `refreshes`, `resolver_calls`,
`resolver_failures`, `entries`, `in_flight`, and `os_resolver_calls`, which
also counts lookups that bypassed the cache); `dns_cache_clear()` empties it.


## Security note


The quick-start connection strings above use `TrustServerCertificate=yes`, which
switches certificate validation off. That is for **local testing against a
container only**. For anything else use a certificate the client trusts (or
`ServerCertificate=` / `HostNameInCertificate=`) and keep `Encrypt=yes`.

