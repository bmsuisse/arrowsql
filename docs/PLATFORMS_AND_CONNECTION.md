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
| `ConnectRetryCount`, `ConnectRetryInterval`, `MultiSubnetFailover`, `ApplicationIntent`, `PacketSize`, `Connection Timeout` | supported |
| `Trusted_Connection=yes` / `Integrated Security=sspi` | Kerberos via GSSAPI (library loaded at runtime) on Unix, SSPI on Windows; not tested here |
| `Authentication=ActiveDirectoryDefault`, `ActiveDirectoryInteractive`, `ActiveDirectoryDeviceCode`, `ActiveDirectoryMSI` | acquired in Python with `azure-identity` (the `[azure]` extra) and passed as an access token; not tested against Entra ID here |
| `Authentication=ActiveDirectoryServicePrincipal` | native FedAuth; the token factory (azure-identity `ClientSecretCredential`, UID = client id, PWD = secret) runs once the server names the tenant; not tested against Entra ID here |
| `token_provider=` / `attrs_before={1256: ...}` (access token) | supported |
| `Authentication=SqlPassword` | supported (SQL login, same as no `Authentication`) |
| `Authentication=ActiveDirectoryPassword` | supported: native FedAuth, token from azure-identity `UsernamePasswordCredential` (fails for MFA accounts); not tested against Entra ID here |
| `Authentication=ActiveDirectoryIntegrated`, `ActiveDirectoryWorkloadIdentity`, NTLM (`DOMAIN\user`) | not supported (`InterfaceError`; use `Trusted_Connection=yes` or `token_provider=`) |
| Always Encrypted (`ColumnEncryption`) | not supported |


## Environment variables


| variable | effect |
|---|---|
| `MSSQL_RS_BUSY_POLL` | `0`, `off` or `false` disables the short busy poll for the first response byte (default on, at most `min(cpus / 4, 2)` spinning threads; it trades a little CPU for lower latency on very short round trips) |
| `MSSQL_RS_MULTISUBNET_FAILOVER_DEFAULT` | `true` / `false`: deployment-wide default for `MultiSubnetFailover` when the connection string does not set it |
| `MSSQL_PYTHON_NATIVE_PROVIDER` | upstream's ODBC provider selector; has no effect on the Rust path |
| `DB_CONNECTION_STRING` | used by the test suites and benchmarks only, never by the library |


## Security note


The quick-start connection strings above use `TrustServerCertificate=yes`, which
switches certificate validation off. That is for **local testing against a
container only**. For anything else use a certificate the client trusts (or
`ServerCertificate=` / `HostNameInCertificate=`) and keep `Encrypt=yes`.

