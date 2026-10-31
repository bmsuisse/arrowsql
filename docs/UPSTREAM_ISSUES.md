# Upstream issue triage (microsoft/mssql-python)

Snapshot: 34 open issues, triaged 2026-10-30 against the vendored Python layer
(upstream commit `30c740d`, `tests/upstream/UPSTREAM_COMMIT`) and the pinned `mssql-tds`
(`mssql-rs@6b6b48e`: has `CancelHandle`, RPC `ReturnValue` tokens, SSPI/GSSAPI auth, bulk load).

Legend. Category: bug / feat / perf / docs / pkg. Class: (a) free from rewrite, (b) Python-layer fix,
(c) Rust-core work, (d) out of scope. Effort S (<1 day) / M (days) / L (week+). "Local" = testable against
the local SQL Server 2022 container. Links: https://github.com/microsoft/mssql-python/issues/N

Caveats: issue bodies were read via `gh`; Rust-side claims about `mssql-tds` come from a quick grep of the
pinned checkout, not from running code. The Rust core has since landed (`src/`), so "(c)" items are no longer
blocked on it; see "Status update" below for what is done. "Free" items are expected, not verified: each needs a
regression test. Strictly-S real fixes number fewer than 15, so the table below also includes
"S: verify and add regression test" and a few cheap M items, labelled as such.

## Status update

| # | Title | Status |
|---|---|---|
| [581](https://github.com/microsoft/mssql-python/issues/581) | Expose SQLSTATE / native error number on exceptions | Done: `sqlstate` and `native_error` on every exception (`tests/common_issues/test_issue_581_sqlstate.py`). Unblocks #682 (retry classification). |
| [492](https://github.com/microsoft/mssql-python/issues/492) | bulkcopy fails for `#temp` tables | Done: native bulk copy runs on the cursor's own connection, so session-scoped `#temp` tables are visible. |
| [874](https://github.com/microsoft/mssql-python/issues/874) | `async-preview` extra pin | The extra pinned `mssql-python-rs==0.3.0`; upstream's issue asks for 0.4.0, the current PyPI release (verified 2026-10-31). The extra now pins `==0.4.0` in `pyproject.toml`; `requirements-dev.txt` still installs 0.3.0 and has not been re-run against 0.4.0. |

## Low-hanging fruit (ranked)

Ranking: real S fixes with clear spec first, then verify-only S, then cheap M.

| Rank | Issue | Cat/Class | Effort | Local | Problem | Fix location | Repro sketch | Upstream PR |
|---|---|---|---|---|---|---|---|---|
| 1 | [#484](https://github.com/microsoft/mssql-python/issues/484) APP keyword reserved | feat / b | S | yes | `APP` is forced to `MSSQL-Python`; users lose per-app identity in `sys.dm_exec_sessions`/Resource Governor. Maintainer (sumitmsft) agreed to allow a user suffix: `MSSQL-Python <AppName>`. | `python/mssql_python/connection.py` ~L1071 (`normalized_params["APP"] = ...`) plus the reserved-key check above it; in our core pass as login-packet app name. | `connect("...;APP=myjob")`; `SELECT program_name FROM sys.dm_exec_sessions WHERE session_id=@@SPID` should be `MSSQL-Python myjob`. | none |
| 2 | [#621](https://github.com/microsoft/mssql-python/issues/621) set/get_audit_context | feat / b | S | yes | No API for `sp_set_session_context`; users hand-roll it. | `Connection.set_audit_context/get_audit_context` in `connection.py`, implemented as `EXEC sp_set_session_context` / `SELECT SESSION_CONTEXT(?)`, with key-length validation (128 / 8000). Start from upstream PR. | `conn.set_audit_context("user","bob")`; `SELECT SESSION_CONTEXT(N'user')` returns `bob`. | [#622](https://github.com/microsoft/mssql-python/pull/622) open |
| 3 | [#515](https://github.com/microsoft/mssql-python/issues/515) callproc() | feat / b+c | S (IN only), M (OUT) | yes | `Cursor.callproc` raises `NotSupportedError`. | `cursor.py` `callproc` (L4163): build `EXEC [schema].[proc] ?, ?`, return the input list. OUT params need RPC return-value tokens from the Rust core (`mssql-tds` has `ReturnValue` parsing). | create proc with `@a int, @b int OUTPUT`; `cur.callproc("dbo.p",[1,0])` returns `[1, <out>]`; result sets via `fetchall`. | none |
| 4 | [#457](https://github.com/microsoft/mssql-python/issues/457) bulkcopy Decimal scale | bug / b or c | S | yes | `bulkcopy` raises `ValueError: Input decimal scale 3 exceeds target scale 2` where `INSERT` rounds. Saurabh500 agreed to relax it. | Decimal conversion in our bulk path (Rust: scale adjust with half-away-from-zero, matching SQL Server) ; interim Python pre-quantize in `Cursor.bulkcopy` (L3507) using target column metadata. | `create table t(d decimal(10,2))`; `cur.bulkcopy('t',[(Decimal('123.456'),)])` should store 123.46. | none |
| 5 | [#850](https://github.com/microsoft/mssql-python/issues/850) SequentialGuid generator | feat / b | S | yes | UUIDv7 / random GUIDs fragment clustered indexes because SQL Server sorts uniqueidentifier by last 6 bytes. | New helper module (e.g. `python/mssql_python/guid.py`, export `sequential_guid()`): timestamp placed in the sort-significant bytes. Pure Python. Feature creep risk: keep tiny, document. | insert 1000 generated GUIDs into `uniqueidentifier` PK table; `ORDER BY id` equals insertion order. | none |
| 6 | [#24](https://github.com/microsoft/mssql-python/issues/24) Cast/type-mapping helpers as functions | docs+refactor / b | S | partly (pure Python) | `_map_sql_type`, `_parse_date`, `_get_numeric_data` etc. are Cursor methods; users want them public. Maintainer asked for a PR with a regression test. | Move to module-level in new `python/mssql_python/type_mapping.py`; keep `Cursor._x` as thin aliases so vendored tests still pass; move MONEY/BIGINT constants to `constants.py`. | `from mssql_python.type_mapping import map_sql_type; map_sql_type(Decimal("1.5"))`. | none |
| 7 | [#278](https://github.com/microsoft/mssql-python/issues/278) Invalid access token crashes | bug / a (+b) | S | yes (negative test) | Malformed `SQL_COPT_SS_ACCESS_TOKEN` segfaulted libmsodbcsql. | No ODBC, so gone. Still add defensive validation in `auth.py` / `_build_pycore_context` (non-empty `bytes`/`str`, sane length) so bad tokens raise `InterfaceError`. | `connect(conn_str, attrs_before={1256: b"\x00junk"})`-style call should raise, not crash. | none |
| 8 | [#261](https://github.com/microsoft/mssql-python/issues/261) JSON / large `(n)varchar(max)` select kills connection | bug / a | S verify | yes (nvarchar(max)); native `json` type needs SQL Server 2025 | Rows longer than ~3136 chars failed with "connection no longer has server response" (ODBC LOB path). | No fix; add a regression test in `tests/` fetching 1k, 3137, 100k and 10M char values for `varchar(max)`, `nvarchar(max)`, `json`-as-text. For SS2025 native JSON type check column type 0xF4 handling in the core. | `select replicate(cast('x' as varchar(max)),100000)`. | none |
| 9 | [#745](https://github.com/microsoft/mssql-python/issues/745) money-range Decimal string-bound in executemany | bug / a | S verify | yes | ODBC layer bound as varchar -> overflow vs `numeric(5,2)`. We bind Decimal natively. | Regression test only; also keep an eye on #503 behaviour. | `executemany("update t set x=1 where v=?",[(Decimal("12345.6789"),)])` against `numeric(5,2)`: must not raise 8115 from varchar conversion. | #752 open, #770 closed unmerged |
| 10 | [#500](https://github.com/microsoft/mssql-python/issues/500) execute() with ~2000 params 14x slower than pyodbc | perf / a? (+b) | S verify | yes | Per-parameter ODBC bind cost. | Add bench to `benchmarks/` (1049-row multi-VALUES insert); profile Python-side param prep in `parameter_helper.py` / `Cursor._create_parameter_types_list` since that may dominate regardless of backend. | script in issue body, N=100k, 2 cols. | none |
| 11 | [#671](https://github.com/microsoft/mssql-python/issues/671) Deadlock with DEBUG logging + threads | bug / a | S verify | yes | GIL/mutex deadlock in C++ log bridge. | Fixed upstream in merged #678 (C++). Our Rust log bridge must never hold a lock while acquiring the GIL; add thread test (`ThreadPoolExecutor`, 4 workers, `setup_logging()`) to the suite. | see issue. | [#678](https://github.com/microsoft/mssql-python/pull/678) merged |
| 12 | [#862](https://github.com/microsoft/mssql-python/issues/862) azure-identity should be optional | pkg / a | S | n/a | Upstream hard-depends on it though imports are lazy. | Already true here: `pyproject.toml` depends only on `mssql-python-rs`. Add `[project.optional-dependencies] azure = ["azure-identity"]` and make the `auth.py` ImportError text say `pip install mssql-python-rs[azure]`. | `pip install` in clean venv, `import mssql_python` must not import `azure`. | none |
| 13 | [#228](https://github.com/microsoft/mssql-python/issues/228) Multi-statement batch with `#temp` returns empty result | bug / c | S-M | yes | `SELECT INTO #t` emits a rowcount-only result first; `fetchall` sees no rows. Workaround `SET NOCOUNT ON`. Not temp-table scoping. | Rust execute/nextset: either auto-skip result sets without columns on first `fetch*` (pyodbc-like behaviour only after `nextset()`; decide semantics) or expose per-statement counts via `cursor.messages` (merged #184). Keep default consistent with vendored tests. | `cur.execute("select 1 x into #t; select * from #t"); cur.fetchall()` -> expect `[(1,)]`. | none |
| 14 | [#174](https://github.com/microsoft/mssql-python/issues/174) varchar vs nvarchar parameter constructors | feat / b+c | M | yes (check plan for index seek / `SQL_VARIANT_PROPERTY`) | All str params bind as nvarchar -> implicit conversion kills seeks on varchar columns. | New `String`/`WString` classes in `type.py` (PEP 249 constructors, exported in `__init__`); param binding in `cursor.py` (`_map_sql_type`) honours them; `setinputsizes` already exists (L1174) so wire `SQL_VARCHAR` there first (S). Rust: bind VARCHAR with collation/codepage. | `cur.execute("select sql_variant_property(?, 'BaseType')", String("a"))` -> `varchar`. | none |
| 15 | [#492](https://github.com/microsoft/mssql-python/issues/492) bulkcopy fails for `#temp` tables | bug / c | M | yes | Upstream `bulkcopy` opens a second connection (mssql_py_core), so session-scoped `#t` is invisible. | By design in our core: run bulk load on the cursor's own connection/transaction (`mssql-tds` bulk load takes an existing connection). Then `#local` and non-autocommit just work. Python `bulkcopy` (L3507) stops building a pycore context. | `create table #t(id int); cur.bulkcopy('#t',[(1,)])`; `select count(*) from #t`. | none (maintainers call it "by design") |

## Fixed for free by the rewrite (verify with a regression test)

| Issue | Why | Residual work |
|---|---|---|
| [#668](https://github.com/microsoft/mssql-python/issues/668) TLS corruption, threaded reads, macOS arm64 | Reproduces with pyodbc too -> msodbcsql18 + OpenSSL bug. We use Rust TLS. | Needs macOS arm64 to confirm; add 2-thread large-fetch stress test (`tests/upstream/test_011_singlethreaded_stress.py` is single-threaded only). |
| [#679](https://github.com/microsoft/mssql-python/issues/679) SIGSEGV in SQLFetchScroll, macOS | libmsodbcsql crash. | Same stress test. |
| [#685](https://github.com/microsoft/mssql-python/issues/685) SIGSEGV at teardown (`iconv_close`) | libmsodbcsql/libiconv. | Teardown test with threads + encrypt. |
| [#826](https://github.com/microsoft/mssql-python/issues/826) macOS Homebrew `openssl@4` breaks bundled ODBC | No ODBC, no dlopen of OpenSSL (depends on TLS backend chosen; rustls = none). | Document TLS backend. |
| [#455](https://github.com/microsoft/mssql-python/issues/455) String column buffer sizing/chunking, 2x memory vs pyodbc | Fixed-width per-column ODBC buffers + 1000-row/1 GiB heuristic. TDS streams variable-length values. | Memory benchmark vs pyodbc with `STRING_AGG` varchar(8000) view. |
| [#746](https://github.com/microsoft/mssql-python/issues/746) pool exceeds max_size | Bug in upstream pool counter. Pool is replaced. | Build ours with RAII slot tokens, not manual `_current_size`; concurrency test. Upstream fix: [#797](https://github.com/microsoft/mssql-python/pull/797) open. |
| [#257](https://github.com/microsoft/mssql-python/issues/257) pyproject.toml | Already uses `pyproject.toml` + maturin. | none |
| [#862](https://github.com/microsoft/mssql-python/issues/862), [#278](https://github.com/microsoft/mssql-python/issues/278), [#261](https://github.com/microsoft/mssql-python/issues/261), [#745](https://github.com/microsoft/mssql-python/issues/745), [#500](https://github.com/microsoft/mssql-python/issues/500), [#671](https://github.com/microsoft/mssql-python/issues/671) | see ranked table | tests / small hardening |

## Larger work

| Issue | Cat/Class | Effort | Local | Notes |
|---|---|---|---|---|
| [#9](https://github.com/microsoft/mssql-python/issues/9) async support | feat / c | L | yes | Vendored tree already has `python/mssql_python/async_query/` (uses `mssql_py_core`; upstream `mssql-py-core/src/async_*.rs` in the pinned crate). Work = port onto our core, trio/anyio compat requested in comments. |
| [#424](https://github.com/microsoft/mssql-python/issues/424) cancel a query | feat / b+c | M | yes (`WAITFOR DELAY`) | Add `Cursor.cancel()`; Rust releases the GIL during execution and uses `mssql-tds` `CancelHandle` (attention packet). Expect `HY008`-equivalent error (ties to #581). Good first Rust feature after basic execute. |
| [#682](https://github.com/microsoft/mssql-python/issues/682) built-in retry logic | feat / b | M | yes (deadlock, `KILL`) | Python wrapper around connect/execute classifying by native error number (1205, 40197, 40501, 49918, 4060...). Depends on #581. Upstream labelled "up for grabs". |
| [#395](https://github.com/microsoft/mssql-python/issues/395) MARS | feat / c | L | yes | Needs SMUX in the TDS layer; not seen in `mssql-tds`. Probably upstream-of-us work. Low demand. |
| [#25](https://github.com/microsoft/mssql-python/issues/25) query builder | feat / d | L | n/a | Out of driver scope. Listed here for tracking; recommend not doing. |
| [#625](https://github.com/microsoft/mssql-python/issues/625) `DOMAIN\user` + password | bug/question / c | M-L | no (needs AD) | FreeTDS silently does NTLM for `DOMAIN\user`. Needs NTLM in the core; `mssql-tds` SSPI/NTLM seems Windows-oriented, Linux has GSSAPI/Kerberos only. Document as unsupported unless TDS adds it. |

## Out of scope

| Issue | Reason |
|---|---|
| [#848](https://github.com/microsoft/mssql-python/issues/848) bulkcopy Schannel 0x8009030D | Windows 10 1607 / Server 2016 Schannel TLS config; Windows-only. |
| [#554](https://github.com/microsoft/mssql-python/issues/554) Dev blog benchmark | Not actionable (user report on arrow speedups); reuse its script for `benchmarks/`. |
| [#450](https://github.com/microsoft/mssql-python/issues/450) copilot instructions / devcontainer docs | Upstream dev infra. |
| [#276](https://github.com/microsoft/mssql-python/issues/276) PR template | Upstream dev infra. |
| [#748](https://github.com/microsoft/mssql-python/issues/748) Django / mssql-django 2.0 announcement | Announcement, not a task. |
| [#25](https://github.com/microsoft/mssql-python/issues/25) query builder | See above. |

## Notes on coverage

Remaining unclassified-by-name items were handled above: #581, #492 and #874 are covered in the status table; every one of the 34 open
issues appears in exactly one section (some are cross-listed from "Fixed for free" to the ranked table
when we should still add a test). Not checked: whether any upstream branch (not PR) addresses #24, #174,
#850; run `gh pr list -R microsoft/mssql-python --search "<n>"` before starting.
