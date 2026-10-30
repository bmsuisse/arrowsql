# Ported test suites

Behaviour tests adapted from other SQL Server drivers and libraries, rewritten
for `mssql_python` (DB-API, `?` parameters) and validated against the upstream
ODBC driver. Each file header names its source, path and license.

- Permissive sources (MIT, MIT-0, BSD-3-Clause, Apache-2.0) are adapted with
  attribution; license texts that require reproduction are in `LICENSE_*.txt`.
- LGPL sources (pymssql) are used for ideas only: those tests are written
  fresh, with no code or test text copied.

Sources: microsoft/mssql-jdbc, microsoft/go-mssqldb, praxiomlabs/rust-mssql-driver,
tediousjs/tedious, dotnet/SqlClient, sqlalchemy/sqlalchemy, microsoft/mssql-rs,
prisma/tiberius, zillow/ctds, pymssql/pymssql (ideas only), mkleehammer/pyodbc,
Rivendael/FastMssql.
