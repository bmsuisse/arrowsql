"""Login with base64-style passwords that contain '=' and '+' ported from tiberius.

Adapted from prisma/tiberius tests/special_char_password.rs (MIT OR Apache-2.0), Copyright 2021 Tiberius Contributors.
Upstream license texts: https://github.com/prisma/tiberius/blob/main/LICENSE-MIT.txt and LICENSE-APACHE.txt.
The cases were re-expressed against mssql_python.connect(); no code was copied.

Creates a SQL login whose password is written unquoted in the connection string, proving the
password survives parsing and the whole login handshake. Requires a sysadmin connection, which
the sandbox provides. Quoting-related cases are covered in common_issues/test_connection_strings.py.
"""

import re
import uuid

import pytest

import mssql_python


def _strip_keys(conn_str, keys):
    pattern = r"(?i)(^|;)\s*(" + "|".join(keys) + r")\s*=[^;]*"
    return re.sub(pattern, r"\1", conn_str).strip(";")


@pytest.mark.parametrize("password", ["Ab1!Zm9vYmFy==", "Ab1!Yy9+Zz0="])
def test_unquoted_base64_password_with_padding_authenticates(cursor, conn_str, password):
    login = f"tib_pw_{uuid.uuid4().hex[:12]}"
    cursor.execute(
        f"CREATE LOGIN [{login}] WITH PASSWORD = '{password}', CHECK_POLICY = OFF"
    )
    cursor.connection.commit()
    try:
        # A fresh login only has CONNECT rights on master, so target it explicitly.
        base = _strip_keys(conn_str, ["uid", "user id", "pwd", "password", "database"])
        user_conn_str = f"{base};Database=master;Uid={login};Pwd={password}"
        user_conn = mssql_python.connect(user_conn_str)
        try:
            user_cur = user_conn.cursor()
            user_cur.execute("SELECT SUSER_SNAME(), @@VERSION")
            row = user_cur.fetchone()
            assert row[0] == login
            assert row[1]
        finally:
            user_conn.close()
    finally:
        # Own autocommit connection: KILL is refused inside a user transaction. The closed user
        # connection may still sit in the pool, so its session is killed before the DROP.
        admin = mssql_python.connect(conn_str, autocommit=True)
        try:
            admin.cursor().execute(
                "DECLARE @k NVARCHAR(MAX) = N''; "
                "SELECT @k += N'KILL ' + CAST(session_id AS NVARCHAR(10)) + N'; ' "
                f"FROM sys.dm_exec_sessions WHERE login_name = N'{login}'; "
                "IF LEN(@k) > 0 EXEC (@k);"
            )
            admin.cursor().execute(f"IF SUSER_ID('{login}') IS NOT NULL DROP LOGIN [{login}]")
        finally:
            admin.close()
