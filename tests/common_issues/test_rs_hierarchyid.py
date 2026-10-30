"""hierarchyid round-trips for the native-TDS rewrite.

hierarchyid is a CLR user-defined type. The server sends it as its serialized
varbinary form, so a TDS implementation has to (a) recognise the UDT
(type id 240 / UDT with a CLR type name) and surface it as bytes, not as an
error or a str, and (b) keep the server-side methods (ToString, GetAncestor,
GetDescendant, IsDescendantOf, Parse) working for parameters and results.

Research (issue trackers and docs, checked for this suite):
- https://github.com/microsoft/mssql-python/releases (v1.4.0: native handling of
  geography / geometry / hierarchyid, "Spatial Type Support" #423; a later fix
  for bulk copy of custom CLR UDTs, Protocol Error 0xF0)
- https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-4-bulk-copy-arrives---load-millions-of-rows-at-native-speed/4503171
- https://github.com/mkleehammer/pyodbc/issues/372 (binary columns, output converters)
- https://github.com/clast-project/mssql-adbc/issues/3 (hierarchyid read as its
  serialized binary; text form needs ToString on the server side)
- https://github.com/efcore/EFCore.SqlServer.HierarchyId/issues/37 (hand-written
  binary encodings disagree with SQL Server; let the server build the values)
- https://github.com/dotnet/efcore/issues/365 (SQL Server hierarchyid support)
- https://github.com/zzzprojects/EntityFramework-Effort/issues/190 (unsupported
  hierarchyid store type in a test double)
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/isdescendantof-database-engine
  (IsDescendantOf returns true for the node itself)
- https://learn.microsoft.com/en-us/ef/core/providers/sql-server/hierarchyid
  (GetDescendant / GetAncestor mapping)
- https://www.sqlshack.com/use-hierarchyid-sql-server/
- https://www.meziantou.net/sql-server-discovering-the-hierarchyid-data-type.htm

Failure modes covered: hierarchyid returned as bytes (not str / not a UDT
error), exact bytes for known paths, ToString and GetLevel types, Parse and
CAST from string parameters, varbinary parameter round-trip into a hierarchyid
column, NULL handling, GetAncestor past the root, GetDescendant siblings and
between-siblings, IsDescendantOf including self (returned as exact bool),
depth-first ordering, implicit CAST to nvarchar, and server-side error mapping
for invalid paths.
"""

import pytest
from mssql_python import ProgrammingError


def _fetch_one(cursor, sql, params=()):
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _make_tree_table(cursor):
    """Temp table with a small tree, inserted out of order."""
    cursor.execute("CREATE TABLE #tree (id int NOT NULL PRIMARY KEY, node hierarchyid NULL)")
    rows = [
        (4, "/2/"),
        (1, "/"),
        (6, "/1/2/"),
        (2, "/1/"),
        (5, "/1/1/"),
        (3, "/1/1/1/"),
    ]
    for node_id, path in rows:
        cursor.execute(
            "INSERT INTO #tree (id, node) VALUES (?, hierarchyid::Parse(?))",
            (node_id, path),
        )
    cursor.connection.commit()


# ---------------------------------------------------------------------------
# Raw value shape
# ---------------------------------------------------------------------------


def test_raw_hierarchyid_is_bytes_with_exact_serialization(cursor):
    row = _fetch_one(cursor, "SELECT CAST('/1/2/' AS hierarchyid)")
    assert type(row[0]) is bytes
    assert row[0] == b"[@"


def test_root_hierarchyid_serialization_and_string(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/' AS hierarchyid), CAST('/' AS hierarchyid).ToString(), "
        "CAST('/' AS hierarchyid).GetLevel()",
    )
    assert type(row[0]) is bytes
    assert row[1] == "/"
    assert row[2] == 0
    assert type(row[2]) is int


def test_null_hierarchyid_fetches_as_none(cursor):
    row = _fetch_one(cursor, "SELECT CAST(NULL AS hierarchyid), CAST(NULL AS hierarchyid).ToString()")
    assert row == (None, None)


def test_hierarchyid_column_in_temp_table_round_trip(cursor):
    _make_tree_table(cursor)
    cursor.execute("SELECT id, node, node.ToString() FROM #tree WHERE id = 6")
    row = cursor.fetchone()
    assert row[0] == 6
    assert type(row[1]) is bytes
    assert row[1] == b"[@"  # same serialization as CAST('/1/2/' AS hierarchyid)
    assert row[2] == "/1/2/"


# ---------------------------------------------------------------------------
# Server-side methods returning str / int / bool
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path, level",
    [("/", 0), ("/1/", 1), ("/1/2/", 2), ("/1/1/1/", 3)],
)
def test_tostring_and_getlevel_types_and_values(cursor, path, level):
    row = _fetch_one(
        cursor,
        "SELECT CAST(? AS hierarchyid).ToString(), CAST(? AS hierarchyid).GetLevel()",
        (path, path),
    )
    assert row[0] == path
    assert type(row[0]) is str
    assert row[1] == level
    assert type(row[1]) is int


def test_implicit_cast_to_nvarchar_gives_same_path(cursor):
    row = _fetch_one(cursor, "SELECT CAST(CAST('/1/2/' AS hierarchyid) AS nvarchar(4000))")
    assert row[0] == "/1/2/"
    assert type(row[0]) is str


def test_is_descendant_of_includes_self_as_python_bool(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/1/2/' AS hierarchyid).IsDescendantOf(CAST('/1/2/' AS hierarchyid))",
    )
    assert row[0] is True


def test_is_descendant_of_false_is_python_bool(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/1/' AS hierarchyid).IsDescendantOf(CAST('/2/' AS hierarchyid))",
    )
    assert row[0] is False


# ---------------------------------------------------------------------------
# Parameters: Parse(?), CAST(? AS hierarchyid), varbinary round trip
# ---------------------------------------------------------------------------


def test_parse_from_string_parameter(cursor):
    row = _fetch_one(cursor, "SELECT hierarchyid::Parse(?).ToString()", ("/1/3/2/",))
    assert row[0] == "/1/3/2/"


def test_cast_string_parameter_to_hierarchyid_is_bytes(cursor):
    row = _fetch_one(cursor, "SELECT CAST(? AS hierarchyid)", ("/1/2/",))
    assert type(row[0]) is bytes
    assert row[0] == b"[@"


def test_varbinary_parameter_cast_back_to_hierarchyid(cursor):
    """Raw bytes fetched from a hierarchyid can be sent back as a binary parameter."""
    raw = _fetch_one(cursor, "SELECT CAST('/1/2/' AS hierarchyid)")[0]
    row = _fetch_one(cursor, "SELECT CAST(? AS hierarchyid).ToString()", (raw,))
    assert row[0] == "/1/2/"


def test_binary_parameter_insert_into_hierarchyid_column(cursor):
    cursor.execute("CREATE TABLE #bin_insert (id int NOT NULL, node hierarchyid NULL)")
    raw = _fetch_one(cursor, "SELECT CAST('/1/1/' AS hierarchyid)")[0]
    cursor.execute("INSERT INTO #bin_insert (id, node) VALUES (?, CAST(? AS hierarchyid))", (1, raw))
    cursor.connection.commit()
    row = _fetch_one(cursor, "SELECT node, node.ToString() FROM #bin_insert WHERE id = 1")
    assert row[0] == raw
    assert row[1] == "/1/1/"


def test_invalid_path_string_raises_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT hierarchyid::Parse(?)", ("/1/x/",))
        cursor.fetchone()


def test_none_parameter_yields_null_hierarchyid(cursor):
    row = _fetch_one(cursor, "SELECT CAST(? AS hierarchyid), CAST(? AS hierarchyid).ToString()", (None, None))
    assert row == (None, None)


# ---------------------------------------------------------------------------
# Tree methods: GetAncestor / GetDescendant
# ---------------------------------------------------------------------------


def test_get_ancestor_levels(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/1/2/' AS hierarchyid).GetAncestor(1).ToString(), "
        "CAST('/1/2/' AS hierarchyid).GetAncestor(2).ToString()",
    )
    assert row == ("/1/", "/")


def test_get_ancestor_beyond_root_is_null(cursor):
    row = _fetch_one(cursor, "SELECT CAST('/1/2/' AS hierarchyid).GetAncestor(3)")
    assert row[0] is None


def test_get_descendant_first_child_and_next_sibling(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/1/' AS hierarchyid).GetDescendant(NULL, NULL).ToString(), "
        "CAST('/1/' AS hierarchyid).GetDescendant(CAST('/1/1/' AS hierarchyid), NULL).ToString()",
    )
    assert row == ("/1/1/", "/1/2/")


def test_get_descendant_between_two_siblings(cursor):
    row = _fetch_one(
        cursor,
        "SELECT CAST('/1/' AS hierarchyid).GetDescendant(CAST('/1/1/' AS hierarchyid), "
        "CAST('/1/2/' AS hierarchyid)).ToString()",
    )
    assert row[0] == "/1/1.1/"


# ---------------------------------------------------------------------------
# Tables: ordering, subtree queries, insert via Parse
# ---------------------------------------------------------------------------


def test_order_by_hierarchyid_is_depth_first(cursor):
    _make_tree_table(cursor)
    cursor.execute("SELECT node.ToString() FROM #tree ORDER BY node")
    paths = [tuple(r)[0] for r in cursor.fetchall()]
    assert paths == ["/", "/1/", "/1/1/", "/1/1/1/", "/1/2/", "/2/"]


def test_subtree_query_with_is_descendant_of_includes_root_node(cursor):
    _make_tree_table(cursor)
    cursor.execute(
        "SELECT id FROM #tree WHERE node.IsDescendantOf(hierarchyid::Parse(?)) = 1 ORDER BY id",
        ("/1/",),
    )
    ids = [r[0] for r in cursor.fetchall()]
    # /1/ itself, /1/1/, /1/1/1/, /1/2/
    assert ids == [2, 3, 5, 6]


def test_direct_children_via_get_ancestor(cursor):
    _make_tree_table(cursor)
    cursor.execute(
        "SELECT id FROM #tree WHERE node.GetAncestor(1) = hierarchyid::Parse(?) ORDER BY id",
        ("/1/",),
    )
    assert [r[0] for r in cursor.fetchall()] == [5, 6]


def test_executemany_insert_parsed_paths_and_read_back(cursor):
    cursor.execute("CREATE TABLE #many (id int NOT NULL, node hierarchyid NOT NULL)")
    data = [(1, "/1/"), (2, "/1/1/"), (3, "/1/2/")]
    cursor.executemany("INSERT INTO #many (id, node) VALUES (?, hierarchyid::Parse(?))", data)
    cursor.connection.commit()
    cursor.execute("SELECT id, node.ToString() FROM #many ORDER BY node")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "/1/"), (2, "/1/1/"), (3, "/1/2/")]
