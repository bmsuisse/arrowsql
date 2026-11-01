"""Review r4 S13: parse errors must not echo value fragments."""

import pytest

from arrowsql.connection_string_parser import _ConnectionStringParser
from arrowsql.exceptions import ConnectionStringParseError


def _errors(conn):
    with pytest.raises(ConnectionStringParseError) as ei:
        _ConnectionStringParser()._parse(conn)
    return str(ei.value)


def test_password_fragment_without_equals_not_echoed():
    msg = _errors("Server=x;PWD=ab;Sup3r!Secret;UID=u")
    assert "Incomplete specification" in msg
    assert "Sup3r" not in msg and "position" in msg


def test_password_fragment_with_equals_not_echoed():
    msg = _errors("Server=x;PWD=ab;p@ss/w0rd!=;UID=u")
    assert "p@ss" not in msg and "position" in msg


def test_known_keyword_still_named():
    assert "'server'" in _errors("Server;Database=mydb").lower()
