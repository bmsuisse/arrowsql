"""A principal in a database mirroring session sends ENVCHANGE type 13 (its
partner's name) with the login response and again when a batch switches to the
mirrored database. msodbcsql only notes it when no Failover_Partner is
configured; neither the login nor the batch may fail on it.

mssql-mock-tds cannot send this token, so this uses a minimal raw TDS
responder: PRELOGIN without encryption, the mock's LOGINACK and ENVCHANGEs plus
the mirroring one, then a database and mirroring ENVCHANGE and a DONE for every
later request.
"""

import socket
import struct
import threading

import mssql_python


def _packet(body: bytes, ptype: int = 0x04) -> bytes:
    return struct.pack(">BBHHBB", ptype, 0x01, 8 + len(body), 0, 1, 0) + body


def _b_varchar(s: str) -> bytes:
    return bytes([len(s)]) + s.encode("utf-16-le")


def _envchange(subtype: int, payload: bytes) -> bytes:
    body = bytes([subtype]) + payload
    return b"\xe3" + struct.pack("<H", len(body)) + body


def _login_response(partner: str) -> bytes:
    prog = _b_varchar("FakeTds")
    ack = (
        b"\x01"
        + struct.pack(">I", 0x74000004)
        + prog
        + bytes([16, 0])
        + struct.pack("<H", 0)
    )
    tokens = b"\xad" + struct.pack("<H", len(ack)) + ack
    tokens += _envchange(7, b"\x05" + struct.pack("<I", 0x09040000) + b"\xd0" + b"\x00")
    tokens += _envchange(1, _b_varchar("master") + b"\x00")
    tokens += _envchange(4, _b_varchar("4096") + _b_varchar("4096"))
    tokens += _envchange(13, _b_varchar(partner) + b"\x00")
    return tokens + _done()


def _done() -> bytes:
    return b"\xfd" + struct.pack("<HHQ", 0, 0x00C1, 0)


def _read_message(conn: socket.socket) -> bytes | None:
    body = b""
    while True:
        header = b""
        while len(header) < 8:
            chunk = conn.recv(8 - len(header))
            if not chunk:
                return None
            header += chunk
        length, status = struct.unpack(">H", header[2:4])[0], header[1]
        remaining = length - 8
        while remaining:
            chunk = conn.recv(remaining)
            if not chunk:
                return None
            body += chunk
            remaining -= len(chunk)
        if status & 0x01:
            return header[:1] + body


def _serve(listener: socket.socket, partner: str) -> None:
    conn, _ = listener.accept()
    with conn:
        _read_message(conn)  # PRELOGIN
        version, encryption = bytes([16, 0, 0, 0, 0, 0]), b"\x02"  # ENCRYPT_NOT_SUP
        directory = (
            b"\x00"
            + struct.pack(">HH", 11, 6)
            + b"\x01"
            + struct.pack(">HH", 17, 1)
            + b"\xff"
        )
        conn.sendall(_packet(directory + version + encryption))
        _read_message(conn)  # LOGIN7
        conn.sendall(_packet(_login_response(partner)))
        while _read_message(conn) is not None:
            switch = _envchange(1, _b_varchar("mirrored") + _b_varchar("master"))
            conn.sendall(
                _packet(
                    switch + _envchange(13, _b_varchar(partner) + b"\x00") + _done()
                )
            )


def test_database_mirroring_partner_envchange_is_accepted():
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = threading.Thread(
        target=_serve, args=(listener, "MIRROR\\INST"), daemon=True
    )
    server.start()
    try:
        conn = mssql_python.connect(
            f"Server=127.0.0.1,{port};UID=u;PWD=p;Encrypt=no", timeout=10
        )
        try:
            cur = conn.cursor()
            cur.execute("USE mirrored")
            cur.execute("USE mirrored")
        finally:
            conn.close()
    finally:
        listener.close()
        server.join(5)
