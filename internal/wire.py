"""
wire.py - length-prefixed message framing over a stream socket.

TCP is a byte stream, not a message stream: a single recv() returns whatever
has arrived so far, so a payload that spans segments comes back short no
matter how large the buffer is. Every message here carries its length up
front, so the receiver knows exactly how many bytes to collect before handing
them up, and a message of any size survives the trip intact.
"""

import struct

HEADER = struct.Struct("!I")           # 4-byte big-endian payload length
MAX_MESSAGE_BYTES = 256 * 1024 * 1024  # guard against a garbled header
CHUNK = 65536


def send_message(sock, payload):
    """
    Writes one framed message. sendall, not send, because send is free to
    write fewer bytes than it was handed.
    """
    sock.sendall(HEADER.pack(len(payload)) + payload)


def recv_message(sock):
    """
    Returns the next whole message as bytes, or None if the peer closed
    cleanly between messages. Raises ConnectionError if a message is cut off
    partway through, so a truncated payload is never mistaken for a short one.
    """
    header = _recv_exactly(sock, HEADER.size)
    if header is None:
        return None

    (length,) = HEADER.unpack(header)
    if length > MAX_MESSAGE_BYTES:
        raise ValueError("refusing a %d byte message" % length)

    payload = _recv_exactly(sock, length)
    if payload is None:
        raise ConnectionError("peer closed before the %d byte payload arrived" % length)
    return payload


def _recv_exactly(sock, n):
    """
    Collects exactly n bytes, or returns None if the peer closed before
    sending any of them.
    """
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(min(remaining, CHUNK))
        if not chunk:
            if remaining == n:
                return None  # clean close, nothing half-read
            raise ConnectionError("peer closed after %d of %d bytes" % (n - remaining, n))
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
