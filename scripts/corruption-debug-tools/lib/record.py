"""
SQLite record format parsing utilities.

SQLite records consist of:
- Header: varint header size, followed by serial types for each column
- Body: column values in order

Serial types:
- 0: NULL
- 1: 8-bit signed integer
- 2: 16-bit big-endian signed integer
- 3: 24-bit big-endian signed integer
- 4: 32-bit big-endian signed integer
- 5: 48-bit big-endian signed integer
- 6: 64-bit big-endian signed integer
- 7: IEEE 754 64-bit float
- 8: integer 0
- 9: integer 1
- 10, 11: reserved
- N >= 12 even: BLOB of (N-12)/2 bytes
- N >= 13 odd: TEXT of (N-13)/2 bytes
"""

import struct
from typing import Any, List, NamedTuple, Set, Tuple

from .page import get_cell_pointers, parse_page_header


class SerialType(NamedTuple):
    """Decoded serial type information."""

    type_code: int
    type_name: str
    byte_size: int


class RecordValue(NamedTuple):
    """A parsed record value."""

    serial_type: SerialType
    value: Any
    raw_bytes: bytes


class IndexKey(NamedTuple):
    """An index key with its associated rowid."""

    key_values: List[Any]
    rowid: int
    raw_payload: bytes


def read_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """
    Read a SQLite varint from data at offset.

    Returns:
        Tuple of (value, bytes_consumed)
    """
    result = 0
    bytes_read = 0

    for i in range(9):
        if offset + i >= len(data):
            break

        byte = data[offset + i]
        bytes_read += 1

        if i < 8:
            result = (result << 7) | (byte & 0x7F)
            if byte < 0x80:
                break
        else:
            # 9th byte uses all 8 bits
            result = (result << 8) | byte

    return result, bytes_read


def decode_serial_type(type_code: int) -> SerialType:  # noqa: C901
    """Decode a serial type code into type information."""
    if type_code == 0:
        return SerialType(type_code, "NULL", 0)
    elif type_code == 1:
        return SerialType(type_code, "INT8", 1)
    elif type_code == 2:
        return SerialType(type_code, "INT16", 2)
    elif type_code == 3:
        return SerialType(type_code, "INT24", 3)
    elif type_code == 4:
        return SerialType(type_code, "INT32", 4)
    elif type_code == 5:
        return SerialType(type_code, "INT48", 6)
    elif type_code == 6:
        return SerialType(type_code, "INT64", 8)
    elif type_code == 7:
        return SerialType(type_code, "FLOAT64", 8)
    elif type_code == 8:
        return SerialType(type_code, "ZERO", 0)
    elif type_code == 9:
        return SerialType(type_code, "ONE", 0)
    elif type_code >= 12 and type_code % 2 == 0:
        size = (type_code - 12) // 2
        return SerialType(type_code, f"BLOB({size})", size)
    elif type_code >= 13 and type_code % 2 == 1:
        size = (type_code - 13) // 2
        return SerialType(type_code, f"TEXT({size})", size)
    else:
        return SerialType(type_code, f"RESERVED({type_code})", 0)


def parse_record(payload: bytes) -> List[RecordValue]:  # noqa: C901
    """
    Parse a SQLite record payload into values.

    Args:
        payload: Record payload bytes (after cell header)

    Returns:
        List of RecordValue for each column
    """
    # Read header size
    header_size, n = read_varint(payload, 0)
    pos = n

    # Read serial types
    serial_types = []
    while pos < header_size:
        st, n = read_varint(payload, pos)
        serial_types.append(decode_serial_type(st))
        pos += n

    # Read values
    values = []
    data_pos = header_size

    for st in serial_types:
        raw = payload[data_pos : data_pos + st.byte_size]

        if st.type_name == "NULL":
            value = None
        elif st.type_name == "ZERO":
            value = 0
        elif st.type_name == "ONE":
            value = 1
        elif st.type_name == "INT8":
            value = struct.unpack(">b", raw)[0]
        elif st.type_name == "INT16":
            value = struct.unpack(">h", raw)[0]
        elif st.type_name == "INT24":
            # Sign-extend 24-bit value
            if raw[0] & 0x80:
                value = struct.unpack(">i", b"\xff" + raw)[0]
            else:
                value = struct.unpack(">i", b"\x00" + raw)[0]
        elif st.type_name == "INT32":
            value = struct.unpack(">i", raw)[0]
        elif st.type_name == "INT48":
            # Sign-extend 48-bit value
            if raw[0] & 0x80:
                value = struct.unpack(">q", b"\xff\xff" + raw)[0]
            else:
                value = struct.unpack(">q", b"\x00\x00" + raw)[0]
        elif st.type_name == "INT64":
            value = struct.unpack(">q", raw)[0]
        elif st.type_name == "FLOAT64":
            value = struct.unpack(">d", raw)[0]
        elif st.type_name.startswith("BLOB"):
            value = raw
        elif st.type_name.startswith("TEXT"):
            try:
                value = raw.decode("utf-8")
            except UnicodeDecodeError:
                value = raw  # Return as bytes if not valid UTF-8
        else:
            value = raw

        values.append(RecordValue(st, value, raw))
        data_pos += st.byte_size

    return values


def get_index_rowids(page_data: bytes, page_offset: int = 0) -> Set[int]:
    """
    Extract all rowids from an index leaf page.

    In an index, the last column of each record is the rowid.

    Args:
        page_data: Page bytes
        page_offset: Header offset (100 for page 1)

    Returns:
        Set of rowids found in the page
    """
    header = parse_page_header(page_data, page_offset)

    if header.page_type != 0x0A:  # Not a leaf index
        return set()

    rowids = set()
    pointers = get_cell_pointers(page_data, page_offset)

    for ptr in pointers:
        # Index leaf cell: varint payload_size, payload
        payload_size, n = read_varint(page_data, ptr)
        payload = page_data[ptr + n : ptr + n + payload_size]

        try:
            values = parse_record(payload)
            if values:
                # Last value is the rowid
                rowid = values[-1].value
                if isinstance(rowid, int):
                    rowids.add(rowid)
        except Exception:
            pass  # Skip malformed records

    return rowids


def get_index_keys(page_data: bytes, page_offset: int = 0) -> List[IndexKey]:
    """
    Extract all keys with their rowids from an index leaf page.

    Args:
        page_data: Page bytes
        page_offset: Header offset (100 for page 1)

    Returns:
        List of IndexKey objects
    """
    header = parse_page_header(page_data, page_offset)

    if header.page_type != 0x0A:  # Not a leaf index
        return []

    keys = []
    pointers = get_cell_pointers(page_data, page_offset)

    for ptr in pointers:
        # Index leaf cell: varint payload_size, payload
        payload_size, n = read_varint(page_data, ptr)
        payload = page_data[ptr + n : ptr + n + payload_size]

        try:
            values = parse_record(payload)
            if values:
                # Last value is rowid, rest are key columns
                rowid = values[-1].value
                key_values = [v.value for v in values[:-1]]

                if isinstance(rowid, int):
                    keys.append(IndexKey(key_values, rowid, payload))
        except Exception:
            pass  # Skip malformed records

    return keys


def get_table_rowids(page_data: bytes, page_offset: int = 0) -> Set[int]:
    """
    Extract all rowids from a table leaf page.

    Args:
        page_data: Page bytes
        page_offset: Header offset (100 for page 1)

    Returns:
        Set of rowids found in the page
    """
    header = parse_page_header(page_data, page_offset)

    if header.page_type != 0x0D:  # Not a leaf table
        return set()

    rowids = set()
    pointers = get_cell_pointers(page_data, page_offset)

    for ptr in pointers:
        # Table leaf cell: varint payload_size, varint rowid, payload
        payload_size, n1 = read_varint(page_data, ptr)
        rowid, n2 = read_varint(page_data, ptr + n1)
        rowids.add(rowid)

    return rowids
