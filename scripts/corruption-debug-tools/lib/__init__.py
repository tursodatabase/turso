"""
SQLite/Limbo corruption debugging library.

Provides utilities for parsing WAL files, database pages, and SQLite records.
"""

from .diff import (
    compare_cell_pointers,
    compare_pages,
    compare_rowids,
)
from .page import (
    PAGE_TYPES,
    get_cell_pointers,
    get_interior_children,
    get_page_at_frame,
    get_page_from_db,
    parse_page_header,
)
from .record import (
    decode_serial_type,
    get_index_keys,
    get_index_rowids,
    get_table_rowids,
    parse_record,
    read_varint,
)
from .wal import (
    FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
    create_truncated_wal,
    get_frame_count,
    get_frame_page,
    get_page_size_from_wal,
    iter_frames,
    parse_frame_header,
    parse_wal_header,
)

__all__ = [
    # WAL
    "WAL_HEADER_SIZE",
    "FRAME_HEADER_SIZE",
    "parse_wal_header",
    "parse_frame_header",
    "get_frame_count",
    "get_page_size_from_wal",
    "iter_frames",
    "get_frame_page",
    "create_truncated_wal",
    # Page
    "PAGE_TYPES",
    "get_page_from_db",
    "get_page_at_frame",
    "parse_page_header",
    "get_cell_pointers",
    "get_interior_children",
    # Record
    "read_varint",
    "decode_serial_type",
    "parse_record",
    "get_index_rowids",
    "get_index_keys",
    "get_table_rowids",
    # Diff
    "compare_pages",
    "compare_cell_pointers",
    "compare_rowids",
]
