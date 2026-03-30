"""
SQLite page parsing utilities.

Page types:
- 0x02: Interior index B-tree page
- 0x05: Interior table B-tree page
- 0x0a: Leaf index B-tree page
- 0x0d: Leaf table B-tree page
"""

import struct
from typing import List, NamedTuple, Optional

from .wal import (
    FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
    get_page_size_from_wal,
)

PAGE_TYPES = {
    0x02: "interior index",
    0x05: "interior table",
    0x0A: "leaf index",
    0x0D: "leaf table",
}


class PageHeader(NamedTuple):
    """Parsed B-tree page header."""

    page_type: int
    type_name: str
    first_freeblock: int
    cell_count: int
    cell_content_start: int
    fragmented_bytes: int
    rightmost_ptr: Optional[int]  # Only for interior pages
    header_size: int

    @property
    def is_leaf(self) -> bool:
        return self.page_type in (0x0A, 0x0D)

    @property
    def is_interior(self) -> bool:
        return self.page_type in (0x02, 0x05)

    @property
    def is_index(self) -> bool:
        return self.page_type in (0x02, 0x0A)

    @property
    def is_table(self) -> bool:
        return self.page_type in (0x05, 0x0D)


def get_page_from_db(db_path: str, page_num: int, page_size: int = 4096) -> bytes:
    """Read a page directly from the database file (1-indexed)."""
    with open(db_path, "rb") as f:
        f.seek((page_num - 1) * page_size)
        return f.read(page_size)


def get_page_at_frame(
    db_path: str, wal_path: str, page_num: int, up_to_frame: int, page_size: Optional[int] = None
) -> bytes:
    """
    Get page state after applying WAL frames up to (but not including) up_to_frame.

    Args:
        db_path: Path to database file
        wal_path: Path to WAL file
        page_num: Page number (1-indexed)
        up_to_frame: Apply frames 0 to up_to_frame-1 (0 means DB file only)
        page_size: Page size (auto-detected from WAL if not provided)

    Returns:
        Page data bytes
    """
    if page_size is None:
        page_size = get_page_size_from_wal(wal_path)

    # Start with page from DB file
    page_data = get_page_from_db(db_path, page_num, page_size)

    if up_to_frame == 0:
        return page_data

    # Apply WAL frames
    frame_size = FRAME_HEADER_SIZE + page_size

    with open(wal_path, "rb") as f:
        wal_data = f.read()

    for frame_idx in range(up_to_frame):
        offset = WAL_HEADER_SIZE + frame_idx * frame_size
        if offset + 4 > len(wal_data):
            break

        frame_page_num = struct.unpack(">I", wal_data[offset : offset + 4])[0]
        if frame_page_num == page_num:
            page_data = wal_data[offset + FRAME_HEADER_SIZE : offset + frame_size]

    return page_data


def parse_page_header(data: bytes, page_offset: int = 0) -> PageHeader:
    """
    Parse B-tree page header.

    Args:
        data: Page data
        page_offset: Offset within page for header (100 for page 1 due to DB header)

    Returns:
        Parsed PageHeader
    """
    base = page_offset
    page_type = data[base]
    first_freeblock = struct.unpack(">H", data[base + 1 : base + 3])[0]
    cell_count = struct.unpack(">H", data[base + 3 : base + 5])[0]
    cell_content_start = struct.unpack(">H", data[base + 5 : base + 7])[0]

    # 0 means 65536
    if cell_content_start == 0:
        cell_content_start = 65536

    fragmented_bytes = data[base + 7]

    # Interior pages have a 4-byte rightmost pointer
    if page_type in (0x02, 0x05):
        rightmost_ptr = struct.unpack(">I", data[base + 8 : base + 12])[0]
        header_size = 12
    else:
        rightmost_ptr = None
        header_size = 8

    return PageHeader(
        page_type=page_type,
        type_name=PAGE_TYPES.get(page_type, f"unknown (0x{page_type:02x})"),
        first_freeblock=first_freeblock,
        cell_count=cell_count,
        cell_content_start=cell_content_start,
        fragmented_bytes=fragmented_bytes,
        rightmost_ptr=rightmost_ptr,
        header_size=header_size,
    )


def get_cell_pointers(data: bytes, page_offset: int = 0) -> List[int]:
    """
    Get list of cell pointers from a page.

    Args:
        data: Page data
        page_offset: Offset for page header (100 for page 1)

    Returns:
        List of cell pointer offsets
    """
    header = parse_page_header(data, page_offset)
    ptr_array_start = page_offset + header.header_size

    pointers = []
    for i in range(header.cell_count):
        ptr_offset = ptr_array_start + i * 2
        ptr = struct.unpack(">H", data[ptr_offset : ptr_offset + 2])[0]
        pointers.append(ptr)

    return pointers


def get_interior_children(data: bytes, page_offset: int = 0) -> List[int]:
    """
    Get child page numbers from an interior B-tree page.

    Returns list of child page numbers. For interior pages, each cell has a
    left child pointer, plus there's a rightmost pointer in the header.

    Args:
        data: Page data
        page_offset: Offset for page header (100 for page 1)

    Returns:
        List of child page numbers
    """
    header = parse_page_header(data, page_offset)

    if not header.is_interior:
        return []

    children = []
    ptr_array_start = page_offset + header.header_size

    for i in range(header.cell_count):
        # Get cell pointer
        ptr_offset = ptr_array_start + i * 2
        cell_ptr = struct.unpack(">H", data[ptr_offset : ptr_offset + 2])[0]

        # Interior cell starts with 4-byte left child pointer
        left_child = struct.unpack(">I", data[cell_ptr : cell_ptr + 4])[0]
        children.append(left_child)

    # Add rightmost pointer
    if header.rightmost_ptr:
        children.append(header.rightmost_ptr)

    return children
