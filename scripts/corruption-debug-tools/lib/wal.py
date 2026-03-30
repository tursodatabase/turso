"""
WAL (Write-Ahead Log) parsing utilities.

SQLite WAL format:
- 32-byte header
- Sequence of frames, each containing:
  - 24-byte frame header
  - Page data (page_size bytes)
"""

import os
import struct
from typing import Iterator, NamedTuple, Optional

WAL_HEADER_SIZE = 32
FRAME_HEADER_SIZE = 24


class WalHeader(NamedTuple):
    """Parsed WAL header."""

    magic: int
    version: int
    page_size: int
    checkpoint_seq: int
    salt1: int
    salt2: int
    checksum1: int
    checksum2: int

    @property
    def is_big_endian(self) -> bool:
        return self.magic == 0x377F0682

    @property
    def is_little_endian(self) -> bool:
        return self.magic == 0x377F0683


class FrameHeader(NamedTuple):
    """Parsed frame header."""

    page_num: int
    db_size: int  # Non-zero indicates commit frame
    salt1: int
    salt2: int
    checksum1: int
    checksum2: int

    @property
    def is_commit(self) -> bool:
        return self.db_size > 0


class Frame(NamedTuple):
    """A WAL frame with header and page data."""

    index: int  # 0-indexed frame number
    header: FrameHeader
    page_data: bytes
    offset: int  # Byte offset in WAL file


def parse_wal_header(data: bytes) -> WalHeader:
    """Parse the 32-byte WAL header."""
    if len(data) < WAL_HEADER_SIZE:
        raise ValueError(f"WAL header too short: {len(data)} bytes")

    magic, version, page_size, checkpoint_seq, salt1, salt2, checksum1, checksum2 = struct.unpack(
        ">IIIIIIII", data[:32]
    )
    return WalHeader(
        magic=magic,
        version=version,
        page_size=page_size,
        checkpoint_seq=checkpoint_seq,
        salt1=salt1,
        salt2=salt2,
        checksum1=checksum1,
        checksum2=checksum2,
    )


def parse_frame_header(data: bytes) -> FrameHeader:
    """Parse a 24-byte frame header."""
    if len(data) < FRAME_HEADER_SIZE:
        raise ValueError(f"Frame header too short: {len(data)} bytes")

    page_num, db_size, salt1, salt2, checksum1, checksum2 = struct.unpack(">IIIIII", data[:24])
    return FrameHeader(
        page_num=page_num,
        db_size=db_size,
        salt1=salt1,
        salt2=salt2,
        checksum1=checksum1,
        checksum2=checksum2,
    )


def get_page_size_from_wal(wal_path: str) -> int:
    """Get page size from WAL header."""
    with open(wal_path, "rb") as f:
        header = parse_wal_header(f.read(WAL_HEADER_SIZE))
    return header.page_size


def get_frame_count(wal_path: str, page_size: Optional[int] = None) -> int:
    """Calculate number of frames in WAL file."""
    if page_size is None:
        page_size = get_page_size_from_wal(wal_path)

    frame_size = FRAME_HEADER_SIZE + page_size
    wal_size = os.path.getsize(wal_path)
    return (wal_size - WAL_HEADER_SIZE) // frame_size


def iter_frames(wal_path: str, page_size: Optional[int] = None) -> Iterator[Frame]:
    """Iterate through all frames in a WAL file."""
    if page_size is None:
        page_size = get_page_size_from_wal(wal_path)

    frame_size = FRAME_HEADER_SIZE + page_size

    with open(wal_path, "rb") as f:
        # Skip WAL header
        f.seek(WAL_HEADER_SIZE)

        frame_idx = 0
        while True:
            offset = WAL_HEADER_SIZE + frame_idx * frame_size
            frame_data = f.read(frame_size)

            if len(frame_data) < frame_size:
                break  # End of file or incomplete frame

            header = parse_frame_header(frame_data[:FRAME_HEADER_SIZE])
            page_data = frame_data[FRAME_HEADER_SIZE:]

            yield Frame(
                index=frame_idx,
                header=header,
                page_data=page_data,
                offset=offset,
            )

            frame_idx += 1


def get_frame_page(wal_path: str, frame_num: int, page_size: Optional[int] = None) -> bytes:
    """Get page data from a specific WAL frame (1-indexed)."""
    if page_size is None:
        page_size = get_page_size_from_wal(wal_path)

    frame_size = FRAME_HEADER_SIZE + page_size
    offset = WAL_HEADER_SIZE + (frame_num - 1) * frame_size

    with open(wal_path, "rb") as f:
        f.seek(offset + FRAME_HEADER_SIZE)
        return f.read(page_size)


def create_truncated_wal(wal_path: str, num_frames: int, output_path: str, page_size: Optional[int] = None) -> None:
    """Create a WAL file with only the first num_frames frames."""
    if page_size is None:
        page_size = get_page_size_from_wal(wal_path)

    frame_size = FRAME_HEADER_SIZE + page_size
    size = WAL_HEADER_SIZE + (num_frames * frame_size)

    with open(wal_path, "rb") as src:
        data = src.read(size)

    with open(output_path, "wb") as dst:
        dst.write(data)
