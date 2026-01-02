"""
Comparison and diff utilities for pages and records.
"""

from typing import List, NamedTuple, Set, Tuple

from .page import get_cell_pointers, parse_page_header
from .record import get_index_rowids


class PageDiff(NamedTuple):
    """Result of comparing two page states."""

    bytes_changed: int
    total_bytes: int
    cell_count_before: int
    cell_count_after: int
    content_start_before: int
    content_start_after: int
    identical: bool


class CellPointerDiff(NamedTuple):
    """Result of comparing cell pointer arrays."""

    pointers_lost: Set[int]
    pointers_gained: Set[int]
    pointers_unchanged: Set[int]
    first_diff_index: int  # -1 if identical


class RowidDiff(NamedTuple):
    """Result of comparing rowid sets."""

    rowids_lost: Set[int]
    rowids_gained: Set[int]
    rowids_unchanged: Set[int]


def compare_pages(before: bytes, after: bytes) -> PageDiff:
    """
    Compare two page states byte-by-byte.

    Args:
        before: Page data before
        after: Page data after

    Returns:
        PageDiff with comparison results
    """
    # Count differing bytes
    min_len = min(len(before), len(after))
    bytes_changed = sum(1 for i in range(min_len) if before[i] != after[i])
    bytes_changed += abs(len(before) - len(after))

    before_header = parse_page_header(before)
    after_header = parse_page_header(after)

    return PageDiff(
        bytes_changed=bytes_changed,
        total_bytes=max(len(before), len(after)),
        cell_count_before=before_header.cell_count,
        cell_count_after=after_header.cell_count,
        content_start_before=before_header.cell_content_start,
        content_start_after=after_header.cell_content_start,
        identical=(bytes_changed == 0),
    )


def compare_cell_pointers(before: bytes, after: bytes, page_offset: int = 0) -> CellPointerDiff:
    """
    Compare cell pointer arrays between two page states.

    Args:
        before: Page data before
        after: Page data after
        page_offset: Header offset (100 for page 1)

    Returns:
        CellPointerDiff with comparison results
    """
    before_ptrs = set(get_cell_pointers(before, page_offset))
    after_ptrs = set(get_cell_pointers(after, page_offset))

    lost = before_ptrs - after_ptrs
    gained = after_ptrs - before_ptrs
    unchanged = before_ptrs & after_ptrs

    # Find first differing index
    before_list = get_cell_pointers(before, page_offset)
    after_list = get_cell_pointers(after, page_offset)

    first_diff = -1
    for i in range(min(len(before_list), len(after_list))):
        if before_list[i] != after_list[i]:
            first_diff = i
            break

    if first_diff == -1 and len(before_list) != len(after_list):
        first_diff = min(len(before_list), len(after_list))

    return CellPointerDiff(
        pointers_lost=lost,
        pointers_gained=gained,
        pointers_unchanged=unchanged,
        first_diff_index=first_diff,
    )


def compare_rowids(before: bytes, after: bytes, page_offset: int = 0) -> RowidDiff:
    """
    Compare rowids between two index page states.

    Args:
        before: Page data before
        after: Page data after
        page_offset: Header offset (100 for page 1)

    Returns:
        RowidDiff with comparison results
    """
    before_rowids = get_index_rowids(before, page_offset)
    after_rowids = get_index_rowids(after, page_offset)

    return RowidDiff(
        rowids_lost=before_rowids - after_rowids,
        rowids_gained=after_rowids - before_rowids,
        rowids_unchanged=before_rowids & after_rowids,
    )


def find_matching_bytes(page: bytes, candidates: List[Tuple[str, bytes]]) -> List[Tuple[str, int, float]]:
    """
    Find how well a page matches each candidate source.

    Args:
        page: Page to check
        candidates: List of (name, page_data) tuples

    Returns:
        List of (name, matching_bytes, percentage) sorted by match percentage
    """
    results = []

    for name, candidate in candidates:
        min_len = min(len(page), len(candidate))
        matching = sum(1 for i in range(min_len) if page[i] == candidate[i])
        percentage = (matching / len(page)) * 100 if len(page) > 0 else 0

        results.append((name, matching, percentage))

    return sorted(results, key=lambda x: x[2], reverse=True)
