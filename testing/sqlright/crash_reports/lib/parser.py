"""AFL crash/hang filename parser."""

import re
from pathlib import Path
from typing import Any, Dict, Optional


class AFLCrashParser:
    """Parse AFL crash and hang filenames to extract metadata."""

    # AFL crash filename format: id:NNNNNN,sig:NN[,src:NNNNNN][,sync:NAME][,op:...]
    CRASH_PATTERN = re.compile(
        r"id:(?P<id>\d+),sig:(?P<sig>\d+)"
        r"(?:,src:(?P<src>\d+))?"
        r"(?:,sync:(?P<sync>[^,]+))?"
        r"(?:,.*)?"
    )

    # AFL hang filename format: id:NNNNNN,src:NNNNNN,op:... (no sig: field)
    HANG_PATTERN = re.compile(
        r"id:(?P<id>\d+)"
        r"(?:,src:(?P<src>\d+))?"
        r"(?:,sync:(?P<sync>[^,]+))?"
        r"(?:,.*)?"
    )

    @classmethod
    def parse(cls, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parse AFL crash or hang filename.

        Returns dict with keys: afl_id, signal_number, source_id, sync_id
        Returns None if filename doesn't match AFL format.
        signal_number is None for hangs.
        """
        # Try crash pattern first (more specific, has sig: field)
        match = cls.CRASH_PATTERN.match(filename)
        if match:
            return {
                "afl_id": int(match.group("id")),
                "signal_number": int(match.group("sig")),
                "source_id": match.group("src"),
                "sync_id": match.group("sync")
            }

        # Try hang pattern (no sig: field)
        match = cls.HANG_PATTERN.match(filename)
        if match:
            return {
                "afl_id": int(match.group("id")),
                "signal_number": None,
                "source_id": match.group("src"),
                "sync_id": match.group("sync")
            }

        return None

    @classmethod
    def is_afl_file(cls, filename: str) -> bool:
        """Check if filename matches AFL crash or hang format."""
        return cls.CRASH_PATTERN.match(filename) is not None or cls.HANG_PATTERN.match(filename) is not None

    @classmethod
    def is_crash_file(cls, filename: str) -> bool:
        """Check if filename matches AFL crash or hang format. Alias for is_afl_file."""
        return cls.is_afl_file(filename)


def parse_crash_filename(file_path: Path) -> Dict[str, Any]:
    """
    Parse crash/hang file and extract metadata.

    Returns:
        Dict with afl_id, signal_number, source_id, sync_id, file_name, file_size
        If parsing fails, returns dict with None values.
    """
    result = {
        "afl_id": None,
        "signal_number": None,
        "source_id": None,
        "sync_id": None,
        "file_name": file_path.name,
        "file_size": 0
    }

    try:
        result["file_size"] = file_path.stat().st_size
    except OSError:
        pass

    parsed = AFLCrashParser.parse(file_path.name)
    if parsed:
        result.update(parsed)

    return result
