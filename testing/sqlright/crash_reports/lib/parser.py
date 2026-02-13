"""AFL crash filename parser."""

import re
from typing import Optional, Dict, Any
from pathlib import Path


class AFLCrashParser:
    """Parse AFL crash filenames to extract metadata."""

    # AFL crash filename format: id:NNNNNN,sig:NN[,src:NNNNNN][,sync:NAME][,op:...]
    PATTERN = re.compile(
        r'id:(?P<id>\d+),sig:(?P<sig>\d+)'
        r'(?:,src:(?P<src>\d+))?'
        r'(?:,sync:(?P<sync>[^,]+))?'
        r'(?:,.*)?'
    )

    @classmethod
    def parse(cls, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parse AFL crash filename.

        Returns dict with keys: afl_id, signal_number, source_id, sync_id
        Returns None if filename doesn't match AFL format.
        """
        match = cls.PATTERN.match(filename)
        if not match:
            return None

        return {
            'afl_id': int(match.group('id')),
            'signal_number': int(match.group('sig')),
            'source_id': match.group('src'),
            'sync_id': match.group('sync')
        }

    @classmethod
    def is_crash_file(cls, filename: str) -> bool:
        """Check if filename matches AFL crash format."""
        return cls.PATTERN.match(filename) is not None


def parse_crash_filename(file_path: Path) -> Dict[str, Any]:
    """
    Parse crash file and extract metadata.

    Returns:
        Dict with afl_id, signal_number, source_id, sync_id, file_name, file_size
        If parsing fails, returns dict with None values.
    """
    result = {
        'afl_id': None,
        'signal_number': None,
        'source_id': None,
        'sync_id': None,
        'file_name': file_path.name,
        'file_size': 0
    }

    try:
        result['file_size'] = file_path.stat().st_size
    except OSError:
        pass

    parsed = AFLCrashParser.parse(file_path.name)
    if parsed:
        result.update(parsed)

    return result
