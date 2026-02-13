"""O(n) crash file scanner with checkpointing."""

from pathlib import Path
from typing import List, Set, Optional
import logging

from .parser import AFLCrashParser

logger = logging.getLogger(__name__)


def find_crash_directories(base_path: Path) -> List[Path]:
    """
    Find all crash directories under base_path.

    Searches for directories matching:
    - {base_path}/*/crashes/
    - {base_path}/crashes/ (if exists)

    Returns sorted list of crash directories.
    """
    crash_dirs = []

    # Check for direct crashes/ subdirectory
    direct_crash_dir = base_path / "crashes"
    if direct_crash_dir.exists() and direct_crash_dir.is_dir():
        crash_dirs.append(direct_crash_dir)

    # Check for instance-level crashes (e.g., primary/crashes, secondary_*/crashes)
    for item in base_path.iterdir():
        if item.is_dir():
            crash_subdir = item / "crashes"
            if crash_subdir.exists() and crash_subdir.is_dir():
                crash_dirs.append(crash_subdir)

    return sorted(crash_dirs)


def find_all_crash_files(crash_dir: Path) -> List[Path]:
    """
    Find all crash files in a directory.

    Returns sorted list of crash files (by modification time).
    Only includes files matching AFL crash format.
    """
    if not crash_dir.exists():
        logger.warning(f"Crash directory does not exist: {crash_dir}")
        return []

    crash_files = []
    for file_path in crash_dir.iterdir():
        if file_path.is_file() and AFLCrashParser.is_crash_file(file_path.name):
            crash_files.append(file_path)

    # Sort by modification time for checkpoint-based processing
    return sorted(crash_files, key=lambda f: f.stat().st_mtime)


def get_unprocessed_crashes(session_path: Path, session_id: int, db, checkpoint: Optional[dict]) -> List[Path]:
    """
    Get list of crash files that haven't been processed yet.

    Uses checkpoint to skip files older than last processed file (O(n) efficiency).
    Double-checks against database to ensure idempotency.

    Args:
        session_path: Base path of fuzzing session
        session_id: Database session ID
        db: Database instance
        checkpoint: Checkpoint dict from database (or None)

    Returns:
        List of unprocessed crash files, sorted by mtime
    """
    # Find all crash directories under session path
    crash_dirs = find_crash_directories(session_path)
    logger.info(f"Found {len(crash_dirs)} crash directories in {session_path}")

    # Collect all crash files
    all_crashes = []
    for crash_dir in crash_dirs:
        crashes = find_all_crash_files(crash_dir)
        all_crashes.extend(crashes)

    logger.info(f"Found {len(all_crashes)} total crash files")

    # Filter based on checkpoint (O(n) optimization)
    if checkpoint and checkpoint['last_file_mtime']:
        last_mtime = checkpoint['last_file_mtime']
        new_files = [f for f in all_crashes if f.stat().st_mtime > last_mtime]
        logger.info(f"Checkpoint filter: {len(new_files)} files newer than checkpoint")
    else:
        new_files = all_crashes
        logger.info("No checkpoint - processing all files")

    # Double-check against database (ensures idempotency even if checkpoint was lost)
    processed_paths = db.get_processed_file_paths(session_id)
    unprocessed = [f for f in new_files if str(f) not in processed_paths]

    logger.info(f"Final unprocessed count: {len(unprocessed)}")

    return sorted(unprocessed, key=lambda f: f.stat().st_mtime)


def scan_session(session_path: Path, db, batch_size: int = 100) -> dict:
    """
    Scan a fuzzing session for crashes and add to database.

    Processes crashes in batches with checkpoint updates for incremental processing.

    Args:
        session_path: Path to fuzzing session directory
        db: Database instance
        batch_size: Number of files to process before committing checkpoint

    Returns:
        Dict with statistics: files_processed, crashes_added, skipped
    """
    from .parser import parse_crash_filename

    stats = {
        'files_processed': 0,
        'crashes_added': 0,
        'skipped': 0,
        'errors': 0
    }

    # Register session
    session_id = db.register_session(str(session_path))

    # Get checkpoint
    checkpoint = db.get_checkpoint(session_id)

    # Find unprocessed crashes
    unprocessed = get_unprocessed_crashes(session_path, session_id, db, checkpoint)

    if not unprocessed:
        logger.info("No new crashes to process")
        return stats

    logger.info(f"Processing {len(unprocessed)} crashes in batches of {batch_size}")

    # Process in batches
    for i in range(0, len(unprocessed), batch_size):
        batch = unprocessed[i:i + batch_size]
        batch_stats = process_batch(batch, session_id, db)

        stats['files_processed'] += batch_stats['processed']
        stats['crashes_added'] += batch_stats['crashes_added']
        stats['skipped'] += batch_stats['skipped']
        stats['errors'] += batch_stats['errors']

        # Update checkpoint after each batch
        if batch:
            last_file = batch[-1]
            last_mtime = last_file.stat().st_mtime
            db.update_checkpoint(session_id, str(last_file), last_mtime, batch_stats['processed'])
            logger.info(f"Checkpoint updated: {batch_stats['processed']} files processed")

    # Update session crash count
    db.update_session_crash_count(session_id)

    return stats


def process_batch(batch: List[Path], session_id: int, db) -> dict:
    """
    Process a batch of crash files.

    Returns dict with processed, crashes_added, skipped, errors counts.
    """
    from .parser import parse_crash_filename

    stats = {
        'processed': 0,
        'crashes_added': 0,
        'skipped': 0,
        'errors': 0
    }

    for file_path in batch:
        try:
            # Check if file still exists (may have been deleted)
            if not file_path.exists():
                logger.warning(f"File deleted during scan: {file_path}")
                stats['skipped'] += 1
                continue

            # Read crash content
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    sql_content = f.read()
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                stats['errors'] += 1
                continue

            # Skip empty files
            if not sql_content.strip():
                logger.warning(f"Empty crash file: {file_path}")
                stats['skipped'] += 1
                continue

            # Parse filename
            metadata = parse_crash_filename(file_path)

            # Find or create crash
            crash_id = db.find_or_create_crash(sql_content, metadata['signal_number'])

            # Add crash instance
            db.add_crash_instance(
                crash_id=crash_id,
                session_id=session_id,
                file_path=str(file_path),
                file_name=metadata['file_name'],
                afl_id=metadata['afl_id'],
                signal_number=metadata['signal_number'],
                source_id=metadata['source_id'],
                sync_id=metadata['sync_id'],
                file_size=metadata['file_size']
            )

            stats['processed'] += 1
            stats['crashes_added'] += 1

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            stats['errors'] += 1

    return stats
