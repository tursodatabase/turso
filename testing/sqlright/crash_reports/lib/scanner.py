"""O(n) crash/hang file scanner with checkpointing."""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from .parser import AFLCrashParser

logger = logging.getLogger(__name__)


def find_afl_directories(base_path: Path) -> List[Tuple[Path, str]]:
    """
    Find all crash and hang directories under base_path.

    Searches for directories matching:
    - {base_path}/*/crashes/  and  {base_path}/*/hangs/
    - {base_path}/crashes/    and  {base_path}/hangs/

    Returns sorted list of (directory, finding_type) tuples.
    """
    afl_dirs = []

    for subdir_name, finding_type in [("crashes", "crash"), ("hangs", "hang")]:
        # Check for direct subdirectory
        direct_dir = base_path / subdir_name
        if direct_dir.exists() and direct_dir.is_dir():
            afl_dirs.append((direct_dir, finding_type))

        # Check for instance-level dirs (e.g., primary/crashes, secondary_*/hangs)
        for item in base_path.iterdir():
            if item.is_dir():
                sub = item / subdir_name
                if sub.exists() and sub.is_dir():
                    afl_dirs.append((sub, finding_type))

    return sorted(afl_dirs, key=lambda x: x[0])


def find_crash_directories(base_path: Path) -> List[Path]:
    """
    Find all crash directories under base_path. Kept for backward compatibility.
    """
    return [d for d, _ in find_afl_directories(base_path)]


def find_all_afl_files(afl_dir: Path) -> List[Path]:
    """
    Find all AFL crash/hang files in a directory.

    Returns sorted list of files (by modification time).
    Only includes files matching AFL filename format.
    """
    if not afl_dir.exists():
        logger.warning(f"Directory does not exist: {afl_dir}")
        return []

    afl_files = []
    for file_path in afl_dir.iterdir():
        if file_path.is_file() and AFLCrashParser.is_afl_file(file_path.name):
            afl_files.append(file_path)

    # Sort by modification time for checkpoint-based processing
    return sorted(afl_files, key=lambda f: f.stat().st_mtime)


def get_unprocessed_crashes(
    session_path: Path, session_id: int, db, checkpoint: Optional[dict]
) -> List[Tuple[Path, str]]:
    """
    Get list of crash/hang files that haven't been processed yet.

    Uses checkpoint to skip files older than last processed file (O(n) efficiency).
    Double-checks against database to ensure idempotency.

    Args:
        session_path: Base path of fuzzing session
        session_id: Database session ID
        db: Database instance
        checkpoint: Checkpoint dict from database (or None)

    Returns:
        List of (file_path, finding_type) tuples for unprocessed files, sorted by mtime
    """
    # Find all AFL directories under session path
    afl_dirs = find_afl_directories(session_path)
    logger.info(f"Found {len(afl_dirs)} AFL directories in {session_path}")

    # Collect all files with their finding type
    all_files = []  # (path, finding_type)
    for afl_dir, finding_type in afl_dirs:
        files = find_all_afl_files(afl_dir)
        for f in files:
            all_files.append((f, finding_type))

    logger.info(f"Found {len(all_files)} total AFL files (crashes + hangs)")

    # Filter based on checkpoint (O(n) optimization)
    if checkpoint and checkpoint["last_file_mtime"]:
        last_mtime = checkpoint["last_file_mtime"]
        new_files = [(f, ft) for f, ft in all_files if f.stat().st_mtime > last_mtime]
        logger.info(f"Checkpoint filter: {len(new_files)} files newer than checkpoint")
    else:
        new_files = all_files
        logger.info("No checkpoint - processing all files")

    # Double-check against database (ensures idempotency even if checkpoint was lost)
    processed_paths = db.get_processed_file_paths(session_id)
    unprocessed = [(f, ft) for f, ft in new_files if str(f) not in processed_paths]

    logger.info(f"Final unprocessed count: {len(unprocessed)}")

    return sorted(unprocessed, key=lambda x: x[0].stat().st_mtime)


def scan_session(session_path: Path, db, batch_size: int = 100) -> dict:
    """
    Scan a fuzzing session for crashes/hangs and add to database.

    Processes files in batches with checkpoint updates for incremental processing.

    Args:
        session_path: Path to fuzzing session directory
        db: Database instance
        batch_size: Number of files to process before committing checkpoint

    Returns:
        Dict with statistics: files_processed, crashes_added, skipped
    """
    stats = {
        "files_processed": 0,
        "crashes_added": 0,
        "skipped": 0,
        "errors": 0
    }

    # Register session
    session_id = db.register_session(str(session_path))

    # Get checkpoint
    checkpoint = db.get_checkpoint(session_id)

    # Find unprocessed files (crashes and hangs)
    unprocessed = get_unprocessed_crashes(session_path, session_id, db, checkpoint)

    if not unprocessed:
        logger.info("No new crashes/hangs to process")
        return stats

    logger.info(f"Processing {len(unprocessed)} files in batches of {batch_size}")

    # Process in batches
    for i in range(0, len(unprocessed), batch_size):
        batch = unprocessed[i:i + batch_size]
        batch_stats = process_batch(batch, session_id, db)

        stats["files_processed"] += batch_stats["processed"]
        stats["crashes_added"] += batch_stats["crashes_added"]
        stats["skipped"] += batch_stats["skipped"]
        stats["errors"] += batch_stats["errors"]

        # Update checkpoint after each batch
        if batch:
            last_file = batch[-1][0]
            last_mtime = last_file.stat().st_mtime
            db.update_checkpoint(session_id, str(last_file), last_mtime, batch_stats["processed"])
            logger.info(f"Checkpoint updated: {batch_stats['processed']} files processed")

    # Update session crash count
    db.update_session_crash_count(session_id)

    return stats


def process_batch(batch: List[Tuple[Path, str]], session_id: int, db) -> dict:
    """
    Process a batch of crash/hang files.

    Args:
        batch: List of (file_path, finding_type) tuples
        session_id: Database session ID
        db: Database instance

    Returns dict with processed, crashes_added, skipped, errors counts.
    """
    from .parser import parse_crash_filename

    stats = {
        "processed": 0,
        "crashes_added": 0,
        "skipped": 0,
        "errors": 0
    }

    for file_path, finding_type in batch:
        try:
            # Check if file still exists (may have been deleted)
            if not file_path.exists():
                logger.warning(f"File deleted during scan: {file_path}")
                stats["skipped"] += 1
                continue

            # Read content
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    sql_content = f.read()
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                stats["errors"] += 1
                continue

            # Skip empty files
            if not sql_content.strip():
                logger.warning(f"Empty file: {file_path}")
                stats["skipped"] += 1
                continue

            # Parse filename
            metadata = parse_crash_filename(file_path)

            # Find or create crash
            crash_id = db.find_or_create_crash(sql_content, metadata["signal_number"], finding_type)

            # Add crash instance
            db.add_crash_instance(
                crash_id=crash_id,
                session_id=session_id,
                file_path=str(file_path),
                file_name=metadata["file_name"],
                afl_id=metadata["afl_id"],
                signal_number=metadata["signal_number"],
                source_id=metadata["source_id"],
                sync_id=metadata["sync_id"],
                file_size=metadata["file_size"]
            )

            stats["processed"] += 1
            stats["crashes_added"] += 1

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            stats["errors"] += 1

    return stats
