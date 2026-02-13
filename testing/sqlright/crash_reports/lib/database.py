"""Database connection and operations for crash report collection."""

import sqlite3
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime


class Database:
    """SQLite database wrapper for crash reports."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = None

    def get_connection(self) -> sqlite3.Connection:
        """Get or create database connection with WAL mode."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def init_database(self, schema_path: Path):
        """Initialize database from schema file."""
        if self.db_path.exists():
            return  # Database already exists

        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        conn = self.get_connection()
        conn.executescript(schema_sql)
        conn.commit()

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def register_session(self, session_path: str) -> int:
        """Register a fuzzing session, return session_id."""
        conn = self.get_connection()
        cursor = conn.execute(
            """
            INSERT INTO fuzzing_sessions (session_path, last_scan)
            VALUES (?, CURRENT_TIMESTAMP)
            ON CONFLICT(session_path) DO UPDATE SET last_scan = CURRENT_TIMESTAMP
            RETURNING session_id
            """,
            (session_path,)
        )
        session_id = cursor.fetchone()[0]
        conn.commit()
        return session_id

    def get_checkpoint(self, session_id: int) -> Optional[Dict[str, Any]]:
        """Get processing checkpoint for a session."""
        conn = self.get_connection()
        cursor = conn.execute(
            "SELECT * FROM processing_checkpoints WHERE session_id = ?",
            (session_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_checkpoint(self, session_id: int, last_file: str, last_mtime: float, files_processed: int):
        """Update processing checkpoint."""
        conn = self.get_connection()
        conn.execute(
            """
            INSERT INTO processing_checkpoints (session_id, last_processed_file, last_file_mtime, files_processed, last_update)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(session_id) DO UPDATE SET
                last_processed_file = excluded.last_processed_file,
                last_file_mtime = excluded.last_file_mtime,
                files_processed = files_processed + excluded.files_processed,
                last_update = CURRENT_TIMESTAMP
            """,
            (session_id, last_file, last_mtime, files_processed)
        )
        conn.commit()

    def get_processed_file_paths(self, session_id: int) -> set:
        """Get set of already processed file paths for a session."""
        conn = self.get_connection()
        cursor = conn.execute(
            "SELECT file_path FROM crash_instances WHERE session_id = ?",
            (session_id,)
        )
        return {row[0] for row in cursor.fetchall()}

    def find_or_create_crash(self, sql_content: str, signal_number: Optional[int]) -> int:
        """Find existing crash by content hash or create new one. Returns crash_id."""
        content_hash = hashlib.md5(sql_content.encode('utf-8')).hexdigest()
        conn = self.get_connection()

        # Try to find existing crash
        cursor = conn.execute(
            "SELECT crash_id FROM crashes WHERE content_hash = ? AND signal_number IS ?",
            (content_hash, signal_number)
        )
        row = cursor.fetchone()

        if row:
            # Update last_seen and instance_count
            crash_id = row[0]
            conn.execute(
                """
                UPDATE crashes
                SET last_seen = CURRENT_TIMESTAMP,
                    instance_count = instance_count + 1
                WHERE crash_id = ?
                """,
                (crash_id,)
            )
            conn.commit()
            return crash_id
        else:
            # Create new crash
            cursor = conn.execute(
                """
                INSERT INTO crashes (content_hash, signal_number, sql_content)
                VALUES (?, ?, ?)
                RETURNING crash_id
                """,
                (content_hash, signal_number, sql_content)
            )
            crash_id = cursor.fetchone()[0]
            conn.commit()
            return crash_id

    def add_crash_instance(self, crash_id: int, session_id: int, file_path: str,
                          file_name: str, afl_id: Optional[int], signal_number: Optional[int],
                          source_id: Optional[str], sync_id: Optional[str], file_size: int):
        """Add a crash instance (file). Idempotent - uses INSERT OR IGNORE."""
        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO crash_instances
                (crash_id, session_id, file_path, file_name, afl_id, signal_number, source_id, sync_id, file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (crash_id, session_id, file_path, file_name, afl_id, signal_number, source_id, sync_id, file_size)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # File already processed, skip
            pass

    def update_session_crash_count(self, session_id: int):
        """Update crash count for a session."""
        conn = self.get_connection()
        conn.execute(
            """
            UPDATE fuzzing_sessions
            SET crash_count = (
                SELECT COUNT(*) FROM crash_instances WHERE session_id = ?
            )
            WHERE session_id = ?
            """,
            (session_id, session_id)
        )
        conn.commit()

    def get_untested_crashes(self) -> List[Dict[str, Any]]:
        """Get crashes that haven't been tested yet."""
        conn = self.get_connection()
        cursor = conn.execute(
            """
            SELECT c.crash_id, c.sql_content, c.signal_number
            FROM crashes c
            LEFT JOIN crash_tests ct ON c.crash_id = ct.crash_id
            WHERE ct.test_id IS NULL
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    def add_crash_test(self, crash_id: int, exit_code: Optional[int], signal_received: Optional[int],
                      timed_out: bool, stdout: str, stderr: str, classification: str):
        """Add crash test result."""
        conn = self.get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO crash_tests
            (crash_id, exit_code, signal_received, timed_out, stdout_output, stderr_output, classification)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (crash_id, exit_code, signal_received, timed_out, stdout, stderr, classification)
        )
        conn.commit()

    def add_sqlite_comparison(self, crash_id: int, sqlite_exit_code: int,
                             sqlite_stdout: str, sqlite_stderr: str,
                             sqlite_classification: str, is_bug: bool,
                             bug_category: Optional[str], notes: Optional[str]):
        """Add SQLite comparison result."""
        conn = self.get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO differential_tests
            (crash_id, sqlite_exit_code, sqlite_stdout, sqlite_stderr,
             sqlite_classification, is_bug, bug_category, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (crash_id, sqlite_exit_code, sqlite_stdout, sqlite_stderr,
             sqlite_classification, is_bug, bug_category, notes)
        )
        conn.commit()

    def get_crash(self, crash_id: int) -> Optional[Dict[str, Any]]:
        """Get crash by ID with test results."""
        conn = self.get_connection()
        cursor = conn.execute(
            """
            SELECT
                c.*,
                ct.classification AS turso_classification,
                ct.stderr_output AS turso_stderr,
                sc.sqlite_classification,
                sc.is_bug,
                sc.bug_category
            FROM crashes c
            LEFT JOIN crash_tests ct ON c.crash_id = ct.crash_id
            LEFT JOIN differential_tests sc ON c.crash_id = sc.crash_id
            WHERE c.crash_id = ?
            """,
            (crash_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_bugs(self) -> List[Dict[str, Any]]:
        """Get all crashes classified as bugs."""
        conn = self.get_connection()
        cursor = conn.execute(
            """
            SELECT
                c.crash_id,
                c.content_hash,
                c.instance_count,
                ct.classification AS turso_classification,
                sc.bug_category,
                LENGTH(c.sql_content) AS sql_length
            FROM crashes c
            JOIN crash_tests ct ON c.crash_id = ct.crash_id
            JOIN differential_tests sc ON c.crash_id = sc.crash_id
            WHERE sc.is_bug = 1
            ORDER BY sc.bug_category, c.crash_id
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        conn = self.get_connection()

        # Total crashes and instances
        cursor = conn.execute(
            """
            SELECT
                COUNT(DISTINCT ci.file_path) AS total_files,
                COUNT(DISTINCT c.crash_id) AS unique_crashes,
                COUNT(DISTINCT CASE WHEN sc.is_bug = 1 THEN c.crash_id END) AS bugs_found
            FROM crashes c
            LEFT JOIN crash_instances ci ON c.crash_id = ci.crash_id
            LEFT JOIN differential_tests sc ON c.crash_id = sc.crash_id
            """
        )
        stats = dict(cursor.fetchone())

        # Bug categories
        cursor = conn.execute(
            """
            SELECT bug_category, COUNT(*) AS count
            FROM differential_tests
            WHERE is_bug = 1
            GROUP BY bug_category
            """
        )
        stats['bug_categories'] = {row[0]: row[1] for row in cursor.fetchall()}

        # Classification breakdown
        cursor = conn.execute(
            """
            SELECT classification, COUNT(*) AS count
            FROM crash_tests
            GROUP BY classification
            """
        )
        stats['classifications'] = {row[0]: row[1] for row in cursor.fetchall()}

        return stats
