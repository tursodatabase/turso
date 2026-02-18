"""Crash testing and differential analysis."""

import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class CrashExecutor:
    """Execute crashes against tursodb and SQLite for differential testing."""

    def __init__(self, tursodb_path: Path, timeout: int = 5):
        self.tursodb_path = tursodb_path
        self.timeout = timeout

        # Verify tursodb exists
        if not self.tursodb_path.exists():
            raise FileNotFoundError(f"tursodb not found at {tursodb_path}")

    def test_crash_against_tursodb(self, sql_content: str) -> Dict[str, Any]:
        """
        Test crash against tursodb.

        Returns dict with:
            - exit_code: Process exit code (or None if signaled)
            - signal_received: Signal number if process was signaled
            - timed_out: Boolean
            - stdout: Standard output
            - stderr: Standard error
            - classification: PANIC/PARSE_ERROR/SUCCESS/CRASH/TIMEOUT/ERROR
        """
        result = {
            "exit_code": None,
            "signal_received": None,
            "timed_out": False,
            "stdout": "",
            "stderr": "",
            "classification": "ERROR"
        }

        try:
            proc = subprocess.run(
                [
                    str(self.tursodb_path),
                    "-q",  # Quiet mode
                    "-m", "list",  # List mode
                    "--experimental-views",
                    "--experimental-strict",
                    "--experimental-triggers",
                    "--experimental-index-method",
                    "--experimental-autovacuum",
                    "--experimental-attach",
                    "--experimental-encryption",
                ],
                input=sql_content,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )

            result["exit_code"] = proc.returncode
            result["stdout"] = proc.stdout
            result["stderr"] = proc.stderr

            # Classify result
            result["classification"] = self._classify_result(
                proc.returncode, proc.stderr, timed_out=False
            )

        except subprocess.TimeoutExpired as e:
            result["timed_out"] = True
            result["stdout"] = e.stdout.decode("utf-8", errors="replace") if e.stdout else ""
            result["stderr"] = e.stderr.decode("utf-8", errors="replace") if e.stderr else ""
            result["classification"] = "TIMEOUT"
            logger.debug("Process timed out")

        except Exception as e:
            result["stderr"] = str(e)
            result["classification"] = "ERROR"
            logger.error(f"Error testing crash: {e}", exc_info=True)

        return result

    def test_crash_against_sqlite(self, sql_content: str) -> Dict[str, Any]:
        """
        Test crash against SQLite.

        Returns dict with:
            - exit_code: Process exit code
            - stdout: Standard output
            - stderr: Standard error
            - classification: PANIC/PARSE_ERROR/SUCCESS/CRASH/ERROR
        """
        result = {
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "classification": "ERROR"
        }

        try:
            proc = subprocess.run(
                ["sqlite3", ":memory:"],
                input=sql_content,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )

            result["exit_code"] = proc.returncode
            result["stdout"] = proc.stdout
            result["stderr"] = proc.stderr

            # Classify result
            result["classification"] = self._classify_result(
                proc.returncode, proc.stderr, timed_out=False
            )

        except subprocess.TimeoutExpired:
            result["classification"] = "TIMEOUT"
            logger.debug("SQLite process timed out")

        except FileNotFoundError:
            result["stderr"] = "sqlite3 not found in PATH"
            result["classification"] = "ERROR"
            logger.error("sqlite3 not found")

        except Exception as e:
            result["stderr"] = str(e)
            result["classification"] = "ERROR"
            logger.error(f"Error testing against SQLite: {e}", exc_info=True)

        return result

    def _classify_result(self, exit_code: int, stderr: str, timed_out: bool) -> str:
        """
        Classify test result based on exit code and stderr.

        Returns one of: PANIC, PARSE_ERROR, SUCCESS, CRASH, TIMEOUT, ERROR
        """
        if timed_out:
            return "TIMEOUT"

        # Check for panic in stderr
        if "panicked at" in stderr or "panic!" in stderr:
            return "PANIC"

        # Check for parse errors
        if "parse error" in stderr.lower() or "syntax error" in stderr.lower():
            return "PARSE_ERROR"

        # Check for crashes (negative exit codes indicate signals)
        if exit_code < 0:
            return "CRASH"

        # Success
        if exit_code == 0:
            return "SUCCESS"

        # Non-zero exit but not a crash
        if "error" in stderr.lower():
            return "PARSE_ERROR"

        return "ERROR"

    def analyze_divergence(self, turso_result: Dict[str, Any],
                          sqlite_result: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Analyze divergence between tursodb and SQLite results.

        Returns:
            (is_bug, bug_category, notes)

        Bug categories:
            - TURSO_PANIC: tursodb panicked but SQLite didn't
            - TURSO_CRASH: tursodb crashed but SQLite didn't
            - TURSO_INCORRECT_PARSE_ERROR: tursodb parse error but SQLite accepted
            - TURSO_INCORRECT_SUCCESS: tursodb succeeded but SQLite rejected
            - TURSO_TIMEOUT: tursodb timed out but SQLite didn't
        """
        turso_class = turso_result["classification"]
        sqlite_class = sqlite_result["classification"]

        # Identical behavior - not a bug
        if turso_class == sqlite_class:
            return False, None, None

        # Tursodb panicked
        if turso_class == "PANIC":
            return True, "TURSO_PANIC", "tursodb panicked on SQL that SQLite handled"

        # Tursodb crashed
        if turso_class == "CRASH":
            return True, "TURSO_CRASH", "tursodb crashed on SQL that SQLite handled"

        # Tursodb timed out
        if turso_class == "TIMEOUT" and sqlite_class != "TIMEOUT":
            return True, "TURSO_TIMEOUT", "tursodb timed out but SQLite completed"

        # Tursodb rejected valid SQL
        if turso_class == "PARSE_ERROR" and sqlite_class == "SUCCESS":
            return True, "TURSO_INCORRECT_PARSE_ERROR", "tursodb rejected SQL that SQLite accepted"

        # Tursodb accepted invalid SQL
        if turso_class == "SUCCESS" and sqlite_class == "PARSE_ERROR":
            return True, "TURSO_INCORRECT_SUCCESS", "tursodb accepted SQL that SQLite rejected"

        # Other divergences - might be implementation differences
        notes = f"Divergence: tursodb={turso_class}, sqlite={sqlite_class}"
        return False, None, notes


def test_crashes(db, tursodb_path: Path, timeout: int = 5) -> Dict[str, int]:
    """
    Test all untested crashes against tursodb and SQLite.

    Args:
        db: Database instance
        tursodb_path: Path to tursodb binary
        timeout: Timeout in seconds for each test

    Returns:
        Dict with stats: tested, bugs_found, errors
    """
    stats = {
        "tested": 0,
        "bugs_found": 0,
        "errors": 0
    }

    executor = CrashExecutor(tursodb_path, timeout)

    # Get untested crashes
    untested = db.get_untested_crashes()
    logger.info(f"Testing {len(untested)} untested crashes")

    for crash in untested:
        crash_id = crash["crash_id"]
        sql_content = crash["sql_content"]

        try:
            # Test against tursodb
            turso_result = executor.test_crash_against_tursodb(sql_content)

            # Save tursodb test result
            db.add_crash_test(
                crash_id=crash_id,
                exit_code=turso_result["exit_code"],
                signal_received=turso_result["signal_received"],
                timed_out=turso_result["timed_out"],
                stdout=turso_result["stdout"],
                stderr=turso_result["stderr"],
                classification=turso_result["classification"]
            )

            # Test against SQLite
            sqlite_result = executor.test_crash_against_sqlite(sql_content)

            # Analyze divergence
            is_bug, bug_category, notes = executor.analyze_divergence(turso_result, sqlite_result)

            # Save SQLite comparison
            db.add_sqlite_comparison(
                crash_id=crash_id,
                sqlite_exit_code=sqlite_result["exit_code"] or 0,
                sqlite_stdout=sqlite_result["stdout"],
                sqlite_stderr=sqlite_result["stderr"],
                sqlite_classification=sqlite_result["classification"],
                is_bug=is_bug,
                bug_category=bug_category,
                notes=notes
            )

            stats["tested"] += 1
            if is_bug:
                stats["bugs_found"] += 1
                logger.info(f"Bug found: {bug_category} (crash_id={crash_id})")

        except Exception as e:
            logger.error(f"Error testing crash {crash_id}: {e}", exc_info=True)
            stats["errors"] += 1

    return stats
