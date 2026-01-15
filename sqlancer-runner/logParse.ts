/**
 * SQLancer output parsing and failure type definitions.
 * Extracts failure information from SQLancer logs and stdout/stderr.
 */

/** Analysis results from corruption debug tools. */
export interface CorruptionAnalysis {
	walInfo: {
		totalFrames: number;
		commitFrames: number;
		uniquePages: number;
		pageSize: number;
	};
	corruptFrame?: {
		frameNumber: number;
		lastGoodFrame: number;
		byteOffset: number;
	};
	integrityCheckOutput?: string;
}

/** Base fields for all failure types. */
interface BaseFailure {
	oracle: string;
	output: string;
	errorSummary: string;
	reproductionSql?: string;
	corruptionAnalysis?: CorruptionAnalysis;
	dbPath?: string;
	/** Random seed used for this SQLancer run - allows reproduction even after crashes. */
	seed?: number;
}

/** SQLancer detected different results between queries. */
interface ResultMismatchFailure extends BaseFailure {
	type: "result_mismatch";
	expectedResult?: string;
	actualResult?: string;
}

/** Database corruption detected via PRAGMA integrity_check. */
interface CorruptionFailure extends BaseFailure {
	type: "corruption";
}

/** Java exception during SQLancer execution. */
interface ExceptionFailure extends BaseFailure {
	type: "exception";
	exceptionClass?: string;
	stackTrace?: string;
}

/** SQLancer process crashed. */
interface CrashFailure extends BaseFailure {
	type: "crash";
	exitCode: number;
}

/** SQLancer run exceeded timeout. */
interface TimeoutFailure extends BaseFailure {
	type: "timeout";
	timeoutSeconds: number;
}

export type SqlancerFailure =
	| ResultMismatchFailure
	| CorruptionFailure
	| ExceptionFailure
	| CrashFailure
	| TimeoutFailure;

/**
 * Parse SQLancer output to extract failure information.
 * Analyzes stdout/stderr and log files to determine failure type.
 */
export function parseFailure(
	stdout: string,
	stderr: string,
	exitCode: number,
	oracle: string,
	logDir: string,
	timeoutSeconds?: number
): SqlancerFailure {
	const output = stdout + "\n" + stderr;
	const lines = output.split('\n');

	// Check for timeout first
	if (timeoutSeconds !== undefined) {
		return {
			type: "timeout",
			oracle,
			output: output.slice(-5000),
			errorSummary: `Timeout after ${timeoutSeconds}s`,
			timeoutSeconds,
		};
	}

	// Check for corruption (PRAGMA integrity_check failure)
	const corruptionInfo = extractCorruptionInfo(lines);
	if (corruptionInfo) {
		return {
			type: "corruption",
			oracle,
			output: output.slice(-10000),
			errorSummary: "Database integrity check failed",
			reproductionSql: extractReproductionSql(logDir),
			dbPath: corruptionInfo.dbPath,
		};
	}

	// Check for result mismatch (main oracle failure)
	const mismatchInfo = extractMismatchInfo(lines);
	if (mismatchInfo) {
		return {
			type: "result_mismatch",
			oracle,
			output: output.slice(-10000),
			errorSummary: mismatchInfo.summary,
			expectedResult: mismatchInfo.expected,
			actualResult: mismatchInfo.actual,
			reproductionSql: extractReproductionSql(logDir),
		};
	}

	// Check for Java exception
	const exceptionInfo = extractExceptionInfo(lines);
	if (exceptionInfo) {
		return {
			type: "exception",
			oracle,
			output: output.slice(-10000),
			errorSummary: exceptionInfo.summary,
			exceptionClass: exceptionInfo.exceptionClass,
			stackTrace: exceptionInfo.stackTrace,
			reproductionSql: extractReproductionSql(logDir),
		};
	}

	// Default: crash with unknown cause
	return {
		type: "crash",
		oracle,
		output: output.slice(-10000),
		errorSummary: extractErrorSummary(lines) || `Process exited with code ${exitCode}`,
		exitCode,
		reproductionSql: extractReproductionSql(logDir),
	};
}

/** Extract corruption-related information from output. */
function extractCorruptionInfo(lines: string[]): { dbPath?: string } | null {
	for (const line of lines) {
		// SQLite integrity check failure patterns
		if (line.includes("PRAGMA integrity_check") ||
			line.includes("database disk image is malformed") ||
			line.includes("corruption detected") ||
			line.includes("integrity_check failed")) {
			// Try to extract DB path from surrounding context
			const dbMatch = lines.join('\n').match(/database[:\s]+([^\s]+\.db)/i);
			return { dbPath: dbMatch?.[1] };
		}
	}
	return null;
}

/** Extract result mismatch information from SQLancer oracle output. */
function extractMismatchInfo(lines: string[]): {
	summary: string;
	expected?: string;
	actual?: string;
} | null {
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];

		// NoREC oracle mismatch pattern
		if (line.includes("NoREC") && line.includes("mismatch")) {
			return {
				summary: line.slice(0, 200),
				expected: extractNextMatch(lines, i, /expected[:\s]+(.+)/i),
				actual: extractNextMatch(lines, i, /actual[:\s]+(.+)/i),
			};
		}

		// TLP oracle mismatch pattern
		if (line.includes("TLP") && (line.includes("mismatch") || line.includes("different"))) {
			return {
				summary: line.slice(0, 200),
			};
		}

		// PQS oracle mismatch pattern
		if (line.includes("PQS") && line.includes("mismatch")) {
			return {
				summary: line.slice(0, 200),
			};
		}

		// Generic mismatch patterns from SQLancer
		if (line.includes("AssertionError") ||
			line.includes("result sets differ") ||
			line.includes("expected") && line.includes("but got")) {
			return {
				summary: line.slice(0, 200),
			};
		}
	}
	return null;
}

/** Extract Java exception information from output. */
function extractExceptionInfo(lines: string[]): {
	summary: string;
	exceptionClass?: string;
	stackTrace?: string;
} | null {
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];

		// Java exception patterns
		const exceptionMatch = line.match(/^([\w.]+Exception|[\w.]+Error):\s*(.+)/);
		if (exceptionMatch) {
			// Collect stack trace
			const stackLines: string[] = [line];
			for (let j = i + 1; j < Math.min(i + 30, lines.length); j++) {
				if (lines[j].trim().startsWith("at ") || lines[j].trim().startsWith("Caused by:")) {
					stackLines.push(lines[j]);
				} else if (stackLines.length > 1) {
					break;
				}
			}

			return {
				summary: exceptionMatch[2]?.slice(0, 200) || exceptionMatch[1],
				exceptionClass: exceptionMatch[1],
				stackTrace: stackLines.join('\n'),
			};
		}

		// SQLException pattern
		if (line.includes("SQLException") || line.includes("SQL error")) {
			return {
				summary: line.slice(0, 200),
				exceptionClass: "SQLException",
			};
		}
	}
	return null;
}

/** Extract a general error summary from output. */
function extractErrorSummary(lines: string[]): string | null {
	// Look for common error indicators
	for (const line of lines) {
		if (line.includes("error:") ||
			line.includes("ERROR:") ||
			line.includes("failed:") ||
			line.includes("FAILED:") ||
			line.includes("panic")) {
			return line.slice(0, 200);
		}
	}

	// Return last non-empty line as fallback
	for (let i = lines.length - 1; i >= 0; i--) {
		const line = lines[i].trim();
		if (line.length > 10) {
			return line.slice(0, 200);
		}
	}

	return null;
}

/** Extract a pattern match from lines after a given index. */
function extractNextMatch(lines: string[], startIndex: number, pattern: RegExp): string | undefined {
	for (let i = startIndex; i < Math.min(startIndex + 10, lines.length); i++) {
		const match = lines[i].match(pattern);
		if (match) {
			return match[1];
		}
	}
	return undefined;
}

/** Extract reproduction SQL from SQLancer log directory. */
function extractReproductionSql(logDir: string): string | undefined {
	// SQLancer writes logs to logs/limbo/database*-cur.log
	// Try to read the most recent log file
	try {
		const fs = require('fs');
		const path = require('path');

		const limboLogDir = path.join(logDir, 'limbo');
		if (!fs.existsSync(limboLogDir)) {
			return undefined;
		}

		const files = fs.readdirSync(limboLogDir)
			.filter((f: string) => f.endsWith('-cur.log'))
			.map((f: string) => path.join(limboLogDir, f));

		if (files.length === 0) {
			return undefined;
		}

		// Get the most recently modified log file
		const latestFile = files
			.map((f: string) => ({ file: f, mtime: fs.statSync(f).mtime }))
			.sort((a: { mtime: Date }, b: { mtime: Date }) => b.mtime.getTime() - a.mtime.getTime())[0]?.file;

		if (!latestFile) {
			return undefined;
		}

		const content = fs.readFileSync(latestFile, 'utf-8');

		// Extract SQL statements (CREATE, INSERT, UPDATE, DELETE, SELECT)
		const sqlStatements = content
			.split('\n')
			.filter((line: string) => {
				const trimmed = line.trim().toUpperCase();
				return trimmed.startsWith('CREATE') ||
					trimmed.startsWith('INSERT') ||
					trimmed.startsWith('UPDATE') ||
					trimmed.startsWith('DELETE') ||
					trimmed.startsWith('SELECT') ||
					trimmed.startsWith('DROP') ||
					trimmed.startsWith('ALTER') ||
					trimmed.startsWith('PRAGMA');
			})
			.slice(-100) // Last 100 statements
			.join('\n');

		return sqlStatements || undefined;
	} catch {
		return undefined;
	}
}

/** Check if a failure appears to be a corruption error. */
export function isCorruptionError(failure: SqlancerFailure): boolean {
	if (failure.type === "corruption") {
		return true;
	}

	// Check output for corruption indicators
	const output = failure.output.toLowerCase();
	return output.includes("malformed") ||
		output.includes("corruption") ||
		output.includes("integrity_check") ||
		output.includes("database disk image");
}
