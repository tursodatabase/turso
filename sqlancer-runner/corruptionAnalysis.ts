/**
 * Corruption analysis module that orchestrates the Python debug tools.
 * Runs WAL analysis and frame bisection to identify corruption sources.
 */

import { spawn } from "child_process";
import * as fs from "fs";
import * as path from "path";
import type { CorruptionAnalysis } from "./logParse.ts";

/** Path to corruption debug tools directory. */
const TOOLS_DIR = process.env.CORRUPTION_TOOLS_DIR || "/app/corruption-debug-tools";

/** Timeout for individual tool runs (ms). */
const TOOL_TIMEOUT_MS = 120_000; // 2 minutes

/**
 * Run corruption analysis on a database with WAL.
 * Returns analysis results for inclusion in GitHub issue.
 */
export async function analyzeCorruption(dbPath: string): Promise<CorruptionAnalysis | null> {
	const walPath = dbPath + "-wal";

	// Check that files exist
	if (!fs.existsSync(dbPath)) {
		console.log(`Corruption analysis: Database not found: ${dbPath}`);
		return null;
	}

	if (!fs.existsSync(walPath)) {
		console.log(`Corruption analysis: WAL not found: ${walPath}`);
		// Run integrity check without WAL analysis
		const integrityOutput = await runIntegrityCheck(dbPath);
		return {
			walInfo: {
				totalFrames: 0,
				commitFrames: 0,
				uniquePages: 0,
				pageSize: 0,
			},
			integrityCheckOutput: integrityOutput || undefined,
		};
	}

	console.log(`Running corruption analysis on ${dbPath}`);

	// Step 1: Get WAL info
	const walInfo = await runWalInfo(walPath);
	if (!walInfo) {
		console.log("Corruption analysis: Failed to get WAL info");
		return null;
	}

	// Step 2: Find corrupting frame
	const corruptFrame = await runFindCorruptFrame(dbPath);

	// Step 3: Run integrity check
	const integrityOutput = await runIntegrityCheck(dbPath);

	return {
		walInfo,
		corruptFrame: corruptFrame || undefined,
		integrityCheckOutput: integrityOutput || undefined,
	};
}

/**
 * Run wal_info.py to get WAL summary.
 */
async function runWalInfo(walPath: string): Promise<CorruptionAnalysis["walInfo"] | null> {
	try {
		const output = await runPythonTool("wal_info.py", [walPath]);
		if (!output) {
			return null;
		}

		// Parse output like:
		// Total frames: 1234
		// Commit frames: 100
		// Unique pages: 50
		// Page size: 4096
		const parseNumber = (pattern: RegExp): number => {
			const match = output.match(pattern);
			return match ? parseInt(match[1], 10) : 0;
		};

		return {
			totalFrames: parseNumber(/Total frames:\s*(\d+)/i),
			commitFrames: parseNumber(/Commit frames:\s*(\d+)/i),
			uniquePages: parseNumber(/Unique pages:\s*(\d+)/i),
			pageSize: parseNumber(/Page size:\s*(\d+)/i),
		};
	} catch (error) {
		console.error(`Error running wal_info.py: ${error}`);
		return null;
	}
}

/**
 * Run find_corrupt_frame.py to identify the corrupting frame.
 */
async function runFindCorruptFrame(dbPath: string): Promise<{
	frameNumber: number;
	lastGoodFrame: number;
	byteOffset: number;
} | null> {
	try {
		const output = await runPythonTool("find_corrupt_frame.py", [dbPath]);
		if (!output) {
			return null;
		}

		// Parse output like:
		// Found: Frame 567 (0-indexed: 566) introduces corruption
		// Last good state: 566 frames
		// Corrupting frame byte offset: 123456 (0x1e240)
		const frameMatch = output.match(/Frame (\d+).*introduces corruption/i);
		const lastGoodMatch = output.match(/Last good state:\s*(\d+)/i);
		const byteOffsetMatch = output.match(/byte offset:\s*(\d+)/i);

		if (frameMatch) {
			return {
				frameNumber: parseInt(frameMatch[1], 10),
				lastGoodFrame: lastGoodMatch ? parseInt(lastGoodMatch[1], 10) : 0,
				byteOffset: byteOffsetMatch ? parseInt(byteOffsetMatch[1], 10) : 0,
			};
		}

		return null;
	} catch (error) {
		console.error(`Error running find_corrupt_frame.py: ${error}`);
		return null;
	}
}

/**
 * Run SQLite integrity check.
 */
async function runIntegrityCheck(dbPath: string): Promise<string | null> {
	try {
		const result = await runCommand("sqlite3", [dbPath, "PRAGMA integrity_check;"], TOOL_TIMEOUT_MS);
		return result.stdout || result.stderr || null;
	} catch (error) {
		console.error(`Error running integrity check: ${error}`);
		return null;
	}
}

/**
 * Run a Python corruption debug tool.
 */
async function runPythonTool(toolName: string, args: string[]): Promise<string | null> {
	const toolPath = path.join(TOOLS_DIR, toolName);

	if (!fs.existsSync(toolPath)) {
		console.log(`Corruption tool not found: ${toolPath}`);
		return null;
	}

	try {
		const result = await runCommand("python3", [toolPath, ...args], TOOL_TIMEOUT_MS);
		return result.stdout + result.stderr;
	} catch (error) {
		console.error(`Error running ${toolName}: ${error}`);
		return null;
	}
}

/**
 * Run a command with timeout and capture output.
 */
function runCommand(
	command: string,
	args: string[],
	timeoutMs: number
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
	return new Promise((resolve, reject) => {
		const proc = spawn(command, args, {
			stdio: ['ignore', 'pipe', 'pipe'],
		});

		let stdout = "";
		let stderr = "";

		proc.stdout.on('data', (data) => {
			stdout += data.toString();
		});

		proc.stderr.on('data', (data) => {
			stderr += data.toString();
		});

		const timeout = setTimeout(() => {
			proc.kill('SIGKILL');
			reject(new Error(`Command timed out after ${timeoutMs}ms`));
		}, timeoutMs);

		proc.on('close', (code) => {
			clearTimeout(timeout);
			resolve({
				stdout,
				stderr,
				exitCode: code || 0,
			});
		});

		proc.on('error', (error) => {
			clearTimeout(timeout);
			reject(error);
		});
	});
}

/**
 * Copy database files to analysis directory for preservation.
 * Returns path to copied database.
 */
export async function preserveCorruptDatabase(
	dbPath: string,
	analysisDir: string
): Promise<string | null> {
	try {
		const walPath = dbPath + "-wal";

		// Create analysis directory
		if (!fs.existsSync(analysisDir)) {
			fs.mkdirSync(analysisDir, { recursive: true });
		}

		const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
		const destDb = path.join(analysisDir, `corrupt-${timestamp}.db`);

		// Copy database file
		if (fs.existsSync(dbPath)) {
			fs.copyFileSync(dbPath, destDb);
		}

		// Copy WAL if exists
		if (fs.existsSync(walPath)) {
			fs.copyFileSync(walPath, destDb + "-wal");
		}

		console.log(`Preserved corrupt database to: ${destDb}`);
		return destDb;
	} catch (error) {
		console.error(`Error preserving database: ${error}`);
		return null;
	}
}
