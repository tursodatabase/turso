#!/usr/bin/env bun
/**
 * SQLancer ECS Runner
 *
 * Continuously runs SQLancer fuzzing against Turso, detecting:
 * - Result mismatches (oracle failures)
 * - Database corruption
 * - Crashes and exceptions
 *
 * Automatically creates GitHub issues for failures with deduplication.
 * Posts summary to Slack when run completes.
 */

import { spawn, type ChildProcess } from "child_process";
import * as fs from "fs";
import * as path from "path";

import { GithubClient } from "./github.ts";
import { SlackClient } from "./slack.ts";
import { parseFailure, isCorruptionError, type SqlancerFailure } from "./logParse.ts";
import { analyzeCorruption, preserveCorruptDatabase, findSqlancerDatabases } from "./corruptionAnalysis.ts";

// Configuration from environment
const TIME_LIMIT_MINUTES = parseInt(process.env.TIME_LIMIT_MINUTES || "240", 10); // 4 hours default
const PER_RUN_TIMEOUT_SECONDS = parseInt(process.env.PER_RUN_TIMEOUT_SECONDS || "300", 10); // 5 min default
const SLEEP_BETWEEN_RUNS_SECONDS = parseInt(process.env.SLEEP_BETWEEN_RUNS_SECONDS || "5", 10);
const LOG_TO_STDOUT = process.env.LOG_TO_STDOUT === "true";
const GIT_HASH = process.env.GIT_HASH || "unknown";

// SQLancer configuration
const SQLANCER_DIR = "/tmp/sqlancer-limbo";
const LIMBO_JAR = process.env.LIMBO_JAR || "/app/turso.jar";
const NATIVE_LIB_DIR = process.env.NATIVE_LIB_DIR || "/app/native";
const ANALYSIS_DIR = "/tmp/corruption-analysis";

// Oracles to rotate through
const ORACLES = ["NoREC"] //  "PQS", "TLP"]; TODO: for now just use default

// Initialize clients
const github = new GithubClient();
const slack = new SlackClient();

// Stats tracking
const stats = {
	totalRuns: 0,
	issuesPosted: 0,
	timeouts: 0,
	corruptions: 0,
	oracleStats: {} as Record<string, { runs: number; failures: number }>,
};

// Initialize oracle stats
for (const oracle of ORACLES) {
	stats.oracleStats[oracle] = { runs: 0, failures: 0 };
}

process.env.RUST_BACKTRACE = "1";

/** Sleep for specified milliseconds. */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Timeout error for run timeouts. */
class TimeoutError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TimeoutError";
	}
}

/**
 * Create a timeout promise that rejects after specified seconds.
 */
function createTimeout(seconds: number, runNumber: number): Promise<never> & { clear: () => void } {
	const start = Date.now();

	// Log progress every 30 seconds
	const progressInterval = setInterval(() => {
		const elapsed = Math.round((Date.now() - start) / 1000);
		console.log(`  Run ${runNumber}: ${elapsed}s elapsed (timeout: ${seconds}s)`);
	}, 30_000);

	let timeout: ReturnType<typeof setTimeout>;

	const promise = new Promise<never>((_, reject) => {
		timeout = setTimeout(() => {
			clearInterval(progressInterval);
			reject(new TimeoutError(`Timeout after ${seconds}s`));
		}, seconds * 1000);
	}) as Promise<never> & { clear: () => void };

	promise.clear = () => {
		clearInterval(progressInterval);
		clearTimeout(timeout);
	};

	return promise;
}

/**
 * Generate a random seed for SQLancer.
 * We generate this before running so we can log it - if the process crashes,
 * we'll still have the seed to reproduce the issue.
 */
function generateSeed(): number {
	// SQLancer uses Java's Random which takes a long seed
	// Generate a random 48-bit integer (safe for JS number precision)
	return Math.floor(Math.random() * 0xFFFFFFFFFFFF);
}

/**
 * Run SQLancer with specified oracle.
 */
async function runSqlancer(
	oracle: string,
	timeoutSeconds: number,
	runNumber: number
): Promise<{ success: boolean; failure?: SqlancerFailure; seed?: number }> {
	// Verify prerequisites on first run
	if (runNumber === 0) {
		console.log("Verifying SQLancer prerequisites...");
		console.log(`  LIMBO_JAR: ${LIMBO_JAR} (exists: ${fs.existsSync(LIMBO_JAR)})`);
		console.log(`  NATIVE_LIB_DIR: ${NATIVE_LIB_DIR} (exists: ${fs.existsSync(NATIVE_LIB_DIR)})`);
		if (fs.existsSync(NATIVE_LIB_DIR)) {
			const libs = fs.readdirSync(NATIVE_LIB_DIR);
			console.log(`  Native libraries: ${libs.join(", ")}`);
		}
	}

	const sqlancerJar = fs.readdirSync(path.join(SQLANCER_DIR, "target"))
		.find((f) => f.startsWith("sqlancer-") && f.endsWith(".jar"));

	if (!sqlancerJar) {
		throw new Error("SQLancer JAR not found in target directory");
	}

	const sqlancerJarPath = path.join(SQLANCER_DIR, "target", sqlancerJar);

	// Generate seed BEFORE starting - if process crashes, we still have the seed logged
	const seed = generateSeed();

	const args = [
		`-Djava.library.path=${NATIVE_LIB_DIR}`,
		"-cp",
		`${sqlancerJarPath}:${LIMBO_JAR}`,
		"sqlancer.Main",
		"--timeout-seconds",
		String(timeoutSeconds),
		"--num-threads",
		"1",
		"--print-progress-summary",
		"true",
		"--random-seed",
		String(seed),
		"limbo",
		"--oracle",
		oracle,
		// Disable unsupported features
		"--test-temp-tables",
		"false",
		"--test-fts",
		"false",
		"--test-rtree",
		"false",
		"--test-check-constraints",
		"false",
		"--test-nulls-first-last",
		"false",
		"--test-generated-columns",
		"false",
		"--test-foreign-keys",
		"false",
	];

	// Log seed BEFORE starting process - critical for crash reproduction
	console.log(`[${new Date().toISOString()}] Run ${runNumber}: oracle=${oracle} seed=${seed}`);

	let proc: ChildProcess;
	let stdout = "";
	let stderr = "";

	const runPromise = new Promise<{ exitCode: number; timedOut: boolean }>((resolve) => {
		// Always pipe stdout/stderr so we capture output for GitHub issues
		proc = spawn("java", args, {
			cwd: SQLANCER_DIR,
			stdio: ["ignore", "pipe", "pipe"],
		});

		proc.stdout?.on("data", (data) => {
			const chunk = data.toString();
			stdout += chunk;
			if (LOG_TO_STDOUT) {
				process.stdout.write(chunk);
			}
		});

		proc.stderr?.on("data", (data) => {
			const chunk = data.toString();
			stderr += chunk;
			if (LOG_TO_STDOUT) {
				process.stderr.write(chunk);
			}
		});

		proc.on("close", (code) => {
			resolve({ exitCode: code || 0, timedOut: false });
		});

		proc.on("error", (error) => {
			stderr += `Process error: ${error.message}`;
			resolve({ exitCode: 1, timedOut: false });
		});
	});

	const timeoutPromise = createTimeout(timeoutSeconds, runNumber);

	try {
		const result = await Promise.race([runPromise, timeoutPromise]);
		timeoutPromise.clear();

		if (result.exitCode === 0) {
			return { success: true, seed };
		}

		// Parse the failure
		const failure = parseFailure(
			stdout,
			stderr,
			result.exitCode,
			oracle,
			path.join(SQLANCER_DIR, "logs")
		);

		// Add seed to failure for reproducibility
		failure.seed = seed;

		return { success: false, failure, seed };
	} catch (error) {
		timeoutPromise.clear();

		if (error instanceof TimeoutError) {
			// Kill the process
			proc!.kill("SIGKILL");
			stats.timeouts++;

			const failure: SqlancerFailure = {
				type: "timeout",
				oracle,
				output: stdout + stderr,
				errorSummary: `Timeout after ${timeoutSeconds}s`,
				timeoutSeconds,
				seed,
			};

			return { success: false, failure, seed };
		}

		throw error;
	}
}

/**
 * Setup SQLancer (clone and build if needed).
 */
async function setupSqlancer(): Promise<void> {
	console.log("=== Setting up SQLancer ===");

	// Check if SQLancer is already built
	if (fs.existsSync(path.join(SQLANCER_DIR, "target"))) {
		const jars = fs.readdirSync(path.join(SQLANCER_DIR, "target"))
			.filter((f) => f.startsWith("sqlancer-") && f.endsWith(".jar"));
		if (jars.length > 0) {
			console.log("SQLancer already built, skipping setup");
			return;
		}
	}

	// Clone SQLancer
	if (!fs.existsSync(SQLANCER_DIR)) {
		console.log("Cloning SQLancer...");
		await runCommandSync("git", [
			"clone",
			"--depth",
			"1",
			"https://github.com/sqlancer/sqlancer.git",
			SQLANCER_DIR,
		]);
	}

	// Copy Limbo provider
	const providerDir = path.join(SQLANCER_DIR, "src/sqlancer/limbo");
	if (!fs.existsSync(providerDir)) {
		fs.mkdirSync(providerDir, { recursive: true });
	}

	const limboProviderSrc = "/app/LimboProvider.java";
	if (fs.existsSync(limboProviderSrc)) {
		fs.copyFileSync(limboProviderSrc, path.join(providerDir, "LimboProvider.java"));
	}

	// Apply patches if needed
	await applyPatches();

	// Patch pom.xml for Limbo JAR
	await patchPomXml();

	// Build SQLancer
	console.log("Building SQLancer...");
	await runCommandSync("mvn", ["package", "-DskipTests"], { cwd: SQLANCER_DIR });

	// Verify the limbo provider was compiled
	const limboClassDir = path.join(SQLANCER_DIR, "target/classes/sqlancer/limbo");
	if (fs.existsSync(limboClassDir)) {
		const classes = fs.readdirSync(limboClassDir);
		console.log(`Limbo provider classes: ${classes.join(", ")}`);
	} else {
		console.log("WARNING: Limbo provider classes not found in target!");
	}

	// Check if the service was registered
	const servicesFile = path.join(SQLANCER_DIR, "target/classes/META-INF/services/sqlancer.DatabaseProvider");
	if (fs.existsSync(servicesFile)) {
		const content = fs.readFileSync(servicesFile, "utf-8");
		if (content.includes("limbo")) {
			console.log("Limbo provider registered in services file");
		} else {
			console.log("WARNING: Limbo provider NOT found in services file!");
			console.log("Services file content:", content);
		}
	} else {
		console.log("WARNING: DatabaseProvider services file not found!");
	}

	console.log("SQLancer setup complete");
}

/**
 * Apply necessary patches to SQLancer.
 */
async function applyPatches(): Promise<void> {
	const schemaFile = path.join(SQLANCER_DIR, "src/sqlancer/sqlite3/schema/SQLite3Schema.java");

	if (!fs.existsSync(schemaFile)) {
		console.log("Schema file not found, skipping patches");
		return;
	}

	let content = fs.readFileSync(schemaFile, "utf-8");

	// Check if already patched
	if (content.includes("Modified for Limbo")) {
		console.log("SQLancer already patched");
		return;
	}

	console.log("Patching SQLite3Schema for Limbo compatibility...");

	// Try to apply the patch file first
	const patchFile = "/app/SQLite3Schema.patch";
	if (fs.existsSync(patchFile)) {
		try {
			// Use --dry-run first to check if patch applies cleanly
			await runCommandSync("patch", ["--dry-run", "-p1", "-i", patchFile], { cwd: SQLANCER_DIR });
			// If dry-run succeeds, apply for real
			await runCommandSync("patch", ["-p1", "-i", patchFile], { cwd: SQLANCER_DIR });
			console.log("Applied SQLite3Schema patch successfully");
			return;
		} catch (e) {
			console.log(`Patch command failed: ${e}`);
			console.log("Trying manual fix...");
		}
	}

	// Manual fix using same approach as run-sqlancer.sh sed commands
	// This is a multi-step approach that's more resilient to formatting changes
	let changes = 0;

	// Fix 1: Replace the UNION query opening to complete the statement
	// This closes the executeQuery call early, making the continuation lines orphaned
	// sed: 's|"SELECT name, type as category, sql FROM sqlite_master UNION "|"SELECT name, type as category, sql FROM sqlite_master GROUP BY name;")) { // Limbo fix|g'
	const pattern1 = '"SELECT name, type as category, sql FROM sqlite_master UNION "';
	if (content.includes(pattern1)) {
		content = content.replace(
			pattern1,
			'"SELECT name, type as category, sql FROM sqlite_master GROUP BY name;")) { // Modified for Limbo'
		);
		console.log("  Applied fix 1: replaced UNION start with complete query");
		changes++;
	} else {
		console.log("  Pattern 1 not found");
	}

	// Fix 2: Delete orphaned continuation lines containing sqlite_temp_master
	// sed: '/sqlite_temp_master WHERE type=.table/d'
	const lines = content.split('\n');
	const filteredLines = lines.filter(line => {
		// Delete lines that are now orphaned string continuations
		if (line.includes("sqlite_temp_master WHERE type='table'") ||
			line.includes("sqlite_temp_master WHERE type='view'")) {
			console.log("  Removing orphaned line");
			changes++;
			return false;
		}
		return true;
	});
	content = filteredLines.join('\n');

	// Fix 3: Remove the UNION clause for index query
	// sed: "s|UNION SELECT name FROM sqlite_temp_master WHERE type='index'||g"
	const pattern3 = "UNION SELECT name FROM sqlite_temp_master WHERE type='index'";
	if (content.includes(pattern3)) {
		content = content.replace(pattern3, "");
		console.log("  Applied fix 3: removed index UNION clause");
		changes++;
	} else {
		console.log("  Pattern 3 not found");
	}

	// Verify and save
	if (changes === 0) {
		console.log("WARNING: No changes were made to SQLite3Schema.java!");
		console.log("The SQLancer API may have changed. Checking file content...");

		// Debug: show what patterns exist
		if (content.includes("sqlite_temp_master")) {
			console.log("  File DOES contain 'sqlite_temp_master'");
			const idx = content.indexOf("sqlite_temp_master");
			console.log("  Context:", content.substring(Math.max(0, idx - 100), idx + 150));
		} else {
			console.log("  File does NOT contain 'sqlite_temp_master' - may already be patched or API changed");
		}
	} else {
		fs.writeFileSync(schemaFile, content);
		console.log(`Applied ${changes} manual patches to SQLite3Schema`);
	}
}

/**
 * Patch pom.xml to include Limbo JDBC driver.
 */
async function patchPomXml(): Promise<void> {
	const pomFile = path.join(SQLANCER_DIR, "pom.xml");
	let content = fs.readFileSync(pomFile, "utf-8");

	if (content.includes("turso")) {
		console.log("pom.xml already patched");
		return;
	}

	console.log("Patching pom.xml for Limbo JDBC driver...");

	// Find sqlite-jdbc dependency and add Limbo dependency after it
	const sqliteDepIndex = content.indexOf("sqlite-jdbc");
	if (sqliteDepIndex === -1) {
		console.log("Could not find sqlite-jdbc in pom.xml");
		return;
	}

	// Find the closing </dependency> tag after sqlite-jdbc
	const closeDepIndex = content.indexOf("</dependency>", sqliteDepIndex);
	if (closeDepIndex === -1) {
		return;
	}

	const insertPoint = closeDepIndex + "</dependency>".length;

	const limboDep = `
    <dependency>
      <groupId>tech.turso</groupId>
      <artifactId>turso</artifactId>
      <version>0.4.0</version>
      <scope>system</scope>
      <systemPath>${LIMBO_JAR}</systemPath>
    </dependency>`;

	content = content.slice(0, insertPoint) + limboDep + content.slice(insertPoint);
	fs.writeFileSync(pomFile, content);
}

/**
 * Run a command synchronously.
 */
function runCommandSync(
	command: string,
	args: string[],
	options?: { cwd?: string }
): Promise<void> {
	return new Promise((resolve, reject) => {
		const proc = spawn(command, args, {
			cwd: options?.cwd,
			stdio: "inherit",
		});

		proc.on("close", (code) => {
			if (code === 0) {
				resolve();
			} else {
				reject(new Error(`${command} exited with code ${code}`));
			}
		});

		proc.on("error", reject);
	});
}

/**
 * Main execution loop.
 */
async function main(): Promise<void> {
	console.log("=== SQLancer ECS Runner ===");
	console.log(`Git Hash: ${GIT_HASH}`);
	console.log(`Time Limit: ${TIME_LIMIT_MINUTES} minutes`);
	console.log(`Per-run Timeout: ${PER_RUN_TIMEOUT_SECONDS} seconds`);
	console.log(`Oracles: ${ORACLES.join(", ")}`);
	console.log("");

	// Setup SQLancer
	await setupSqlancer();

	// Initialize GitHub client
	await github.initialize();

	const startTime = Date.now();
	const timeLimitMs = TIME_LIMIT_MINUTES * 60 * 1000;

	console.log("");
	console.log("=== Starting SQLancer runs ===");
	console.log("");

	while (Date.now() - startTime < timeLimitMs) {
		const oracle = ORACLES[stats.totalRuns % ORACLES.length];
		stats.oracleStats[oracle].runs++;

		try {
			const result = await runSqlancer(oracle, PER_RUN_TIMEOUT_SECONDS, stats.totalRuns);

			if (!result.success && result.failure) {
				console.log(`[${new Date().toISOString()}] Run ${stats.totalRuns}: FAILURE (${result.failure.type})`);
				stats.oracleStats[oracle].failures++;

				// Run corruption analysis for corruption errors OR crashes/panics
				// Crashes might have left a corrupt database that's useful for debugging
				const shouldAnalyze = isCorruptionError(result.failure) ||
					result.failure.type === "crash" ||
					result.failure.type === "exception";

				if (shouldAnalyze) {
					if (isCorruptionError(result.failure)) {
						stats.corruptions++;
					}
					console.log(`  Running database analysis for ${result.failure.type}...`);

					// Find the database to analyze
					let dbToAnalyze: string | null = null;

					if (result.failure.dbPath) {
						// Use the known path from error parsing
						dbToAnalyze = result.failure.dbPath;
					} else {
						// For crashes/panics, search for recently modified databases
						const databases = findSqlancerDatabases(SQLANCER_DIR);
						if (databases.length > 0) {
							console.log(`  Found ${databases.length} database(s): ${databases.join(", ")}`);
							// Use the most recently modified one
							dbToAnalyze = databases[0];
						}
					}

					if (dbToAnalyze) {
						// Preserve the database
						const preserved = await preserveCorruptDatabase(dbToAnalyze, ANALYSIS_DIR);

						if (preserved) {
							const analysis = await analyzeCorruption(preserved);
							if (analysis) {
								result.failure.corruptionAnalysis = analysis;
								console.log(`  Analysis complete: ${analysis.walInfo.totalFrames} WAL frames`);
							}
						}
					} else {
						console.log("  No database found for analysis");
					}
				}

				// Post GitHub issue
				try {
					await github.postGitHubIssue(result.failure);
					stats.issuesPosted++;
				} catch (error) {
					console.error(`  Failed to post GitHub issue: ${error}`);
				}
			} else {
				console.log(`[${new Date().toISOString()}] Run ${stats.totalRuns}: OK`);
			}
		} catch (error) {
			console.error(`[${new Date().toISOString()}] Run ${stats.totalRuns}: ERROR - ${error}`);
		}

		stats.totalRuns++;

		// Sleep between runs
		if (SLEEP_BETWEEN_RUNS_SECONDS > 0) {
			await sleep(SLEEP_BETWEEN_RUNS_SECONDS * 1000);
		}
	}

	const timeElapsed = Math.round((Date.now() - startTime) / 1000);

	console.log("");
	console.log("=== SQLancer Run Complete ===");
	console.log(`Total runs: ${stats.totalRuns}`);
	console.log(`Issues posted: ${stats.issuesPosted}`);
	console.log(`Corruptions: ${stats.corruptions}`);
	console.log(`Timeouts: ${stats.timeouts}`);
	console.log(`Time elapsed: ${Math.round(timeElapsed / 60)} minutes`);
	console.log("");
	console.log("Oracle breakdown:");
	for (const [oracle, oracleStats] of Object.entries(stats.oracleStats)) {
		console.log(`  ${oracle}: ${oracleStats.runs} runs, ${oracleStats.failures} failures`);
	}

	// Post Slack summary
	try {
		await slack.postRunSummary({
			totalRuns: stats.totalRuns,
			issuesPosted: stats.issuesPosted,
			timeouts: stats.timeouts,
			corruptions: stats.corruptions,
			timeElapsed,
			gitHash: GIT_HASH,
			oracleStats: stats.oracleStats,
		});
	} catch (error) {
		console.error(`Failed to post Slack summary: ${error}`);
	}
}

// Run main
main().catch((error) => {
	console.error(`Fatal error: ${error}`);
	process.exit(1);
});
