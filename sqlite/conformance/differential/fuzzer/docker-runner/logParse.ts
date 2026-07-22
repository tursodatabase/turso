// Helper functions

export type StackTraceInfo = {
	type: "panic";
	trace: string;
	mainError: string;
};

export type AssertionFailureInfo = {
	type: "assertion";
	output: string;
	mainError: string;
};

/**
 * Extract failure information from log output
 */
export function extractFailureInfo(
	output: string,
): StackTraceInfo | AssertionFailureInfo {
	const lines = output.split("\n");

	const info = getTraceFromOutput(lines) ?? getAssertionFailureInfo(lines);

	if (!info) {
		throw new Error("No failure information found");
	}

	return info;
}

function getTraceFromOutput(lines: string[]): StackTraceInfo | null {
	// Look for panic patterns
	const panicLineIndex = lines.findIndex(
		(line) =>
			line.includes("panicked at") ||
			line.includes("panic occurred") ||
			(line.includes("thread '") && line.includes("panic")),
	);
	if (panicLineIndex === -1) {
		return null;
	}

	const startIndex = panicLineIndex;
	const endIndex = Math.min(lines.length, startIndex + 50);

	const trace = lines.slice(startIndex, endIndex).join("\n");
	const mainError = lines[startIndex] ?? "???";

	return { type: "panic", trace, mainError };
}

function getAssertionFailureInfo(lines: string[]): AssertionFailureInfo | null {
	// Look for oracle failure or assertion patterns (case-insensitive)
	const failureLineIndex = lines.findIndex((line) => {
		const lower = line.toLowerCase();
		return (
			(lower.includes("oracle") &&
				(lower.includes("failure") || lower.includes("mismatch"))) ||
			(lower.includes("assertion") && lower.includes("failed")) ||
			lower.includes("error: oracle failure") ||
			lower.includes("simulation failed:")
		);
	});
	if (failureLineIndex === -1) {
		return null;
	}

	const startIndex = failureLineIndex;
	const endIndex = Math.min(lines.length, startIndex + 50);

	const output = lines.slice(startIndex, endIndex).join("\n");
	const mainError = lines[startIndex] ?? "???";

	return { type: "assertion", output, mainError };
}
