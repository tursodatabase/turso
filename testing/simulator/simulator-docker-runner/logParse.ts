// Helper functions

export type StackTraceInfo = {
  type: "panic";
  trace: string;
  mainError: string;
}

export type AssertionFailureInfo = {
  type: "assertion";
  output: string;
  mainError: string;
}

/**
 * Extract failure information from log output
 */
export function extractFailureInfo(output: string): StackTraceInfo | AssertionFailureInfo {
  const lines = output.split('\n');

  const info = getTraceFromOutput(lines) ?? getAssertionFailureInfo(lines);

  if (!info) {
    throw new Error("No failure information found");
  }

  return info;
}

function getTraceFromOutput(lines: string[]): StackTraceInfo | null {
  const panicLineIndex = lines.findIndex(line => line.includes("panic occurred"));
  if (panicLineIndex === -1) {
    return null;
  }

  const startIndex = panicLineIndex + 1;
  const endIndex = Math.min(lines.length, startIndex + 50);

  const trace = lines.slice(startIndex, endIndex).join('\n');
  const mainError = lines[startIndex] ?? "???";

  return { type: "panic", trace, mainError };
}

function getAssertionFailureInfo(lines: string[]): AssertionFailureInfo | null {
  const simulationFailedLineIndex = lines.findIndex(line => line.includes("simulation failed:"));
  if (simulationFailedLineIndex === -1) {
    return null;
  }

  const startIndex = simulationFailedLineIndex;
  const endIndex = Math.min(lines.length, startIndex + 50);

  const output = lines.slice(startIndex, endIndex).join('\n');
  const mainError = lines[startIndex] ?? "???";

  return { type: "assertion", output, mainError };
}