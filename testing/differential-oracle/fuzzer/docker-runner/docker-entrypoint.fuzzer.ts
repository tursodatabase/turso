#!/usr/bin/env bun

import { spawn } from "bun";
import { GithubClient } from "./github";
import { SlackClient } from "./slack";
import { extractFailureInfo } from "./logParse";
import { randomSeed } from "./random";

// Configuration from environment variables
const SLEEP_BETWEEN_RUNS_SECONDS = Number.isInteger(Number(process.env.SLEEP_BETWEEN_RUNS_SECONDS)) ? Number(process.env.SLEEP_BETWEEN_RUNS_SECONDS) : 0;
const TIME_LIMIT_MINUTES = Number.isInteger(Number(process.env.TIME_LIMIT_MINUTES)) ? Number(process.env.TIME_LIMIT_MINUTES) : 24 * 60;
const PER_RUN_TIMEOUT_SECONDS = Number.isInteger(Number(process.env.PER_RUN_TIMEOUT_SECONDS)) ? Number(process.env.PER_RUN_TIMEOUT_SECONDS) : 20 * 60;
const LOG_TO_STDOUT = process.env.LOG_TO_STDOUT === "true";
const NUM_STATEMENTS = Number.isInteger(Number(process.env.NUM_STATEMENTS)) ? Number(process.env.NUM_STATEMENTS) : 1000;

const github = new GithubClient();
const slack = new SlackClient();

process.env.RUST_BACKTRACE = "1";

console.log("Starting fuzzer in a loop...");
console.log(`Git hash: ${github.GIT_HASH}`);
console.log(`GitHub issues enabled: ${github.mode === 'real'}`);
console.log(`Slack notifications enabled: ${slack.mode === 'real'}`);
console.log(`Time limit: ${TIME_LIMIT_MINUTES} minutes`);
console.log(`Log simulator output to stdout: ${LOG_TO_STDOUT}`);
console.log(`Sleep between runs: ${SLEEP_BETWEEN_RUNS_SECONDS} seconds`);
console.log(`Per run timeout: ${PER_RUN_TIMEOUT_SECONDS} seconds`);
console.log(`Num statements per run: ${NUM_STATEMENTS}`);

process.on("SIGINT", () => {
  console.log("Received SIGINT, exiting...");
  process.exit(0);
});
process.on("SIGTERM", () => {
  console.log("Received SIGTERM, exiting...");
  process.exit(0);
});

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}

/**
 * Returns a promise that rejects when the timeout is reached.
 * Prints a message to the console every 10 seconds.
 * @param seconds - The number of seconds to timeout.
 * @param runNumber - The number of the run.
 * @returns A promise that rejects when the timeout is reached.
 */
const timeouter = (seconds: number, runNumber: number) => {
  const start = new Date();
  const stdoutNotifyInterval = setInterval(() => {
    const elapsedSeconds = Math.round((new Date().getTime() - start.getTime()) / 1000);
    console.log(`Run ${runNumber} - ${elapsedSeconds}s elapsed (timeout: ${seconds}s)`);
  }, 10 * 1000);
  let timeout: Timer;
  const timeouterPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      clearInterval(stdoutNotifyInterval);
      reject(new TimeoutError("Timeout"));
    }, seconds * 1000);
  });
  // @ts-ignore
  timeouterPromise.clear = () => {
    clearInterval(stdoutNotifyInterval);
    if (timeout) {
      clearTimeout(timeout);
    }
  }
  return timeouterPromise;
}

let unexpectedExits = 0;

const run = async (seed: string, bin: string, args: string[]): Promise<boolean> => {
  const proc = spawn([`/app/${bin}`, ...args], {
    stdout: LOG_TO_STDOUT ? "inherit" : "pipe",
    stderr: LOG_TO_STDOUT ? "inherit" : "pipe",
    env: { ...process.env }
  });

  const timeout = timeouter(PER_RUN_TIMEOUT_SECONDS, runNumber);
  let issuePosted = false;

  try {
    const exitCode = await Promise.race([proc.exited, timeout]);
    const stdout = await new Response(proc.stdout).text();
    const stderr = await new Response(proc.stderr).text();

    if (exitCode !== 0 && exitCode !== 137) {
      console.log(`[${new Date().toISOString()}]: ${bin} ${args.join(" ")} exited with code ${exitCode}`);
      const output = stdout + stderr;

      // Extract simulator seed and stack trace
      try {
        const seedForGithubIssue = seed;
        const failureInfo = extractFailureInfo(output);

        console.log(`Simulator seed: ${seedForGithubIssue}`);

        // Post the issue to Github and continue
        if (failureInfo.type === "panic") {
          await github.postGitHubIssue({
            type: "panic",
            seed: seedForGithubIssue,
            command: args.join(" "),
            stackTrace: failureInfo,
          });
          issuePosted = true;
        } else {
          await github.postGitHubIssue({
            type: "assertion",
            seed: seedForGithubIssue,
            command: args.join(" "),
            failureInfo,
          });
          issuePosted = true;
        }
      } catch (err2) {
        console.error(`Error extracting simulator seed and stack trace: ${err2}`);
        console.log("Last 100 lines of stdout: ", (stdout?.toString() || "").split("\n").slice(-100).join("\n"));
        console.log("Last 100 lines of stderr: ", (stderr?.toString() || "").split("\n").slice(-100).join("\n"));
        console.log(`Simulator seed: ${seed}`);
        process.exit(1);
      }
    } else if (exitCode === 137) {
      console.error("Child process exited due to sigkill, ignoring...");
      unexpectedExits++;
    }
  } catch (err) {
    if (err instanceof TimeoutError) {
      console.log(`Timeout on seed ${seed}, posting to Github...`);
      proc.kill();
      const stdout = await new Response(proc.stdout).text();
      const stderr = await new Response(proc.stderr).text();
      const output = stdout + '\n' + stderr;
      const seedForGithubIssue = seed;
      const lastLines = output.split('\n').slice(-100).join('\n');
      console.log(`Simulator seed: ${seedForGithubIssue}`);
      await github.postGitHubIssue({
        type: "timeout",
        seed: seedForGithubIssue,
        command: args.join(" "),
        output: lastLines,
      });
      issuePosted = true;
    } else {
      throw err;
    }
  } finally {
    // @ts-ignore
    timeout.clear();
  }

  return issuePosted;
}

// Main execution loop
const startTime = new Date();
let runNumber = 0;
let totalIssuesPosted = 0;

while (new Date().getTime() - startTime.getTime() < TIME_LIMIT_MINUTES * 60 * 1000) {
  const timestamp = new Date().toISOString();
  const seed = randomSeed();

  // Build args: -n 1000 --verbose --seed <SEED>
  const args = [
    '-n', String(NUM_STATEMENTS),
    '--verbose',
    '--seed', seed
  ];

  console.log(`[${timestamp}]: Running "fuzzer ${args.join(" ")}" - (seed ${seed}, run number ${runNumber})`);
  const issuePosted = await run(seed, "differential_fuzzer", args);

  if (issuePosted) {
    totalIssuesPosted++;
  }

  runNumber++;

  SLEEP_BETWEEN_RUNS_SECONDS > 0 && (await sleep(SLEEP_BETWEEN_RUNS_SECONDS));
}

// Post summary to Slack after the run completes
const endTime = new Date();
const timeElapsed = Math.floor((endTime.getTime() - startTime.getTime()) / 1000);
console.log(`\nRun completed! Total runs: ${runNumber}, Issues posted: ${totalIssuesPosted}, Time elapsed: ${timeElapsed}s`);

await slack.postRunSummary({
  totalRuns: runNumber,
  issuesPosted: totalIssuesPosted,
  unexpectedExits,
  timeElapsed,
  gitHash: github.GIT_HASH,
});

async function sleep(sec: number) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000));
}
