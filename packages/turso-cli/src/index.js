#!/usr/bin/env node

const { spawnSync } = require("child_process");
const path = require("path");

const PLATFORMS = {
  "darwin-arm64": "@tursodatabase/cli-darwin-arm64",
  "darwin-x64": "@tursodatabase/cli-darwin-x64",
  "linux-arm64": "@tursodatabase/cli-linux-arm64-gnu",
  "linux-x64": "@tursodatabase/cli-linux-x64-gnu",
  "win32-x64": "@tursodatabase/cli-win32-x64-msvc",
};

function getBinaryPath() {
  const key = `${process.platform}-${process.arch}`;
  const pkg = PLATFORMS[key];

  if (!pkg) {
    console.error(
      `Unsupported platform: ${process.platform}-${process.arch}\n` +
        `Turso CLI supports: macOS (arm64, x64), Linux (arm64, x64), Windows (x64)`
    );
    process.exit(1);
  }

  const binary = process.platform === "win32" ? "tursodb.exe" : "tursodb";

  try {
    return path.join(path.dirname(require.resolve(`${pkg}/package.json`)), binary);
  } catch {
    console.error(
      `Could not find the Turso CLI binary for your platform (${key}).\n` +
        `The package ${pkg} may not have been installed correctly.\n` +
        `Try reinstalling with: npm install -g turso`
    );
    process.exit(1);
  }
}

const result = spawnSync(getBinaryPath(), process.argv.slice(2), {
  stdio: "inherit",
});

if (result.error) {
  if (result.error.code === "ENOENT") {
    console.error("Could not find the Turso CLI binary. Try reinstalling with: npm install -g turso");
  } else {
    console.error(`Failed to run Turso CLI: ${result.error.message}`);
  }
  process.exit(1);
}

process.exit(result.status ?? 1);
