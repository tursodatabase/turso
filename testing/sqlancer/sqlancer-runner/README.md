# SQLancer ECS Runner

Continuously runs [SQLancer](https://github.com/sqlancer/sqlancer) fuzzing against Turso to detect bugs and database corruption.

## Features

- Runs SQLancer in a loop with multiple oracles (NoREC, PQS, TLP)
- Automatically creates GitHub issues for failures (with deduplication)
- Runs corruption analysis using `scripts/corruption-debug-tools/` for corruption bugs
- Posts summary to Slack when runs complete
- Designed for ECS Fargate deployment

## Local Development

### Prerequisites

- [Bun](https://bun.sh/) runtime
- Java 17+
- Maven 3.9+
- Python 3.11+
- SQLite3 CLI

### Run Locally

```bash
# Install dependencies
cd sqlancer-runner
bun install

# Run in dry-run mode (no GitHub/Slack posting)
TIME_LIMIT_MINUTES=5 LOG_TO_STDOUT=true bun docker-entrypoint.sqlancer.ts
```

### Build Docker Image

```bash
# Build the image
docker build -f testing/sqlancer/sqlancer-runner/Dockerfile.sqlancer -t sqlancer-runner:test --build-arg GIT_HASH=$(git rev-parse HEAD) .

# Run with dry-run mode
docker run -e TIME_LIMIT_MINUTES=5 -e LOG_TO_STDOUT=true sqlancer-runner:test
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIME_LIMIT_MINUTES` | 240 | Total run time (4 hours) |
| `PER_RUN_TIMEOUT_SECONDS` | 300 | Timeout per SQLancer run |
| `SLEEP_BETWEEN_RUNS_SECONDS` | 5 | Sleep between runs |
| `LOG_TO_STDOUT` | false | Log SQLancer output to stdout |
| `GIT_HASH` | unknown | Git commit hash for issue tracking |
| `GITHUB_APP_ID` | - | GitHub App ID |
| `GITHUB_APP_PRIVATE_KEY` | - | GitHub App private key |
| `GITHUB_APP_INSTALLATION_ID` | - | GitHub App installation ID |
| `MAX_OPEN_SQLANCER_ISSUES` | 10 | Max open issues before stopping |
| `SLACK_BOT_TOKEN` | - | Slack bot token |
| `SLACK_CHANNEL` | - | Slack channel for summaries |

### GitHub Secrets Required

- `LIMBO_SIM_AWS_REGION`: AWS region (e.g., `us-east-1`)
- `SQLANCER_DEPLOYER_IAM_ROLE`: IAM role ARN for GitHub Actions
- `SQLANCER_ECR_URL`: ECR registry URL

### Trigger Build

The GitHub workflow `.github/workflows/build-sqlancer.yml` builds and pushes the Docker image on:
- Push to `main` (when relevant files change)
- Manual trigger via `workflow_dispatch`

## Failure Types

| Type | Description |
|------|-------------|
| `result_mismatch` | Oracle detected different query results |
| `corruption` | Database integrity check failed |
| `exception` | Java exception during execution |
| `crash` | SQLancer process crashed |
| `timeout` | Run exceeded timeout |
