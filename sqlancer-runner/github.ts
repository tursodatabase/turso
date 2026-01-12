import { App } from "octokit";
import { levenshtein } from "./levenshtein.ts";
import type { SqlancerFailure, CorruptionAnalysis } from "./logParse.ts";

const MAX_OPEN_SQLANCER_ISSUES = parseInt(process.env.MAX_OPEN_SQLANCER_ISSUES || "10", 10);

const GITHUB_ISSUE_TITLE_MAX_LENGTH = 256;
const GITHUB_ISSUE_BODY_MAX_LENGTH = 65536;

export class GithubClient {
	/** Git hash of the commit that the runner was built from. */
	GIT_HASH: string;
	/** GitHub App ID. */
	GITHUB_APP_ID: string;
	/** Private key of the GitHub App (Turso GitHub Handyman). */
	GITHUB_APP_PRIVATE_KEY: string;
	/** Installation ID of the GitHub App in the tursodatabase organization. */
	GITHUB_APP_INSTALLATION_ID: number;
	GITHUB_REPO: string;
	GITHUB_ORG: string = "tursodatabase";
	GITHUB_REPO_NAME: string = "turso";
	mode: 'real' | 'dry-run';
	app: App | null;
	initialized: boolean = false;
	openIssues: { title: string; number: number }[] = [];

	constructor() {
		this.GIT_HASH = process.env.GIT_HASH || "unknown";
		this.GITHUB_APP_PRIVATE_KEY = process.env.GITHUB_APP_PRIVATE_KEY || "";
		this.GITHUB_APP_ID = process.env.GITHUB_APP_ID || "";
		this.GITHUB_APP_INSTALLATION_ID = parseInt(process.env.GITHUB_APP_INSTALLATION_ID || "0", 10);
		this.mode = this.GITHUB_APP_PRIVATE_KEY ? 'real' : 'dry-run';
		this.GITHUB_REPO = `${this.GITHUB_ORG}/${this.GITHUB_REPO_NAME}`;

		this.app = this.mode === 'real' ? new App({
			appId: this.GITHUB_APP_ID,
			privateKey: this.GITHUB_APP_PRIVATE_KEY,
		}) : null;
	}

	private async getOpenIssues(): Promise<{ title: string; number: number }[]> {
		const octokit = await this.app!.getInstallationOctokit(this.GITHUB_APP_INSTALLATION_ID);
		const issues = await octokit.request('GET /repos/{owner}/{repo}/issues', {
			owner: this.GITHUB_ORG,
			repo: this.GITHUB_REPO_NAME,
			state: 'open',
			creator: 'app/turso-github-handyman',
		});
		return issues.data.map((issue) => ({ title: issue.title, number: issue.number }));
	}

	async initialize(): Promise<void> {
		if (this.mode === 'dry-run') {
			console.log("Dry-run mode: Skipping GitHub initialization");
			this.initialized = true;
			return;
		}
		this.openIssues = await this.getOpenIssues();
		this.initialized = true;
	}

	async postGitHubIssue(failure: SqlancerFailure): Promise<void> {
		if (!this.initialized) {
			await this.initialize();
		}

		let title = this.createIssueTitle(failure);
		title = title.slice(0, GITHUB_ISSUE_TITLE_MAX_LENGTH);

		// Check for similar existing issues using Levenshtein distance
		for (const existingIssue of this.openIssues) {
			const SIMILARITY_THRESHOLD = 6;
			if (levenshtein(existingIssue.title, title) < SIMILARITY_THRESHOLD) {
				console.log(`Found similar issue #${existingIssue.number}: "${existingIssue.title}"`);
				await this.commentOnIssue(existingIssue.number, failure);
				return;
			}
		}

		let body = this.createIssueBody(failure);
		body = body.slice(0, GITHUB_ISSUE_BODY_MAX_LENGTH);

		if (this.mode === 'dry-run') {
			console.log(`Dry-run mode: Would create issue in ${this.GITHUB_REPO}`);
			console.log(`Title: ${title}`);
			console.log(`Body:\n${body}`);
			return;
		}

		if (this.openIssues.length >= MAX_OPEN_SQLANCER_ISSUES) {
			console.log(`Max open SQLancer issues reached: ${MAX_OPEN_SQLANCER_ISSUES}`);
			console.log(`Would create issue with title: ${title}`);
			return;
		}

		const [owner, repo] = this.GITHUB_REPO.split('/');
		const octokit = await this.app!.getInstallationOctokit(this.GITHUB_APP_INSTALLATION_ID);

		const response = await octokit.request('POST /repos/{owner}/{repo}/issues', {
			owner,
			repo,
			title,
			body,
			labels: ['bug', 'sqlancer', 'automated']
		});

		console.log(`Successfully created GitHub issue: ${response.data.html_url}`);
		this.openIssues.push({ title, number: response.data.number });
	}

	private async commentOnIssue(issueNumber: number, failure: SqlancerFailure): Promise<void> {
		const comment = this.createCommentBody(failure);

		if (this.mode === 'dry-run') {
			console.log(`Dry-run mode: Would comment on issue #${issueNumber}`);
			console.log(`Comment:\n${comment}`);
			return;
		}

		const octokit = await this.app!.getInstallationOctokit(this.GITHUB_APP_INSTALLATION_ID);

		const response = await octokit.request('POST /repos/{owner}/{repo}/issues/{issue_number}/comments', {
			owner: this.GITHUB_ORG,
			repo: this.GITHUB_REPO_NAME,
			issue_number: issueNumber,
			body: comment,
		});

		console.log(`Successfully commented on issue #${issueNumber}: ${response.data.html_url}`);
	}

	private createIssueTitle(failure: SqlancerFailure): string {
		const typePrefix = `SQLancer ${failure.oracle}`;
		switch (failure.type) {
			case "result_mismatch":
				return `${typePrefix}: result mismatch - ${failure.errorSummary}`;
			case "corruption":
				return `${typePrefix}: database corruption detected`;
			case "exception":
				return `${typePrefix}: ${failure.errorSummary}`;
			case "crash":
				return `${typePrefix}: crash - ${failure.errorSummary}`;
			case "timeout":
				return `${typePrefix}: timeout after ${failure.timeoutSeconds}s`;
		}
	}

	private createFaultDetails(failure: SqlancerFailure): string {
		const gitShortHash = this.GIT_HASH.substring(0, 7);
		const seedInfo = failure.seed !== undefined ? `\n- **Seed**: ${failure.seed}` : '';
		const seedArg = failure.seed !== undefined ? ` --seed ${failure.seed}` : '';
		return `- **Oracle**: ${failure.oracle}
- **Git Hash**: ${this.GIT_HASH}
- **Timestamp**: ${new Date().toISOString()}
- **Failure Type**: ${failure.type}${seedInfo}

### Run locally

\`\`\`bash
git checkout ${this.GIT_HASH}
./scripts/run-sqlancer.sh --oracle ${failure.oracle} --timeout 300${seedArg}
\`\`\`

### Run with Docker

\`\`\`bash
git checkout ${this.GIT_HASH}
docker build -f Dockerfile.sqlancer -t sqlancer-runner:${gitShortHash} --build-arg GIT_HASH=${this.GIT_HASH} .
docker run -e TIME_LIMIT_MINUTES=10 -e LOG_TO_STDOUT=true sqlancer-runner:${gitShortHash}
\`\`\``;
	}

	private createCommentBody(failure: SqlancerFailure): string {
		return `### Duplicate occurrence detected

${this.createFaultDetails(failure)}
`;
	}

	private createIssueBody(failure: SqlancerFailure): string {
		let body = `## SQLancer failure type: ${failure.type}

${this.createFaultDetails(failure)}

### SQLancer Output

\`\`\`
${failure.output.slice(0, 10000)}
\`\`\`
`;

		// Add reproduction SQL if available
		if (failure.reproductionSql) {
			body += `
### Reproduction SQL

\`\`\`sql
${failure.reproductionSql.slice(0, 20000)}
\`\`\`
`;
		}

		// Add corruption analysis if available
		if (failure.corruptionAnalysis) {
			body += this.formatCorruptionAnalysis(failure.corruptionAnalysis);
		}

		return body;
	}

	private formatCorruptionAnalysis(analysis: CorruptionAnalysis): string {
		let section = `
## Corruption Analysis

### WAL Summary
- Total frames: ${analysis.walInfo.totalFrames}
- Commit frames: ${analysis.walInfo.commitFrames}
- Unique pages: ${analysis.walInfo.uniquePages}
- Page size: ${analysis.walInfo.pageSize} bytes
`;

		if (analysis.corruptFrame) {
			section += `
### Corruption Location
- **Corrupting frame**: ${analysis.corruptFrame.frameNumber}
- **Last good state**: ${analysis.corruptFrame.lastGoodFrame} frames
- **Byte offset**: 0x${analysis.corruptFrame.byteOffset.toString(16)}
`;
		}

		if (analysis.integrityCheckOutput) {
			section += `
### Integrity Check Output

\`\`\`
${analysis.integrityCheckOutput.slice(0, 5000)}
\`\`\`
`;
		}

		return section;
	}
}
