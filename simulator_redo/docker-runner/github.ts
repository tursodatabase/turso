import { App } from "octokit";
import { AssertionFailureInfo, StackTraceInfo } from "./logParse";
import { levenshtein } from "./levenshtein";

type FaultPanic = {
  type: "panic"
  seed: string
  command: string
  stackTrace: StackTraceInfo
}

type FaultAssertion = {
  type: "assertion"
  seed: string
  command: string
  failureInfo: AssertionFailureInfo
}

type FaultTimeout = {
  type: "timeout"
  seed: string
  command: string
  output: string
}

type Fault = FaultPanic | FaultTimeout | FaultAssertion;

const MAX_OPEN_SIMULATOR_ISSUES = parseInt(process.env.MAX_OPEN_SIMULATOR_ISSUES || "10", 10);

const GITHUB_ISSUE_TITLE_MAX_LENGTH = 256;
const GITHUB_ISSUE_BODY_MAX_LENGTH = 65536;

export class GithubClient {
  /* This is the git hash of the commit that the simulator was built from. */
  GIT_HASH: string;
  /* Github app ID. */
  GITHUB_APP_ID: string;
  /* This is the private key of the Github App. */
  GITHUB_APP_PRIVATE_KEY: string;
  /* This is the unique installation id of the app into the organization. */
  GITHUB_APP_INSTALLATION_ID: number;
  GITHUB_REPO: string;
  GITHUB_ORG: string = "tursodatabase";
  GITHUB_REPO_NAME: string = "limbo";
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

    // Initialize GitHub OAuth App
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
      labels: 'simulator_redo',
      creator: 'app/turso-github-handyman',
    });
    return issues.data.map((issue) => ({ title: issue.title, number: issue.number }));
  }

  async initialize(): Promise<void> {
    if (this.mode === 'dry-run') {
      console.log("Dry-run mode: Skipping initialization");
      this.initialized = true;
      return;
    }
    this.openIssues = await this.getOpenIssues();
    this.initialized = true;
  }

  async postGitHubIssue(fault: Fault): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }

    let title = ((f: Fault) => {
      if (f.type === "panic") {
        return `sim_redo panic: "${f.stackTrace.mainError}"`;
      } else
      if (f.type === "assertion") {
        return `sim_redo assertion failure: "${f.failureInfo.mainError}"`;
      }
      return `sim_redo timeout using git hash ${this.GIT_HASH}`;
    })(fault);
    title = title.slice(0, GITHUB_ISSUE_TITLE_MAX_LENGTH);
    for (const existingIssue of this.openIssues) {
      const MAGIC_NUMBER = 6;
      if (levenshtein(existingIssue.title, title) < MAGIC_NUMBER) {
        console.log(`Found similar issue #${existingIssue.number}: "${existingIssue.title}"`);
        await this.commentOnIssue(existingIssue.number, fault);
        return;
      }
    }

    let body = this.createIssueBody(fault);
    body = body.slice(0, GITHUB_ISSUE_BODY_MAX_LENGTH);

    if (this.mode === 'dry-run') {
      console.log(`Dry-run mode: Would create issue in ${this.GITHUB_REPO} with title: ${title} and body: ${body}`);
      return;
    }

    if (this.openIssues.length >= MAX_OPEN_SIMULATOR_ISSUES) {
      console.log(`Max open simulator issues reached: ${MAX_OPEN_SIMULATOR_ISSUES}`);
      console.log(`Would create issue in ${this.GITHUB_REPO} with title: ${title} and body: ${body}`);
      return;
    }

    const [owner, repo] = this.GITHUB_REPO.split('/');

    const octokit = await this.app!.getInstallationOctokit(this.GITHUB_APP_INSTALLATION_ID);

    const response = await octokit.request('POST /repos/{owner}/{repo}/issues', {
      owner,
      repo,
      title,
      body,
      labels: ['bug', 'simulator_redo', 'automated']
    });

    console.log(`Successfully created GitHub issue: ${response.data.html_url}`);
    this.openIssues.push({ title, number: response.data.number });
  }

  private async commentOnIssue(issueNumber: number, fault: Fault): Promise<void> {
    const comment = this.createCommentBody(fault);

    if (this.mode === 'dry-run') {
      console.log(`Dry-run mode: Would comment on issue #${issueNumber} with: ${comment}`);
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

  private createFaultDetails(fault: Fault): string {
    const gitShortHash = this.GIT_HASH.substring(0, 7);
    return `- **Seed**: ${fault.seed}
- **Git Hash**: ${this.GIT_HASH}
- **Command**: \`sim_redo ${fault.command}\`
- **Timestamp**: ${new Date().toISOString()}

### Run locally with Docker

\`\`\`
gh pr checkout 4642
docker buildx build -t sim-redo:${gitShortHash} -f simulator_redo/docker-runner/Dockerfile . --build-arg GIT_HASH=$(git rev-parse HEAD)
docker run --network host sim-redo:${gitShortHash} ${fault.command}
\`\`\`

### Run locally with Cargo

\`\`\`
gh pr checkout 4642
cargo build -p sim_redo
./target/debug/sim_redo ${fault.command}
\`\`\``;
  }

  private createCommentBody(fault: Fault): string {
    return `### Duplicate occurrence detected

${this.createFaultDetails(fault)}
`;
  }

  private createIssueBody(fault: Fault): string {
    const output = fault.type === "panic" ? fault.stackTrace.trace : fault.type === "assertion" ? fault.failureInfo.output : fault.output;
    return `## sim_redo failure type: ${fault.type}

${this.createFaultDetails(fault)}

### ${fault.type === "panic" ? "Stack Trace" : "Output"}

\`\`\`
${output}
\`\`\`
`;
  }
}
