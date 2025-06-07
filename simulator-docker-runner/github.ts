import { App } from "octokit";
import { StackTraceInfo } from "./logParse";
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
  output: string
}

type FaultTimeout = {
  type: "timeout"
  seed: string
  command: string
  output: string
}

type Fault = FaultPanic | FaultTimeout | FaultAssertion;

const MAX_OPEN_SIMULATOR_ISSUES = parseInt(process.env.MAX_OPEN_SIMULATOR_ISSUES || "10", 10);

export class GithubClient {
  /* This is the git hash of the commit that the simulator was built from. */
  GIT_HASH: string;
  /* Github app ID. */
  GITHUB_APP_ID: string;
  /* This is the private key of the "Turso Github Handyman" Github App. It's stored in AWS secrets manager and will be injected into the container at runtime. */
  GITHUB_APP_PRIVATE_KEY: string;
  /* This is the unique installation id of the above app into the tursodatabase organization. */
  GITHUB_APP_INSTALLATION_ID: number;
  GITHUB_REPO: string;
  GITHUB_ORG: string = "tursodatabase";
  GITHUB_REPO_NAME: string = "limbo";
  mode: 'real' | 'dry-run';
  app: App | null;
  initialized: boolean = false;
  openIssueTitles: string[] = [];
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

  private async getOpenIssues(): Promise<string[]> {
    const octokit = await this.app!.getInstallationOctokit(this.GITHUB_APP_INSTALLATION_ID);
    const issues = await octokit.request('GET /repos/{owner}/{repo}/issues', {
      owner: this.GITHUB_ORG,
      repo: this.GITHUB_REPO_NAME,
      state: 'open',
      creator: 'app/turso-github-handyman',
    });
    return issues.data.map((issue) => issue.title);
  }

  async initialize(): Promise<void> {
    if (this.mode === 'dry-run') {
      console.log("Dry-run mode: Skipping initialization");
      this.initialized = true;
      return;
    }
    this.openIssueTitles = await this.getOpenIssues();
    this.initialized = true;
  }

  async postGitHubIssue(fault: Fault): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }

    const title = ((f: Fault) => {
      if (f.type === "panic") {
        return `Simulator panic: "${f.stackTrace.mainError}"`;
      } else 
      if (f.type === "assertion") {
        return `Simulator assertion failure: "${f.output}"`;
      }
      return `Simulator timeout using git hash ${this.GIT_HASH}`;
    })(fault);
    for (const existingIssueTitle of this.openIssueTitles) {
      const MAGIC_NUMBER = 6;
      if (levenshtein(existingIssueTitle, title) < MAGIC_NUMBER) {
        console.log(`Not creating issue ${title} because it is too similar to ${existingIssueTitle}`);
        return;
      }
    }

    const body = this.createIssueBody(fault);

    if (this.mode === 'dry-run') {
      console.log(`Dry-run mode: Would create issue in ${this.GITHUB_REPO} with title: ${title} and body: ${body}`);
      return;
    }

    if (this.openIssueTitles.length >= MAX_OPEN_SIMULATOR_ISSUES) {
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
      labels: ['bug', 'simulator', 'automated']
    });

    console.log(`Successfully created GitHub issue: ${response.data.html_url}`);
    this.openIssueTitles.push(title);
  }

  private createIssueBody(fault: Fault): string {
    const gitShortHash = this.GIT_HASH.substring(0, 7);
    return `
 ## Simulator ${fault.type}
 
 - **Seed**: ${fault.seed}
 - **Git Hash**: ${this.GIT_HASH}
 - **Command**: \`limbo-sim ${fault.command}\`
 - **Timestamp**: ${new Date().toISOString()}

 ### Run locally with Docker

 \`\`\`
 git checkout ${this.GIT_HASH}
 docker buildx build -t limbo-sim:${gitShortHash} -f simulator-docker-runner/Dockerfile.simulator . --build-arg GIT_HASH=$(git rev-parse HEAD)
 docker run --network host limbo-sim:${gitShortHash} ${fault.command}
 \`\`\`

 ### ${fault.type === "panic" ? "Stack Trace" : "Output"}
 
 \`\`\`
 ${fault.type === "panic" ? fault.stackTrace.trace : fault.output}
 \`\`\`
 `;
  }
}