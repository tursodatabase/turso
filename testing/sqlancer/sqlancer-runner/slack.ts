/**
 * Slack client for posting SQLancer run summaries.
 * Posts to configured Slack channel when runs complete.
 */
export class SlackClient {
	private botToken: string;
	private channel: string;
	mode: 'real' | 'dry-run';

	constructor() {
		this.botToken = process.env.SLACK_BOT_TOKEN || "";
		this.channel = process.env.SLACK_CHANNEL || "#sqlancer-results-fake";
		this.mode = this.botToken ? 'real' : 'dry-run';

		if (this.mode === 'real') {
			if (this.channel === "#sqlancer-results-fake") {
				throw new Error("SLACK_CHANNEL must be set to a real channel when running in real mode");
			}
		} else {
			if (this.channel !== "#sqlancer-results-fake") {
				throw new Error("SLACK_CHANNEL must be set to #sqlancer-results-fake when running in dry-run mode");
			}
		}
	}

	async postRunSummary(stats: {
		totalRuns: number;
		issuesPosted: number;
		timeouts: number;
		corruptions: number;
		timeElapsed: number;
		gitHash: string;
		oracleStats: Record<string, { runs: number; failures: number }>;
	}): Promise<void> {
		const blocks = this.createSummaryBlocks(stats);
		const fallbackText = this.createFallbackText(stats);

		if (this.mode === 'dry-run') {
			console.log(`Dry-run mode: Would post to Slack channel ${this.channel}`);
			console.log(`Fallback text: ${fallbackText}`);
			console.log(`Blocks: ${JSON.stringify(blocks, null, 2)}`);
			return;
		}

		try {
			const response = await fetch('https://slack.com/api/chat.postMessage', {
				method: 'POST',
				headers: {
					'Authorization': `Bearer ${this.botToken}`,
					'Content-Type': 'application/json',
				},
				body: JSON.stringify({
					channel: this.channel,
					text: fallbackText,
					blocks: blocks,
				}),
			});

			const result = await response.json() as { ok: boolean; error?: string };

			if (!result.ok) {
				console.error(`Failed to post to Slack: ${result.error}`);
				return;
			}

			console.log(`Successfully posted summary to Slack channel ${this.channel}`);
		} catch (error) {
			console.error(`Error posting to Slack: ${error}`);
		}
	}

	private createFallbackText(stats: {
		totalRuns: number;
		issuesPosted: number;
		timeouts: number;
		corruptions: number;
		timeElapsed: number;
		gitHash: string;
	}): string {
		const { totalRuns, issuesPosted, corruptions, timeElapsed, gitHash } = stats;
		const hours = Math.floor(timeElapsed / 3600);
		const minutes = Math.floor((timeElapsed % 3600) / 60);
		const seconds = Math.floor(timeElapsed % 60);
		const timeString = `${hours}h ${minutes}m ${seconds}s`;
		const gitShortHash = gitHash.substring(0, 7);

		return `SQLancer Run Complete - ${totalRuns} runs, ${issuesPosted} issues, ${corruptions} corruptions, ${timeString} elapsed (${gitShortHash})`;
	}

	private createSummaryBlocks(stats: {
		totalRuns: number;
		issuesPosted: number;
		timeouts: number;
		corruptions: number;
		timeElapsed: number;
		gitHash: string;
		oracleStats: Record<string, { runs: number; failures: number }>;
	}): object[] {
		const { totalRuns, issuesPosted, timeouts, corruptions, timeElapsed, gitHash, oracleStats } = stats;
		const hours = Math.floor(timeElapsed / 3600);
		const minutes = Math.floor((timeElapsed % 3600) / 60);
		const seconds = Math.floor(timeElapsed % 60);
		const timeString = `${hours}h ${minutes}m ${seconds}s`;

		const statusEmoji = issuesPosted > 0 || corruptions > 0 ? "🔴" : "✅";
		const statusText = issuesPosted > 0 ? `${issuesPosted} issues found` : "No issues found";
		const gitShortHash = gitHash.substring(0, 7);

		// Build oracle breakdown string
		const oracleBreakdown = Object.entries(oracleStats)
			.map(([oracle, { runs, failures }]) => `${oracle}: ${runs} runs, ${failures} failures`)
			.join('\n');

		return [
			{
				"type": "header",
				"text": {
					"type": "plain_text",
					"text": "SQLancer Run Complete"
				}
			},
			{
				"type": "section",
				"text": {
					"type": "mrkdwn",
					"text": `${statusEmoji} *${statusText}*`
				}
			},
			{
				"type": "divider"
			},
			{
				"type": "section",
				"fields": [
					{
						"type": "mrkdwn",
						"text": `*Total runs:*\n${totalRuns}`
					},
					{
						"type": "mrkdwn",
						"text": `*Issues posted:*\n${issuesPosted}`
					},
					{
						"type": "mrkdwn",
						"text": `*Corruptions:*\n${corruptions}`
					},
					{
						"type": "mrkdwn",
						"text": `*Timeouts:*\n${timeouts}`
					},
					{
						"type": "mrkdwn",
						"text": `*Time elapsed:*\n${timeString}`
					},
					{
						"type": "mrkdwn",
						"text": `*Git hash:*\n\`${gitShortHash}\``
					}
				]
			},
			{
				"type": "section",
				"text": {
					"type": "mrkdwn",
					"text": `*Oracle Breakdown:*\n\`\`\`\n${oracleBreakdown}\n\`\`\``
				}
			},
			{
				"type": "divider"
			},
			{
				"type": "section",
				"text": {
					"type": "mrkdwn",
					"text": `*See open issues:*\n<https://github.com/tursodatabase/turso/issues?q=is%3Aissue%20state%3Aopen%20sqlancer%20author%3Aapp%2Fturso-github-handyman|Open SQLancer issues>`
				}
			},
			{
				"type": "context",
				"elements": [
					{
						"type": "mrkdwn",
						"text": `Full git hash: \`${gitHash}\` | Timestamp: ${new Date().toISOString()}`
					}
				]
			}
		];
	}
}
