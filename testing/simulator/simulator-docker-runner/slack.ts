export class SlackClient {
  private botToken: string;
  private channel: string;
  mode: 'real' | 'dry-run';
  
  constructor() {
    this.botToken = process.env.SLACK_BOT_TOKEN || "";
    this.channel = process.env.SLACK_CHANNEL || "#simulator-results-fake";
    this.mode = this.botToken ? 'real' : 'dry-run';

    if (this.mode === 'real') {
      if (this.channel === "#simulator-results-fake") {
        throw new Error("SLACK_CHANNEL must be set to a real channel when running in real mode");
      }
    } else {
      if (this.channel !== "#simulator-results-fake") {
        throw new Error("SLACK_CHANNEL must be set to #simulator-results-fake when running in dry-run mode");
      }
    }
  }

  async postRunSummary(stats: {
    totalRuns: number;
    issuesPosted: number;
    unexpectedExits: number;
    timeElapsed: number;
    gitHash: string;
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

      const result = await response.json();
      
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
    unexpectedExits: number;
    timeElapsed: number;
    gitHash: string;
  }): string {
    const { totalRuns, issuesPosted, unexpectedExits, timeElapsed, gitHash } = stats;
    const hours = Math.floor(timeElapsed / 3600);
    const minutes = Math.floor((timeElapsed % 3600) / 60);
    const seconds = Math.floor(timeElapsed % 60);
    const timeString = `${hours}h ${minutes}m ${seconds}s`;
    const gitShortHash = gitHash.substring(0, 7);
    
    return `🤖 Turso Simulator Run Complete - ${totalRuns} runs, ${issuesPosted} issues posted, ${unexpectedExits} unexpected exits, ${timeString} elapsed (${gitShortHash})`;
  }

  private createSummaryBlocks(stats: {
    totalRuns: number;
    issuesPosted: number;
    unexpectedExits: number;
    timeElapsed: number;
    gitHash: string;
  }): any[] {
    const { totalRuns, issuesPosted, unexpectedExits, timeElapsed, gitHash } = stats;
    const hours = Math.floor(timeElapsed / 3600);
    const minutes = Math.floor((timeElapsed % 3600) / 60);
    const seconds = Math.floor(timeElapsed % 60);
    const timeString = `${hours}h ${minutes}m ${seconds}s`;
    
    const statusEmoji = issuesPosted > 0 || unexpectedExits > 0 ? "🔴" : "✅";
    const statusText = issuesPosted > 0 ? `${issuesPosted} issues found` : "No issues found";
    const gitShortHash = gitHash.substring(0, 7);
    
    return [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "🤖 Turso Simulator Run Complete"
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
            "text": `*Unexpected exits:*\n${unexpectedExits}`
          },
          {
            "type": "mrkdwn",
            "text": `*Time elapsed:*\n${timeString}`
          },
          {
            "type": "mrkdwn",
            "text": `*Git hash:*\n\`${gitShortHash}\``
          },
          {
            "type": "mrkdwn",
            "text": `*See open issues:*\n<https://github.com/tursodatabase/turso/issues?q=is%3Aissue%20state%3Aopen%20simulator%20author%3Aapp%2Fturso-github-handyman|Open issues>`
          }
        ]
      },
      {
        "type": "divider"
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