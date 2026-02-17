import { Session, type SessionConfig } from "@tursodatabase/serverless";

export interface RemoteWriterConfig {
    url: string;
    authToken?: string | (() => Promise<string>);
    remoteEncryptionKey?: string;
}

/**
 * Manages remote write routing using Session from @tursodatabase/serverless.
 * For standalone writes (not in txn), creates a temporary Session per write.
 * For transactions, reuses Session (baton-based) across all statements.
 */
export class RemoteWriter {
    private session: Session | null = null;
    private _inRemoteTxn: boolean = false;
    private config: RemoteWriterConfig;

    constructor(config: RemoteWriterConfig) {
        this.config = config;
    }

    private async resolveAuthToken(): Promise<string | undefined> {
        if (typeof this.config.authToken === "function") {
            return await this.config.authToken();
        }
        return this.config.authToken;
    }

    private async createSession(): Promise<Session> {
        const authToken = await this.resolveAuthToken();
        const sessionConfig: SessionConfig = {
            url: this.config.url,
            authToken,
            remoteEncryptionKey: this.config.remoteEncryptionKey,
        };
        return new Session(sessionConfig);
    }

    /**
     * Execute single SQL on remote. Creates temp session if not in txn.
     */
    async execute(sql: string, args: any[] = []): Promise<{
        columns: string[];
        rows: any[];
        rowsAffected: number;
        lastInsertRowid: number | undefined;
    }> {
        if (this._inRemoteTxn) {
            return await this.session!.execute(sql, args);
        }
        const session = await this.createSession();
        try {
            return await session.execute(sql, args);
        } finally {
            await session.close();
        }
    }

    /**
     * Execute multi-statement SQL on remote (for exec() path).
     */
    async sequence(sql: string): Promise<void> {
        if (this._inRemoteTxn) {
            await this.session!.sequence(sql);
            return;
        }
        const session = await this.createSession();
        try {
            await session.sequence(sql);
        } finally {
            await session.close();
        }
    }

    /**
     * Begin a remote transaction. Creates a session and sends BEGIN.
     */
    async beginTransaction(mode: string): Promise<void> {
        this.session = await this.createSession();
        await this.session.sequence("BEGIN " + mode);
        this._inRemoteTxn = true;
    }

    /**
     * Commit the remote transaction. Sends COMMIT and closes the session.
     */
    async commitTransaction(): Promise<void> {
        if (!this.session) {
            throw new Error("No active remote transaction");
        }
        try {
            await this.session.sequence("COMMIT");
        } finally {
            this._inRemoteTxn = false;
            await this.session.close();
            this.session = null;
        }
    }

    /**
     * Rollback the remote transaction. Sends ROLLBACK and closes the session.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.session) {
            throw new Error("No active remote transaction");
        }
        try {
            await this.session.sequence("ROLLBACK");
        } finally {
            this._inRemoteTxn = false;
            await this.session.close();
            this.session = null;
        }
    }

    get isInTransaction(): boolean {
        return this._inRemoteTxn;
    }

    async close(): Promise<void> {
        if (this.session) {
            try {
                if (this._inRemoteTxn) {
                    await this.session.sequence("ROLLBACK");
                }
            } catch {
                // ignore errors during cleanup
            }
            await this.session.close();
            this.session = null;
            this._inRemoteTxn = false;
        }
    }
}
