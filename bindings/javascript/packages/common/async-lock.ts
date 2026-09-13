export class AsyncLock {
    locked: boolean;
    queue: any[];
    constructor() {
        this.locked = false;
        this.queue = []
    }
    async acquire(): Promise<void> {
        if (!this.locked) {
            this.locked = true;
            return Promise.resolve();
        } else {
            const block = new Promise<void>(resolve => { this.queue.push(resolve) });
            return block;
        }
    }
    release() {
        if (this.locked == false) {
            throw new Error("invalid state: lock was already unlocked");
        }
        const item = this.queue.shift();
        if (item != null) {
            this.locked = true;
            item();
        } else {
            this.locked = false;
        }
    }
}