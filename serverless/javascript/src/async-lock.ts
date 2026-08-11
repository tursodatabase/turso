/**
 * The minimal locking surface statement execution serializes on. Satisfied
 * by `AsyncLock` and by transaction gates that never lock (the transaction
 * wrapper already holds the connection's lock for its whole duration).
 */
export interface Lock {
  acquire(): Promise<void>;
  release(): void;
}

export class AsyncLock implements Lock {
  private locked = false;
  private queue: Array<() => void> = [];

  async acquire(): Promise<void> {
    if (!this.locked) {
      this.locked = true;
      return;
    }
    return new Promise(resolve => { this.queue.push(resolve); });
  }

  release(): void {
    const next = this.queue.shift();
    if (next) {
      next();
    } else {
      this.locked = false;
    }
  }
}
