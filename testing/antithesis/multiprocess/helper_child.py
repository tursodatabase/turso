import sys

from antithesis.assertions import sometimes
from antithesis.random import get_random
from helper_common import connect, execute_with_busy_retry


def commit_and_checkpoint():
    con = connect()
    cur = con.cursor()
    num_accts = cur.execute("SELECT num_accts FROM initial_state").fetchone()[0]
    sender = get_random() % num_accts + 1
    recipient = (sender % num_accts) + 1
    value = get_random() % 1000
    if execute_with_busy_retry(cur, "BEGIN IMMEDIATE") is None:
        print("child: could not start a write transaction")
        return
    cur.execute(f"UPDATE accounts SET balance = balance - {value} WHERE id = {sender}")
    cur.execute(f"UPDATE accounts SET balance = balance + {value} WHERE id = {recipient}")
    cur.execute("UPDATE accounts SET generation = generation + 1")
    cur.execute("COMMIT")
    busy, log, checkpointed = cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
    print(f"child: committed, then wal_checkpoint(PASSIVE): busy={busy} log={log} checkpointed={checkpointed}")
    sometimes(
        busy == 0 and checkpointed < log,
        "a reader in another process stopped a checkpoint before the end of the WAL",
        {"mode": "PASSIVE", "log": log, "checkpointed": checkpointed},
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["commit-and-checkpoint"]:
        commit_and_checkpoint()
