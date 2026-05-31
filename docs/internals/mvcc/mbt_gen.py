#!/usr/bin/env python3
"""Generate MBT trace fixtures from checkpoint.qnt for the Rust adapter.

Runs `quint run --mbt` over several seeds, then transpiles each ITF trace into a
trivial line format (no JSON dependency in the Rust test):

    TRACE
    <action> <committed-id>...      # committed set AFTER this step
    ...

Output: ../../../core/mvcc/database/mbt_traces.mbt (next to the adapter test, so
it can be include_str!'d; `.mbt` rather than `.txt` because the repo globally
ignores `**/*.txt`). Re-run after changing checkpoint.qnt.

    python3 mbt_gen.py
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "checkpoint.qnt")
OUT = os.path.normpath(os.path.join(HERE, "../../../core/mvcc/database/mbt_traces.mbt"))
SEEDS = list(range(1, 41))
MAX_STEPS = 200


def resolve(committed_set):
    """Resolve a Set[Op] (the spec's `committed`) to the live (key, value)
    contents: latest ts per key wins; a tombstone (`del`) drops the key."""
    by_key = {}
    for op in committed_set["#set"]:
        k = int(op["key"]["#bigint"])
        ts = int(op["ts"]["#bigint"])
        v = int(op["value"]["#bigint"])
        deleted = bool(op["del"])
        if k not in by_key or ts > by_key[k][0]:
            by_key[k] = (ts, v, deleted)
    return sorted((k, v) for k, (_ts, v, deleted) in by_key.items() if not deleted)


def in_flight_set(state):
    """Map the spec's `inFlightTxs` state var into {txId: [(key, value, del), ...]}."""
    out = {}
    for tx in state["inFlightTxs"]["#set"]:
        tx_id = int(tx["txId"]["#bigint"])
        ops = []
        for op in tx["ops"]["#set"]:
            ops.append((
                int(op["key"]["#bigint"]),
                int(op["value"]["#bigint"]),
                bool(op["del"]),
            ))
        out[tx_id] = sorted(ops)
    return out


def encode_staged_ops(ops):
    """Encode a list of (key, value, del) tuples as `k:v:d,k:v:d,...`. No spaces
    inside the token: each line is `<action> <contents-tokens>` where contents are
    space-delimited, so the staged-op list must be one whitespace-free token."""
    if not ops:
        return "-"
    return ",".join(f"{k}:{v}:{1 if d else 0}" for k, v, d in ops)


def gen_trace(seed: int):
    with tempfile.NamedTemporaryFile(suffix=".itf.json", delete=False) as f:
        itf = f.name
    subprocess.run(
        ["quint", "run", SPEC, "--mbt", "--out-itf", itf,
         "--max-steps", str(MAX_STEPS), "--max-samples", "1", "--seed", hex(seed)],
        check=True, capture_output=True, cwd=HERE,
    )
    with open(itf) as f:
        d = json.load(f)
    os.unlink(itf)

    # The model represents a transaction as prepareTx (plants in-flight ops) +
    # finalizeTx (assigns ts, commits). When the pair is ADJACENT (no other action
    # interleaves them — the common "idle" case), it is observationally an atomic
    # commit and we collapse it into a single `commit` step so the existing adapter
    # episode/diff logic keeps working unchanged for that case. When something
    # else interleaves (notably a checkpoint phase), we keep both events with
    # their txId so the adapter can park a real connection at BuildLogRecordStart
    # for the prepareTx and resume it at finalizeTx — the only schedule that
    # exposes speculative-state (TxID-end) tombstones to the checkpoint.
    raw = []
    prev_in_flight = {}
    for st in d["states"]:
        action = st["mbt::actionTaken"]
        contents = resolve(st["committed"])
        in_flight_now = in_flight_set(st)
        raw.append({
            "action": action,
            "ckpt_mode": st["ckptMode"],
            "contents": contents,
            "in_flight_before": prev_in_flight,
            "in_flight_now": in_flight_now,
        })
        prev_in_flight = in_flight_now

    out_lines = []
    i = 0
    n = len(raw)
    while i < n:
        st = raw[i]
        action = st["action"]
        contents = st["contents"]
        # The `seed` pre-populate step is a large atomic commit in the spec
        # (a single `seed` action that materializes seedKeys); emit as `commit`
        # so the existing adapter replays it as one insert txn.
        if action == "seed":
            out_lines.append(" ".join(["commit"] + [f"{k}:{v}" for k, v in contents]))
            i += 1
            continue

        if action == "prepareTx":
            # The new entry in inFlightTxs is the txn just staged.
            new_ids = sorted(
                set(st["in_flight_now"].keys()) - set(st["in_flight_before"].keys())
            )
            assert len(new_ids) == 1, (
                f"prepareTx state should add exactly one in-flight txn, got {new_ids}"
            )
            tx_id = new_ids[0]
            ops = st["in_flight_now"][tx_id]
            # Adjacent prepare+finalize of the SAME txId, with no other action in
            # between, behaves observationally as an atomic commit — collapse it
            # so the existing `commit` episode handler keeps covering this case.
            if i + 1 < n:
                nxt = raw[i + 1]
                if nxt["action"] == "finalizeTx":
                    removed_ids = sorted(
                        set(nxt["in_flight_before"].keys())
                        - set(nxt["in_flight_now"].keys())
                    )
                    if len(removed_ids) == 1 and removed_ids[0] == tx_id:
                        out_lines.append(
                            " ".join(["commit"] + [f"{k}:{v}" for k, v in nxt["contents"]])
                        )
                        i += 2
                        continue
            # Non-adjacent: emit with explicit txId + staged ops so the adapter
            # can park a real connection between this step and finalizeTx.
            tok = f"prepareTx@{tx_id}@{encode_staged_ops(ops)}"
            out_lines.append(" ".join([tok] + [f"{k}:{v}" for k, v in contents]))
            i += 1
            continue

        if action == "finalizeTx":
            # The removed in-flight entry identifies the txn just committed.
            removed_ids = sorted(
                set(st["in_flight_before"].keys()) - set(st["in_flight_now"].keys())
            )
            assert len(removed_ids) == 1, (
                f"finalizeTx should remove exactly one in-flight txn, got {removed_ids}"
            )
            tx_id = removed_ids[0]
            tok = f"finalizeTx@{tx_id}"
            out_lines.append(" ".join([tok] + [f"{k}:{v}" for k, v in contents]))
            i += 1
            continue

        # Tag the checkpoint's mode onto its start action so the adapter replays
        # the right CheckpointMode (passive/full/restart/truncate).
        if action == "checkpointStart":
            action = f"checkpointStart:{st['ckpt_mode']}"
        out_lines.append(" ".join([action] + [f"{k}:{v}" for k, v in contents]))
        i += 1

    return out_lines


def main():
    blocks = []
    for seed in SEEDS:
        try:
            blocks.append(gen_trace(seed))
        except subprocess.CalledProcessError as e:
            print(f"quint failed for seed {seed}: {e.stderr.decode()}", file=sys.stderr)
            sys.exit(1)
    with open(OUT, "w") as f:
        f.write("# Generated by docs/internals/mvcc/mbt_gen.py from checkpoint.qnt.\n")
        f.write("# Do not edit by hand. Each TRACE block: `<action> <committed-id>...`\n")
        f.write("# (committed set after the step). Replayed by the Rust MBT adapter.\n")
        for b in blocks:
            f.write("TRACE\n")
            for line in b:
                f.write(line + "\n")
    print(f"wrote {len(blocks)} traces to {OUT}")


if __name__ == "__main__":
    main()
