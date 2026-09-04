#!/usr/bin/env python3
"""Fail if an :ok txn reads [] for a key created before that txn invoked."""
import re
import sys

EVENT = re.compile(
    r"\{:type :(ok|fail|invoke|info), :f :txn, :value \[(.*?)\], :process (\d+), :index (\d+), :time (\d+)\}"
)
APPEND = re.compile(r'\[:append "([^"]+)" (\d+)\]')
READ = re.compile(r'\[:r "([^"]+)" (nil|\[\]|\[([^\]]*)\])\]')


def main(path: str) -> int:
    created: dict[str, int] = {}
    invoke: dict[int, int] = {}
    misses = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = EVENT.search(line)
            if not m:
                continue
            kind, value, proc, index, time = (
                m.group(1),
                m.group(2),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
            )
            if kind == "invoke":
                invoke[proc] = time
                continue
            if kind != "ok":
                invoke.pop(proc, None)
                continue
            snapshot = invoke.pop(proc, time)
            for key, _val in APPEND.findall(value):
                created.setdefault(key, time)
            for key, result, _inner in READ.findall(value):
                if result == "[]" and key in created and snapshot > created[key]:
                    misses.append((index, time, snapshot, key, created[key]))
    if not misses:
        print(f"ok: no unique-miss empty reads in {path}")
        return 0
    print(f"FAIL: {len(misses)} empty reads of already-created keys in {path}")
    for index, time, snapshot, key, created_at in misses[:20]:
        print(
            f"  t={time} idx={index} snap={snapshot} r {key} [] after create at t={created_at}"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
