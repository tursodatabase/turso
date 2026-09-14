import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    root = Path('/workspace')
    name, *command = sys.argv[1:]
    assert command
    marker = 'target/logical-plan-commits/mark-projections/measure.py'
    records = json.loads((root / 'target/logical-plan-commits/mark-projections/measurements.json').read_text())
    assert len(records) >= 4 and all(step['phase'] == 'native' and step['exit_code'] == 0 for step in records[:4])
    processes = process_list()
    matching = [pid for pid, (_, _, args) in processes.items() if args == 'python3 ' + marker]
    assert len(matching) <= 1, matching
    for pid, (_, state, args) in processes.items():
        assert not (Path(args.split()[0]).name in {'cargo', 'rustc'} and 'T' not in state), (pid, args)
    paused = []
    record = {'command': command, 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'active': True, 'pids': paused}
    destination = root / 'target/logical-plan-commits/left-join' / (name + '.json')
    started = time.monotonic()
    try:
        if matching:
            pending = matching
            while pending:
                pid = pending.pop()
                try:
                    os.kill(pid, signal.SIGSTOP)
                except ProcessLookupError:
                    continue
                paused.append(pid)
                processes = process_list()
                pending.extend(child for child, (parent, _, _) in processes.items() if parent == pid)
            deadline = time.monotonic() + 10
            while True:
                processes = process_list()
                if all(pid not in processes or 'T' in processes[pid][1] for pid in paused):
                    break
                assert time.monotonic() < deadline, 'Benchmark processes did not stop.'
                time.sleep(0.01)
            record['paused_processes'] = {pid: processes.get(pid) for pid in paused}
        destination.write_text(json.dumps(record, indent=2) + '\n')
        print(f'Paused {len(paused)} benchmark processes for {name}.', flush=True)
        result = subprocess.run(command, cwd=root)
        record['exit_code'] = result.returncode
        result.check_returncode()
    finally:
        for pid in reversed(paused):
            try:
                os.kill(pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        record.update(active=False, finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), elapsed_seconds=time.monotonic() - started)
        destination.write_text(json.dumps(record, indent=2) + '\n')
        print('Resumed the saved-binary benchmark.', flush=True)


def process_list():
    rows = subprocess.check_output(['ps', '-eo', 'pid,ppid,stat,args'], text=True).splitlines()[1:]
    return {int(pid): (int(parent), state, args) for row in rows for pid, parent, state, args in [row.strip().split(None, 3)]}


main()
