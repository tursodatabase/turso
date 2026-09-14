from pathlib import Path
import collections
import gzip
import json
import re

root = Path(__file__).resolve().parents[4]
base = root / 'perf/logical-plan/results'
costs_by_run = []
for directory in ['prepare-subquery-cache-copies', 'prepare-lazy-legacy-copy']:
    path = base / directory / 'callgrind-1.out.15.gz'
    lines = gzip.open(path, 'rt').read().splitlines()
    names = {}
    for line in lines:
        match = re.fullmatch(r'(?:c)?fn=\((\d+)\) (.+)', line)
        if match:
            names[match[1]] = re.sub(r"'\d+$", '', match[2])
    costs = collections.Counter()
    function = None
    skip_call_cost = False
    for line in lines:
        if line.startswith('fn='):
            function = names[re.match(r'fn=\((\d+)\)', line)[1]]
        elif line.startswith('calls='):
            skip_call_cost = True
        elif re.fullmatch(r'[\d*+\-]+ \d+', line):
            if skip_call_cost:
                skip_call_cost = False
            else:
                costs[function] += int(line.split()[1])
    total = int(next(line.split()[1] for line in lines if line.startswith('totals:')))
    assert sum(costs.values()) == total, (directory, sum(costs.values()), total)
    costs_by_run.append(costs)
rows = [
    {'function': name, 'before': costs_by_run[0][name], 'after': costs_by_run[1][name], 'delta': costs_by_run[1][name]-costs_by_run[0][name]}
    for name in costs_by_run[0].keys() | costs_by_run[1].keys()
    if costs_by_run[0][name] != costs_by_run[1][name]
]
rows.sort(key=lambda row: (-abs(row['delta']), row['function']))
(Path(__file__).parent / 'first-round-self-costs.json').write_text(json.dumps(rows, indent=2) + '\n')
