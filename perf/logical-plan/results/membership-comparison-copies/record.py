import hashlib
import json
from pathlib import Path
import shutil

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/membership-comparison-copies'
output = root / 'perf/logical-plan/results/membership-comparison-copies'
output.mkdir(exist_ok=True)
for name in ['before.json', 'source.json', 'regression.json', 'validation.json', 'builds.json', 'build.py', 'validate.py', 'check-execution.py', 'measure.py', 'compare-prepare.py', 'record.py', 'test-before.txt', 'fmt.txt', 'optimizer.txt', 'relational.txt', 'forced.txt', 'integration.txt', 'runner-build.txt', 'sql.txt', 'clippy.txt']:
    shutil.copy2(scratch / name, output / name)
for name in ['run.json', 'run.txt', 'comparison.json']:
    (output / 'execution').mkdir(exist_ok=True)
    shutil.copy2(scratch / 'execution' / name, output / 'execution' / name)
for source, target in [('run.json', 'run.json'), ('run.txt', 'run.txt'), ('outcome.json', 'outcome.json'), ('simulator-output/schema.json', 'schema.json'), ('simulator-output/coverage.txt', 'coverage.txt')]:
    (output / 'seed-57291020').mkdir(exist_ok=True)
    shutil.copy2(scratch / 'seed-57291020' / source, output / 'seed-57291020' / target)
before = json.loads((scratch / 'before.json').read_text())
for name, digest in before['sha256'].items():
    if name not in ['core/translate/relational/scalar.rs', 'core/translate/relational/rules/tests.rs']:
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
comparison = json.loads((scratch / 'execution/comparison.json').read_text())
assert comparison['cases'] == 114 and not comparison['changed_plans']
outcome = {'validation': {'optimizer_and_relational': 65, 'query_processing_integration': 487, 'sql': 1484, 'execution_fixtures': 114}, 'execution_plans_changed': [], 'differential': json.loads((scratch / 'seed-57291020/outcome.json').read_text()), 'prepare_measurements': 'Pending; run measure.py only after the joined-projection benchmark series finishes and the machine is free of builds and tests.', 'saved_binaries': 'source.json', 'measurement_overlap': 'Correctness checks and builds overlap instruction counting for saved joined-projection binaries. They overlap no native timing or second benchmark.', 'deferred_files_unchanged': True, 'remaining_work': 'Measure this change against the fixed original limits, resolve recorded regressions, and complete general unnesting, integration and final validation.'}
(output / 'outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
print(json.dumps(outcome, indent=2))
