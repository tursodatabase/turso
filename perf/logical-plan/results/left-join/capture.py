import hashlib
import json
from pathlib import Path
import shutil
import subprocess


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/left-join'
saved = root / 'target/logical-plan-left-join'
saved.mkdir(exist_ok=True)
before = json.loads((scratch / 'before.json').read_text())
for name, digest in before['sha256'].items():
    if name != 'core/translate/relational/lower.rs':
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
assert subprocess.check_output(['git', 'ls-files', '--stage', '--', 'prompt2.md', 'Neumann-Unnesting-1.pdf', 'Neumann-Unnesting-2.pdf'], cwd=root, text=True) == before['input_index']
lower_before = (scratch / 'lower-before.rs').read_text()
lower_head = (scratch / 'lower-head.rs').read_text()
lower = (root / 'core/translate/relational/lower.rs').read_text()
marker, following = '            Relation::Join {\n', '            Relation::Membership {\n'
start, end = lower_before.index(marker), lower_before.index(following)
old = lower_before[start:end]
new = lower[lower.index(marker):lower.index(following)]
assert lower_head.count(old) == 1
clean = lower_head.replace(old, new)
assert lower_before.replace(old, new) == lower
(scratch / 'lower-clean.rs').write_text(clean)
(scratch / 'deferred-preservation.json').write_text(json.dumps({'other_protected_files_unchanged': True, 'input_index_unchanged': True, 'lowering_change_only_replaces_join_arm': True, 'clean_lower_sha256': hashlib.sha256(clean.encode()).hexdigest()}, indent=2) + '\n')
paths = sorted(str(path.relative_to(root)) for path in (root / 'core/translate/relational').rglob('*.rs'))
paths += ['core/translate/optimizer/mod.rs', 'core/translate/optimizer/unnest.rs', 'core/translate/main_loop/close.rs', 'core/benches/prepare_benchmark.rs', 'core/benches/unnesting_execution.rs']
source = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), 'source_sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths}, 'source_diff': subprocess.check_output(['git', 'diff', '--', *paths], cwd=root, text=True), 'deferred_drafts_preserved': True, 'binaries': {}}
fuzzer = root / 'target/logical-plan-build/debug/differential_fuzzer'
shutil.copy2(fuzzer, saved / fuzzer.name)
source['binaries']['differential_fuzzer'] = {'path': str(saved / fuzzer.name), 'sha256': hashlib.sha256((saved / fuzzer.name).read_bytes()).hexdigest()}
(scratch / 'source.json').write_text(json.dumps(source, indent=2) + '\n')
print('Saved source and fuzzer; deferred files and staged inputs are unchanged.')
