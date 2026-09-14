import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

root=Path('/workspace')
out=root/'target/logical-plan-commits/membership-joined-projections'
saved=root/'target/logical-plan-membership-joined-projections'
original=root/'target/logical-plan-original-joined-projection-fixtures'
results=root/'perf/logical-plan/results'
assert json.loads((out/'restored.json').read_text())['source_restored']
assert (original/'restored.json').exists()
source=json.loads((out/'source.json').read_text())
assert subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip()=='logical-plan-codex'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==source['head']
for name,digest in source['source_sha256'].items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
processes=subprocess.check_output(['ps','-eo','pid,comm'],text=True)
assert not any(any(name in line for name in ['cargo','rustc','valgrind','callgrind','differential_f','prepare_bench','unnesting_exe','sqltest']) for line in processes.splitlines())
(out/'isolation.json').write_text(json.dumps({'processes_before_measurements':processes,'builds_complete':True,'measurements_and_tests_are_sequential':True},indent=2)+'\n')
execution=out/'execution'
execution.mkdir()
plans={}
for phase in ['before','after']:
    destination=execution/phase
    destination.mkdir()
    binary=saved/phase/'unnesting_execution'
    command=[str(binary),'--color','never','--test']
    env=dict(os.environ,TURSO_BENCH_PLAN_DIR=str(destination/'plans'))
    start=time.monotonic()
    print('checking execution fixtures',phase,flush=True)
    with (destination/'run.txt').open('w') as stream:
        result=subprocess.run(command,cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT)
    record={'command':command,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'env':{'TURSO_BENCH_PLAN_DIR':env['TURSO_BENCH_PLAN_DIR']}}
    (destination/'run.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record),flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
    plans[phase]={path.name:json.loads(path.read_text()) for path in (destination/'plans').glob('*.json')}
    assert len(plans[phase])==114,len(plans[phase])
assert plans['before'].keys()==plans['after'].keys()
changed=[name for name,plan in plans['before'].items() if plan!=plans['after'][name]]
new_cases=['membership_in_joined_projection','membership_not_in_joined_projection','membership_row_not_in_joined_projection','membership_in_joined_projection_small_indexed']
before_modes_same={name:all(plans['before'][name+'-auto.json']['plan']==plans['before'][name+'-'+mode+'.json']['plan'] for mode in ['forced','disabled']) for name in new_cases}
comparison={'cases':114,'changed_plans':sorted(changed),'existing_cases_with_changed_plans':[name for name in changed if not any(name.startswith(case+'-') for case in new_cases)],'before_new_cases_have_the_same_plan_in_every_mode':before_modes_same}
(execution/'comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
print(json.dumps(comparison),flush=True)
fuzz=out/'seed-57291020'
fuzz.mkdir()
command=[str(saved/'after/differential_fuzzer'),'--seed','57291020','--profile','correlated-selects','--max-subquery-depth','1','-n','1000','--coverage','--keep-files']
env=dict(os.environ,RUST_LOG='info,logical_optimizer=trace')
start=time.monotonic()
print('running SELECT-only differential checks',flush=True)
with (fuzz/'run.txt').open('w') as stream:
    result=subprocess.run(command,cwd=fuzz,env=env,stdout=stream,stderr=subprocess.STDOUT)
record={'command':command,'cwd':str(fuzz),'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start,'env':{'RUST_LOG':env['RUST_LOG']},'source':'../source.json'}
(fuzz/'run.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record),flush=True)
if result.returncode:
    raise SystemExit(result.returncode)
filters=json.loads((out/'filters.json').read_text())
before_filter=filters['before_execution'] if all(before_modes_same.values()) else filters['execution']
jobs=[
    ('prepare-original-joined-projection-fixtures',original/'prepare_benchmark','prepare',filters['original_prepare'],'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus the same six newer prepare fixtures; binary.json records source'),
    ('prepare-joined-projections-before',saved/'before/prepare_benchmark','prepare',filters['prepare'],'8dc712b61 plus identical benchmark fixtures and deferred drafts; builds.json records source'),
    ('prepare-joined-projections',saved/'after/prepare_benchmark','prepare',filters['prepare'],'8dc712b61 plus membership projection over joined inputs; source.json records source'),
    ('execution-original-joined-projections',original/'unnesting_execution','execution',filters['original_execution'],'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus four joined-projection fixtures; binary.json records source'),
    ('execution-joined-projections-before',saved/'before/unnesting_execution','execution',before_filter,'8dc712b61 plus identical benchmark fixtures and deferred drafts; builds.json records source'),
    ('execution-joined-projections',saved/'after/unnesting_execution','execution',filters['execution'],'8dc712b61 plus membership projection over joined inputs; source.json records source'),
]
measurements=[]
for phase in ['native','callgrind']:
    for directory,binary,kind,pattern,revision in jobs:
        command=['python3',str(root/'perf/logical-plan/measure.py'),str(binary),str(results/directory),'--phase',phase,'--kind',kind,'--cpu',str(filters['cpu']),'--filter',pattern,'--source-revision',revision]
        started=time.monotonic()
        result=subprocess.run(command,cwd=root)
        record={'directory':directory,'phase':phase,'command':command,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-started}
        measurements.append(record)
        (out/'measurements.json').write_text(json.dumps(measurements,indent=2)+'\n')
        if result.returncode:
            raise SystemExit(result.returncode)
print('All measurement rounds completed.',flush=True)
