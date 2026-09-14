import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

root=Path('/workspace')
out=root/'target/logical-plan-commits/membership-joined-projections'
before=json.loads((out/'before.json').read_text())
assert all(step['exit_code']==0 for name in ['validation.json','final-tests.json'] for step in json.loads((out/name).read_text()))
assert json.loads((out/'validation.json').read_text())[-1]['name']=='clippy'
assert json.loads((out/'final-tests.json').read_text())[-1]['name']=='declined-json'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==before['head']
path='core/translate/relational/membership.rs'
current=(root/path).read_bytes()
(out/'restore-membership.rs').write_bytes(current)
old=subprocess.check_output(['git','show',before['head']+':'+path],cwd=root)
assert hashlib.sha256(old).hexdigest()==before['sha256'][path]
source_paths=['core/translate/optimizer/mod.rs','core/translate/optimizer/unnest.rs','core/translate/relational/membership.rs','core/translate/relational/lower.rs','core/translate/main_loop/close.rs','core/benches/prepare_benchmark.rs','core/benches/unnesting_execution.rs']
source={'head':before['head'],'source_sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in source_paths},'working_diff':subprocess.check_output(['git','diff','--',*source_paths],cwd=root,text=True),'deferred_drafts':'Previously deferred UPDATE FROM and empty automatic-index drafts remain unchanged in the working tree and are excluded from this implementation commit.'}
(out/'source.json').write_text(json.dumps(source,indent=2)+'\n')
saved=root/'target/logical-plan-membership-joined-projections'
saved.mkdir()
for phase in ['before','after']:
    (saved/phase).mkdir()
shutil.copy2(root/'target/logical-plan-build/debug/differential_fuzzer',saved/'after/differential_fuzzer')
source['fuzzer_sha256']=hashlib.sha256((saved/'after/differential_fuzzer').read_bytes()).hexdigest()
(out/'source.json').write_text(json.dumps(source,indent=2)+'\n')
env=dict(os.environ,CARGO_TARGET_DIR=str(root/'target/logical-plan-build'),CARGO_BUILD_JOBS='4')
records=[]
try:
    for phase,content in [('before',old),('after',current)]:
        (root/path).write_bytes(content)
        for target,features in [('prepare_benchmark','bench'),('unnesting_execution','bench,simulator')]:
            command=['cargo','build','--locked','-p','turso_core','--features',features,'--bench',target,'--profile','dev','--message-format=json']
            name=phase+'-'+target
            print('building',name,flush=True)
            started=time.monotonic()
            with (out/(name+'.txt')).open('w') as stream:
                result=subprocess.run(command,cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT)
            record={'name':name,'command':command,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-started,'engine_membership_sha256':hashlib.sha256(content).hexdigest(),'source':'8dc712b61 plus identical benchmark fixtures and deferred drafts' if phase=='before' else 'source.json'}
            records.append(record)
            (out/'builds.json').write_text(json.dumps(records,indent=2)+'\n')
            if result.returncode:
                raise SystemExit(result.returncode)
            artifacts=[]
            for line in (out/(name+'.txt')).read_text().splitlines():
                try:
                    message=json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message.get('reason')=='compiler-artifact' and message.get('target',{}).get('name')==target and message.get('executable'):
                    artifacts.append(message['executable'])
            assert len(artifacts)==1,artifacts
            binary=saved/phase/target
            shutil.copy2(artifacts[0],binary)
            record.update({'binary':str(binary),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()})
            (out/'builds.json').write_text(json.dumps(records,indent=2)+'\n')
            print(json.dumps(record),flush=True)
finally:
    (root/path).write_bytes(current)
    for name,digest in source['source_sha256'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
    for name,digest in before['sha256'].items():
        if name not in [path,'core/translate/relational/rules/tests.rs']:
            assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
    assert subprocess.check_output(['git','ls-files','--stage','--','prompt2.md','Neumann-Unnesting-1.pdf','Neumann-Unnesting-2.pdf'],cwd=root,text=True)==before['input_index']
    (out/'restored.json').write_text(json.dumps({'source_restored':True,'staged_inputs_preserved':True},indent=2)+'\n')
