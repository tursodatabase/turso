import json
import os
from pathlib import Path
import subprocess
import time

root=Path('/workspace')
out=root/'target/logical-plan-commits/membership-comparison-copies'
env=dict(os.environ,CARGO_TARGET_DIR=str(root/'target/logical-plan-build'),CARGO_BUILD_JOBS='4')
previous=json.loads((root/'target/logical-plan-commits/membership-joined-projections/validation.json').read_text())
steps=[]
for step in previous:
    name=step['name']
    if name in ['prepare-build','execution-build']:
        continue
    command=step['command']
    print(name,flush=True)
    started=time.monotonic()
    with (out/(name+'.txt')).open('w') as stream:
        result=subprocess.run(command,cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT)
    record={'name':name,'command':command,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-started,'log':name+'.txt'}
    steps.append(record)
    (out/'validation.json').write_text(json.dumps(steps,indent=2)+'\n')
    print(json.dumps(record),flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
