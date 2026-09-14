import json
from pathlib import Path
import re
import subprocess

path=Path('sqlite/conformance/sqlite-sqltests/row-value-in.sqltest')
original=path.read_text()
setup=re.search(r'^setup logical_correlated_membership \{\n(.*?)^\}',original,re.M|re.S)[1]
typed=re.search(r'^setup logical_correlated_membership_types \{\n(.*?)^\}',original,re.M|re.S)[1]
joined='FROM inner_rows i JOIN inner_rows j ON j.k=i.k WHERE i.k=o.k AND i.enabled=1 AND j.enabled=1'
cases=[]
for operator, label in [('IN','in'),('NOT IN','not-in')]:
    cases.extend([
        (f'{label}-outer-projection-joined-input','logical_correlated_membership',f'SELECT o.id FROM outer_rows o WHERE o.a+1 {operator} (SELECT i.x+o.k+j.enabled-1 {joined}) ORDER BY o.id;'),
        (f'row-{label}-outer-projection-joined-input','logical_correlated_membership',f'SELECT o.id FROM outer_rows o WHERE (o.a+1,o.b) {operator} (SELECT i.x+o.k,j.y {joined}) ORDER BY o.id;'),
        (f'{label}-outer-projection-joined-input-without-filter','logical_correlated_membership',f'SELECT o.id FROM outer_rows o WHERE o.a+o.k {operator} (SELECT i.x+o.k FROM inner_rows i JOIN inner_rows j ON j.k=i.k) ORDER BY o.id;'),
        (f'{label}-outer-projection-joined-input-collation','logical_correlated_membership_types',f'SELECT o.id FROM typed_outer o WHERE o.k {operator} (SELECT (CASE WHEN o.grp>0 THEN i.k ELSE o.k END) COLLATE NOCASE FROM typed_inner i JOIN typed_inner j ON i.rowid=j.rowid WHERE i.grp=o.grp) ORDER BY o.id;'),
    ])
cases.extend([
    ('in-outer-only-projection-joined-input','logical_correlated_membership',f'SELECT o.id FROM outer_rows o WHERE o.a IN (SELECT o.a {joined}) ORDER BY o.id;'),
    ('in-outer-projection-joined-input-keeps-duplicates','logical_correlated_membership',f'SELECT o.k,o.a FROM outer_rows o WHERE o.a+1 IN (SELECT i.x+o.k {joined}) ORDER BY o.k,o.a;'),
    ('not-in-outer-projection-joined-input-with-no-matches','logical_correlated_membership','SELECT o.id FROM outer_rows o WHERE o.a NOT IN (SELECT i.x+o.k FROM inner_rows i JOIN inner_rows j ON j.k=i.k WHERE i.k=123456) ORDER BY o.id;'),
    ('row-not-in-outer-projection-joined-input-disjunction','logical_correlated_membership','SELECT o.id FROM outer_rows o WHERE (o.a,o.b) NOT IN (SELECT i.x+o.k,j.y FROM inner_rows i JOIN inner_rows j ON j.k=i.k WHERE i.k<o.k OR j.y=o.b) ORDER BY o.id;'),
])
records=[]
blocks=[]
for name, setup_name, query in cases:
    script=(typed if setup_name.endswith('_types') else setup)+'\n'+query+'\n'
    result=subprocess.run(['/tmp/sqlite-version-3.50.4/sqlite3',':memory:'],input=script,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
    expected=result.stdout.strip('\n').splitlines()
    blocks.append(f'\n@setup {setup_name}\ntest {name} {{\n    {query}\n}}\nexpect {{\n'+''.join('    '+row+'\n' for row in expected)+'}\n')
    records.append({'name':name,'setup':setup_name,'sql':query,'expected':expected})
path.write_text(original+''.join(blocks))
Path('target/logical-plan-commits/membership-joined-projections/sqlite-expectations.json').write_text(json.dumps(records,indent=2)+'\n')
print(json.dumps(records,indent=2))
