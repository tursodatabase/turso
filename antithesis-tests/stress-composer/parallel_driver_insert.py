#!/usr/bin/env -S python3 -u

import json
import limbo
from utils import generate_random_value
from antithesis.random import get_random


# Get initial state
con_init = limbo.connect('init_state.db')
cur_init = con_init.cursor()

tbl_len = cur_init.execute('SELECT count FROM tables').fetchone()[0]
selected_tbl = get_random() % tbl_len
tbl_schema = json.loads(cur_init.execute(f'SELECT schema FROM schemas WHERE tbl = {selected_tbl}').fetchone()[0])
cols = ', '.join([f'col_{col}' for col in range(tbl_schema['colCount'])])

con = limbo.connect('stress_composer.db')
cur = con.cursor()

# insert up to 100 rows in the selected table
insertions = get_random() % 100
print(f'Inserting {insertions} rows...')

for i in range(insertions):
    values = [generate_random_value(tbl_schema[f'col_{col}']['data_type']) for col in range(tbl_schema['colCount'])]
    cur.execute(f'''
        INSERT INTO tbl_{selected_tbl} ({cols})
        VALUES ({', '.join(values)})
    ''')

