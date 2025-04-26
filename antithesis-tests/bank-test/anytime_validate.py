#!/usr/bin/env -S python3 -u

import limbo
from antithesis.random import get_random
from antithesis.assertions import always

con = limbo.connect("bank_test.db")
cur = con.cursor()

initial_state = cur.execute(f'''
    SELECT * FROM initial_state
''').fetchone()

curr_total = cur.execute(f'''
    SELECT SUM(balance) AS total FROM accounts;
''').fetchone()

always(
    initial_state[1] == curr_total[0], 
    '[Anytime] Initial balance always equals current balance',
    {
        'init_bal': initial_state[1],
        'curr_bal': curr_total[0]
    }
)

