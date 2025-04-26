#!/usr/bin/env -S python3 -u

import limbo
import logging
from logging.handlers import RotatingFileHandler
from antithesis.random import get_random

handler = RotatingFileHandler(filename='bank_test.log', mode='a', maxBytes=1*1024*1024, backupCount=5, encoding=None, delay=0)
handler.setLevel(logging.INFO)

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)

logger.addHandler(handler)

con = limbo.connect("bank_test.db")
cur = con.cursor()

length = cur.execute("SELECT num_accts FROM initial_state").fetchone()[0]

def transaction():
    # check that sender and recipient are different
    sender = get_random() % length + 1
    recipient = get_random() % length + 1
    if sender != recipient:
        # get a random value to transfer between accounts
        value = get_random() % 1e9

        logger.info(f"Sender ID: {sender} | Recipient ID: {recipient} | Txn Val: {value}")

        cur.execute("BEGIN TRANSACTION;")
        
        # subtract value from balance of the sender account
        cur.execute(f'''
            UPDATE accounts 
            SET balance = balance - {value}
            WHERE account_id = {sender};
        ''')

        # add value to balance of the recipient account
        cur.execute(f'''
            UPDATE accounts 
            SET balance = balance + {value}
            WHERE account_id = {recipient};
        ''')

        cur.execute("COMMIT;")

# run up to 100 transactions
iterations = get_random() % 100
# logger.info(f"Starting {iterations} iterations")
for i in range(iterations):
    transaction()
# logger.info(f"Finished {iterations} iterations")
