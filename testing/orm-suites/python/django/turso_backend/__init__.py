import os
import sys

# Django's admin_scripts tests start subprocesses whose PYTHONPATH holds only
# this package's parent directory, so find turso_sqlite3 from here.
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "shim"))
