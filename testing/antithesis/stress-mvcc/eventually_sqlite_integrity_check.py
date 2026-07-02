#!/usr/bin/env -S python3 -u

import os
import runpy

runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sqlite_integrity_check.py"))
