#!/bin/bash
rm -f testing/testing_clone.db
sqlite3 testing/testing/db '.clone testing/testing_clone.db' > /dev/null
