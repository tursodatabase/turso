#!/bin/bash
rm -f testing/tmp_db/testing_clone.db*
scripts/generate_db.sh
sqlite3 testing/tmp_db/testing.db '.clone testing/tmp_db/testing_clone.db' > /dev/null
