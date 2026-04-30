# testing/runner was renamed to testing/sqltests (commit 26545aae4).
# Forward the documented targets so older invocations keep working.

.PHONY: build build-tursodb build-runner check run run-cli run-rust run-js \
        test test-sqlite run-tests run-turso-tests run-examples run-filter \
        run-one install-sqlite convert convert-verbose clean

build build-tursodb build-runner check run run-cli run-rust run-js \
test test-sqlite run-tests run-turso-tests run-examples run-filter \
run-one install-sqlite convert convert-verbose clean:
	$(MAKE) -C ../sqltests $@
