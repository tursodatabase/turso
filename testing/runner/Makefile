# Forwarding Makefile so `make -C testing/runner <target>` works for tooling
# that targets the canonical "runner" path. The actual test runner lives at
# testing/sqltests; all targets below are delegated there.

SQLTESTS_DIR := ../sqltests

TARGETS := all build build-runner build-tursodb check run run-cli run-rust \
           run-js build-js-bindings test test-sqlite run-tests run-turso-tests \
           run-examples run-filter run-one install-sqlite convert convert-verbose clean

.PHONY: $(TARGETS)

$(TARGETS):
	$(MAKE) -C $(SQLTESTS_DIR) $@
