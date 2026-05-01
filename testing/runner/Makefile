# Compatibility shim: testing/runner was renamed to testing/sqltests in
# commit 26545aae4. Some tooling still references the old path; delegate
# every target to the real Makefile in testing/sqltests so those commands
# keep working.

.PHONY: FORCE
FORCE:

%: FORCE
	$(MAKE) -C ../sqltests $@
