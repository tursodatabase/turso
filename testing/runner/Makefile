# `testing/runner` was renamed to `testing/sqltests` in commit 26545aae4.
# This passthrough Makefile keeps legacy invocations (`make -C testing/runner ...`)
# working by delegating to the real Makefile.

.PHONY: %
%:
	$(MAKE) -C ../sqltests $@
