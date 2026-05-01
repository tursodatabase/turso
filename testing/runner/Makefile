# `testing/runner` was renamed to `testing/sqltests` (commit 26545aae4).
# This proxy keeps stale tooling working by forwarding every target to the
# real Makefile next door.

.DEFAULT_GOAL := all

%:
	@$(MAKE) -C ../sqltests $@
