# Makefile for the MVCC passive checkpoint Quint model
#
# Each spec Mvcc*.qnt has a file with the same name plus Configs.qnt, with one
# module per configuration. The module named main is the main configuration.
# Every other module is a variant. The comment above each module says which
# result to expect and which invariants to check. This Makefile reads that
# comment. To add a spec, add the two files. The Makefile finds them.

QUINT ?= quint
# Quint runs TLC and Apalache with the `java` on PATH. Set JAVA_HOME to use
# another Java.
JAVA_HOME ?=
WORKERS ?= 4
SAMPLES ?= 10000
STEPS ?= 40
TRACES ?= 10
BUG_SAMPLES ?= 200000
# Quint starts an Apalache server here to prepare the spec for TLC.
# Two runs at the same time need two ports.
SERVER ?= localhost:8822
# Module names to run, separated by spaces. Empty means every module.
ONLY ?=

SPECS ?= $(basename $(filter-out %Configs.qnt,$(wildcard Mvcc*.qnt)))
STATES := $(CURDIR)/states

ifneq ($(JAVA_HOME),)
export PATH := $(JAVA_HOME)/bin:$(PATH)
endif
# Apalache and TLC write their state and temporary files here, not in /tmp.
export TMPDIR := $(STATES)/tmp
export JAVA_TOOL_OPTIONS := -Djava.io.tmpdir=$(STATES)/tmp

# Prints "module|expect|invariants" for each module of a configs file.
# expect is the invariant that must fail, or empty for "no error".
CONFIGS = awk '/^\/\/ Expect /{ e = ""; if (match($$0, /Expect [A-Za-z]+ to fail/)) e = substr($$0, RSTART + 7, RLENGTH - 15) } \
	/^\/\/ Invariants: /{ i = $$0; sub(/^\/\/ Invariants: /, "", i) } \
	$$1 == "module" { print $$2 "|" e "|" i }'

# The module name of the main configuration in each configs file.
MAIN = main

all: check variants

# The main configuration of each spec. The TLC backend must find no error.
check: KIND = main
# Every other configuration. Each one must give the result in its comment.
# Quint reports a violation of the combined invariant q_inv, not by name. So
# each run checks only the invariants of its comment, and a variant that must
# fail lists only that one invariant. A violation then names it.
variants: KIND = variants

check variants:
	@set -e; trap 'rm -rf $(STATES) _apalache-out' EXIT; \
	mkdir -p $(TMPDIR); echo '{"workers": $(WORKERS)}' > $(STATES)/tlc.json; \
	for spec in $(SPECS); do \
	  main=$(MAIN); \
	  $(CONFIGS) $${spec}Configs.qnt | while IFS='|' read -r mod expect invs; do \
	    if [ "$(KIND)" = main ] && [ "$$mod" != "$$main" ]; then continue; fi; \
	    if [ "$(KIND)" = variants ] && [ "$$mod" = "$$main" ]; then continue; fi; \
	    if [ -n "$(ONLY)" ] && ! echo " $(ONLY) " | grep -q " $$mod "; then continue; fi; \
	    name=$$spec-$$mod; start=$$(date +%s); \
	    out=$$($(QUINT) verify $${spec}Configs.qnt --main $$mod --init Init --step Next \
	      --invariants $$invs --backend tlc --tlc-config $(STATES)/tlc.json \
	      --server-endpoint $(SERVER) \
	      --verbosity 3 < /dev/null 2>&1 || true); \
	    rm -rf $(TMPDIR)/* _apalache-out; \
	    secs=$$(( $$(date +%s) - start )); \
	    states=$$(echo "$$out" | grep -o '[0-9,]* distinct states found' | tail -1 | cut -d' ' -f1); \
	    if echo "$$out" | grep -q '^\[ok\] No violation found'; then got=""; \
	    elif echo "$$out" | grep -q '^\[violation\] Found an issue'; then got=$$invs; \
	    else echo "FAIL $$name: quint or TLC did not finish"; echo "$$out" | tail -20; exit 1; fi; \
	    if [ "$$got" = "$$expect" ]; then \
	      if [ -n "$$expect" ]; then echo "ok   $$name: $$expect fails as expected ($$states states, $${secs}s)"; \
	      else echo "ok   $$name: no error ($$states states, $${secs}s)"; fi; \
	    else \
	      if [ -n "$$expect" ]; then echo "FAIL $$name: expected $$expect to fail"; \
	      else echo "FAIL $$name: expected no error, but $$invs fails"; fi; \
	      echo "$$out" | tail -40; exit 1; \
	    fi; \
	  done; \
	done

# Random runs of each main configuration with the Quint simulator. Faster
# than check, but it does not visit every state.
simulate:
	@set -e; for spec in $(SPECS); do \
	  main=$(MAIN); \
	  invs=$$($(CONFIGS) $${spec}Configs.qnt | awk -F'|' -v m=$$main '$$1 == m { print $$3 }'); \
	  out=$$($(QUINT) run $${spec}Configs.qnt --main $$main --init Init --step Next \
	    --invariants $$invs --max-samples $(SAMPLES) --max-steps $(STEPS) \
	    --n-threads $(WORKERS) --verbosity 1 < /dev/null 2>&1 || true); \
	  if echo "$$out" | grep -q '^\[ok\] No violation found'; then \
	    echo "ok   $$spec-$$main: no error in $(SAMPLES) runs of $(STEPS) steps"; \
	  else echo "FAIL $$spec-$$main"; echo "$$out" | tail -40; exit 1; fi; \
	done

# Random traces of each main configuration in the Informal Trace Format, for
# trace replay. Each state records the action it took (mbt::actionTaken) and
# the values the action picked (mbt::nondetPicks).
traces:
	@set -e; mkdir -p traces; for spec in $(SPECS); do \
	  main=$(MAIN); \
	  $(QUINT) run $${spec}Configs.qnt --main $$main --init Init --step Next \
	    --mbt --out-itf "traces/$${spec}_{seq}.itf.json" --n-traces $(TRACES) \
	    --max-samples $(TRACES) --max-steps $(STEPS) --n-threads $(WORKERS) \
	    --verbosity 1 < /dev/null; \
	done; ls traces

# One failing trace in the Informal Trace Format for each variant that must
# fail, found by the simulator, for trace replay of known bugs. The TLC
# backend finds these counterexamples too, but does not write them as ITF.
bug-traces:
	@set -e; mkdir -p traces/bugs; for spec in $(SPECS); do \
	  $(CONFIGS) $${spec}Configs.qnt | while IFS='|' read -r mod expect invs; do \
	    if [ -z "$$expect" ]; then continue; fi; \
	    if [ -n "$(ONLY)" ] && ! echo " $(ONLY) " | grep -q " $$mod "; then continue; fi; \
	    file=traces/bugs/$$spec-$$mod.itf.json; \
	    out=$$($(QUINT) run $${spec}Configs.qnt --main $$mod --init Init --step Next \
	      --invariants $$invs --mbt --out-itf $$file --max-samples $(BUG_SAMPLES) \
	      --max-steps $(STEPS) --n-threads $(WORKERS) --verbosity 1 < /dev/null 2>&1 || true); \
	    if echo "$$out" | grep -q '^\[violation\]'; then echo "ok   $$spec-$$mod: $$expect fails, trace in $$file"; \
	    else rm -f $$file; echo "miss $$spec-$$mod: no violation in $(BUG_SAMPLES) runs, use the TLC backend"; fi; \
	  done; \
	done

# The crash recovery spec (MvccRecovery.qnt) alone: its main configuration,
# then every variant and reachability check.
recovery:
	@$(MAKE) --no-print-directory check variants SPECS=MvccRecovery

typecheck:
	@set -e; for spec in $(SPECS); do $(QUINT) typecheck $${spec}Configs.qnt; done

clean:
	rm -rf states/ traces/ _apalache-out/

.PHONY: all check variants recovery simulate traces bug-traces typecheck clean
